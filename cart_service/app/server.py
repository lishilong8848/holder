import asyncio
import logging
import os
import sys

# === 紧急修复：解决 onnxruntime 在 Windows 上的导入挂起问题 ===
os.environ["OMP_WAIT_POLICY"] = "PASSIVE"
os.environ["OMP_NUM_THREADS"] = "1"
# =========================================================

print("\n🚀 [系统准备] 正在加载服务核心组件，请稍候...", flush=True)

import uuid
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel

project_root = Path(__file__).resolve().parents[1]

try:
    from .env_loader import load_app_env
except ImportError:
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from app.env_loader import load_app_env

loaded_env_path = load_app_env(project_root=project_root, override=True)
if loaded_env_path:
    print(f"[系统准备] 已加载环境配置: {loaded_env_path}", flush=True)
else:
    print("[系统准备] 未找到 .env 文件，使用当前系统环境变量", flush=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from .certificate_query import (
        EFFECTIVE_END_FIELD,
        CertificateQuery,
        ExtractedCertificateCard,
        PersonQueryResult,
    )
    from .feishu_reader import FeishuTableReader
    from .feishu_listener import create_listener_from_env
    from .service import (
        BatchPersonResult,
        BatchRequestCoordinator,
        CertificateService,
        ClientDisconnectedError,
        QueueFullError,
        QueueTimeoutError,
    )
except ImportError:
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from app.certificate_query import (
        EFFECTIVE_END_FIELD,
        CertificateQuery,
        ExtractedCertificateCard,
        PersonQueryResult,
    )
    from app.feishu_reader import FeishuTableReader
    from app.feishu_listener import create_listener_from_env
    from app.service import (
        BatchPersonResult,
        BatchRequestCoordinator,
        CertificateService,
        ClientDisconnectedError,
        QueueFullError,
        QueueTimeoutError,
    )


MAX_BATCH_SIZE = 20
DEFAULT_CONCURRENCY = int(os.environ.get("DEFAULT_CONCURRENCY", 3))
REQUEST_VALIDATION_DETAIL = "请求参数不合法"

REQUEST_COORDINATOR = BatchRequestCoordinator(
    max_queue_size=100,
    queue_timeout_seconds=300,
)

SUPPORTED_CERT_TYPES = (
    "high_voltage",
    "low_voltage",
    "refrigeration",
    "working_at_height",
)

app = FastAPI(title="证书批量查询回填 API (并发增强版)", version="3.1.0")


# === 飞书群消息监听器，服务启动时自动运行 ===
_feishu_handler_client = None


def _message_callback(msg: dict) -> None:
    """群消息处理回调：检测 记录ID=xxx 并触发完整业务流程"""
    global _feishu_handler_client

    try:
        from .message_handler import handle_message_async
    except ImportError:
        from app.message_handler import handle_message_async

    if _feishu_handler_client is None:
        app_id = os.environ.get("FEISHU_APP_ID", "").strip()
        app_secret = os.environ.get("FEISHU_APP_SECRET", "").strip()
        app_token = os.environ.get(
            "FEISHU_APP_TOKEN", "X9CwbB3zhaLK7JsQZVZcFR9fnTf"
        ).strip()
        table_id = os.environ.get("FEISHU_TABLE_ID", "tblWHIbp172MNjM1").strip()

        if app_id and app_secret:
            _feishu_handler_client = FeishuTableReader(
                app_id=app_id,
                app_secret=app_secret,
                app_token=app_token,
                table_id=table_id,
            )

    if _feishu_handler_client:
        handle_message_async(msg, _feishu_handler_client)


@app.on_event("startup")
async def startup_feishu_listener():
    """服务启动时自动初始化飞书群消息监听器"""
    if "pytest" in sys.modules:
        logger.info("检测到 pytest 环境，跳过飞书群消息监听")
        return

    started = create_listener_from_env(on_message=_message_callback)
    if started:
        logger.info("飞书群消息监听器已集成到服务")
    else:
        logger.info("未配置飞书监听环境变量，跳过群消息监听")


class TriggerRequest(BaseModel):
    record_id: str
    token: Optional[str] = None  # 可选的简单鉴权 token


@app.post("/api/trigger", summary="飞书多维表格自动化直接触发接口")
async def trigger_by_record_id(req: TriggerRequest):
    """
    供飞书多维表格自动化（HTTP请求节点）直接调用。
    无需解析群消息卡片，直接传入 record_id 触发证书查询回填流程。
    调用示例：POST http://your-server:58000/api/trigger
    Body: {"record_id": "recvgJe0RmBmIM"}
    """
    record_id = req.record_id.strip()
    if not record_id.startswith("rec"):
        raise HTTPException(
            status_code=400, detail="record_id 格式不正确，应以 rec 开头"
        )

    # 初始化 feishu client（复用已有逻辑）
    global _feishu_handler_client
    if _feishu_handler_client is None:
        app_id = os.environ.get("FEISHU_APP_ID", "").strip()
        app_secret = os.environ.get("FEISHU_APP_SECRET", "").strip()
        app_token = os.environ.get("FEISHU_APP_TOKEN", "").strip()
        table_id = os.environ.get("FEISHU_TABLE_ID", "").strip()
        if app_id and app_secret:
            try:
                from .feishu_reader import FeishuTableReader
            except ImportError:
                from app.feishu_reader import FeishuTableReader
            _feishu_handler_client = FeishuTableReader(
                app_id=app_id,
                app_secret=app_secret,
                app_token=app_token,
                table_id=table_id,
            )

    if not _feishu_handler_client:
        raise HTTPException(
            status_code=500, detail="飞书客户端未初始化，请检查环境变量配置"
        )

    try:
        from .message_handler import claim_record_processing, finish_record_processing, process_record_message
    except ImportError:
        from app.message_handler import claim_record_processing, finish_record_processing, process_record_message

    import threading

    if not claim_record_processing(record_id):
        return {
            "status": "duplicate_ignored",
            "record_id": record_id,
            "message": "该记录正在处理或近期已处理，已跳过重复触发",
        }

    def _run():
        try:
            process_record_message(record_id, _feishu_handler_client)
        except Exception as e:
            logger.error(f"[API触发] 处理记录 {record_id} 失败: {e}")
        finally:
            finish_record_processing(record_id)

    thread = threading.Thread(target=_run, name=f"api-trigger-{record_id}", daemon=True)
    thread.start()
    logger.info(f"[API触发] 已异步启动记录 {record_id} 的处理任务")

    return {
        "status": "accepted",
        "record_id": record_id,
        "message": "任务已接受，正在后台处理",
    }


class FeishuConfigRequest(BaseModel):
    app_id: str
    app_secret: str
    app_token: str
    table_id: str


class PersonRequest(BaseModel):
    record_id: Optional[str] = None
    name: str
    id_number: str


class LookupRequest(BaseModel):
    id_number_field: str
    name_field: Optional[str] = None


class CertificateFieldMappingRequest(BaseModel):
    expire_field: str
    review_due_field: str
    review_actual_field: str
    attachment_field: str


class FieldMappingRequest(BaseModel):
    high_voltage: Optional[CertificateFieldMappingRequest] = None
    low_voltage: Optional[CertificateFieldMappingRequest] = None
    refrigeration: Optional[CertificateFieldMappingRequest] = None
    working_at_height: Optional[CertificateFieldMappingRequest] = None


class BatchQueryRequest(BaseModel):
    feishu: FeishuConfigRequest
    lookup: Optional[LookupRequest] = None
    field_mapping: FieldMappingRequest
    people: List[PersonRequest]
    concurrency: Optional[int] = None


class ResultItem(BaseModel):
    name: str
    id_number: str
    success: bool
    record_id: Optional[str] = None
    query_status: Optional[str] = None
    query_error: Optional[str] = None
    writeback_error: Optional[str] = None


class BatchQueryResponse(BaseModel):
    total: int
    success: int
    failed: int
    results: List[ResultItem]


class CertificateDetail(BaseModel):
    fields: Dict[str, Any]
    local_path: Optional[str] = None


class PersonQueryDetail(BaseModel):
    record_id: str
    name: str
    id_number: str
    status: str
    error: Optional[str] = None
    records: List[Dict[str, Any]]
    selected_certificates: Dict[str, CertificateDetail]
    queried_at: str


class OnlyQueryResponse(BaseModel):
    batch_path: str
    total: int
    results: List[PersonQueryDetail]


@app.exception_handler(RequestValidationError)
async def request_validation_exception_handler(
    _request: Request, exc: RequestValidationError
):
    return JSONResponse(
        status_code=400,
        content={
            "detail": REQUEST_VALIDATION_DETAIL,
            "errors": exc.errors(),
        },
    )


def normalize_feishu_config(config: FeishuConfigRequest) -> Dict[str, str]:
    normalized: Dict[str, str] = {}
    for key in ("app_id", "app_secret", "app_token", "table_id"):
        value = getattr(config, key).strip()
        if not value:
            env_value = os.environ.get(f"FEISHU_{key.upper()}", "").strip()
            if not env_value:
                raise HTTPException(status_code=400, detail=f"feishu.{key} 不能为空")
            value = env_value
        normalized[key] = value
    return normalized


def normalize_field_mapping(
    field_mapping: FieldMappingRequest,
) -> Dict[str, Dict[str, str]]:
    normalized: Dict[str, Dict[str, str]] = {}
    for cert_type in SUPPORTED_CERT_TYPES:
        mapping = getattr(field_mapping, cert_type)
        if mapping is None:
            continue

        clean_mapping: Dict[str, str] = {}
        for field_name in (
            "expire_field",
            "review_due_field",
            "review_actual_field",
            "attachment_field",
        ):
            value = getattr(mapping, field_name).strip()
            if not value:
                raise HTTPException(
                    status_code=400,
                    detail=f"field_mapping.{cert_type}.{field_name} 不能为空",
                )
            clean_mapping[field_name] = value
        normalized[cert_type] = clean_mapping

    if not normalized:
        raise HTTPException(
            status_code=400, detail="field_mapping 至少需要配置一种证书类型"
        )
    return normalized


def normalize_lookup(
    lookup: Optional[LookupRequest],
) -> Optional[Dict[str, Optional[str]]]:
    if lookup is None:
        return None

    id_number_field = lookup.id_number_field.strip()
    if not id_number_field:
        raise HTTPException(status_code=400, detail="lookup.id_number_field 不能为空")

    name_field = None
    if lookup.name_field is not None:
        name_field = lookup.name_field.strip() or None

    return {
        "id_number_field": id_number_field,
        "name_field": name_field,
    }


def normalize_people(people: List[PersonRequest]) -> List[Dict[str, str]]:
    if not people:
        raise HTTPException(status_code=400, detail="people 不能为空")
    if len(people) > MAX_BATCH_SIZE:
        raise HTTPException(
            status_code=400, detail=f"people 最多支持 {MAX_BATCH_SIZE} 条"
        )

    normalized_people: List[Dict[str, str]] = []
    for index, person in enumerate(people, start=1):
        record_id = (person.record_id or "").strip()
        name = person.name.strip()
        id_number = person.id_number.strip()
        if not name:
            raise HTTPException(
                status_code=400, detail=f"people[{index}].name 不能为空"
            )
        if not id_number:
            raise HTTPException(
                status_code=400, detail=f"people[{index}].id_number 不能为空"
            )
        normalized_people.append(
            {
                "record_id": record_id,
                "name": name,
                "id_number": id_number,
            }
        )
    return normalized_people


def build_response(result_rows: List[BatchPersonResult]) -> List[ResultItem]:
    return [
        ResultItem(
            name=row.name,
            id_number=row.id_number,
            success=row.success,
            record_id=row.record_id,
            query_status=row.query_status,
            query_error=row.query_error,
            writeback_error=row.writeback_error,
        )
        for row in result_rows
    ]


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.post(
    "/api/v1/query/batch",
    response_model=BatchQueryResponse,
    response_model_exclude_none=True,
)
async def batch_query(request: Request, payload: BatchQueryRequest):
    request_id = uuid.uuid4().hex
    feishu_config = normalize_feishu_config(payload.feishu)
    lookup = normalize_lookup(payload.lookup)
    field_mapping = normalize_field_mapping(payload.field_mapping)
    people = normalize_people(payload.people)
    if any(not person["record_id"] for person in people) and lookup is None:
        raise HTTPException(
            status_code=400, detail="未传入 people[].record_id 时，必须提供 lookup 配置"
        )

    if payload.concurrency is not None:
        logger.warning(
            "请求 %s 传入了已废弃的 concurrency=%s，系统将忽略该参数",
            request_id,
            payload.concurrency,
        )

    service = CertificateService(
        feishu_config=feishu_config,
        chrome_bin=os.environ.get("CHROME_BIN"),
        chromedriver_path=os.environ.get("CHROMEDRIVER_PATH"),
    )

    try:
        queued_result = await REQUEST_COORDINATOR.run(
            request_id=request_id,
            work=lambda: asyncio.to_thread(
                service.process_batch_request,
                request_id=request_id,
                people=people,
                lookup=lookup,
                field_mapping=field_mapping,
                debug=True,
            ),
            is_disconnected=request.is_disconnected,
        )
    except QueueFullError as exc:
        raise HTTPException(status_code=429, detail=str(exc)) from exc
    except QueueTimeoutError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ClientDisconnectedError:
        logger.info("请求 %s 在开始执行前已断开连接", request_id)
        raise HTTPException(status_code=499, detail="客户端在任务开始执行前已断开连接")
    except RuntimeError as exc:
        logger.error("请求 %s 因服务运行时错误失败：%s", request_id, exc, exc_info=True)
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        logger.error(f"批量查询引擎故障: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"批量查询引擎故障: {exc}")

    batch_result = queued_result.result
    return BatchQueryResponse(
        total=batch_result.total,
        success=batch_result.success,
        failed=batch_result.failed,
        results=build_response(batch_result.results),
    )


@app.post("/api/v1/query/only", response_model=OnlyQueryResponse)
async def query_only(
    people: List[PersonRequest],
    concurrency: Optional[int] = Query(None),
    debug: bool = Query(False),
):
    import json
    from datetime import datetime

    normalized_people = normalize_people(people)
    concurrency = concurrency or DEFAULT_CONCURRENCY

    service = CertificateService(
        max_workers=concurrency,
        chrome_bin=os.environ.get("CHROME_BIN"),
        chromedriver_path=os.environ.get("CHROMEDRIVER_PATH"),
    )

    try:
        query_results = service.run_batch(people=normalized_people)
    except Exception as exc:
        logger.error(f"查询引擎故障: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"查询引擎故障: {exc}")

    # 准备本地存储
    project_root = Path(__file__).resolve().parents[1]
    output_root = project_root / "output"
    output_root.mkdir(exist_ok=True)

    batch_id = datetime.now().strftime("batch_%Y%m%d_%H%M%S")
    batch_dir = output_root / batch_id
    batch_dir.mkdir(exist_ok=True)

    response_items = []
    for res in query_results:
        selected_certs = {}
        query_time_str = datetime.now().strftime("%Y%m%d%H%M%S")

        for cert_type, card in res.selected_certificates.items():
            local_img_path = None
            if card.screenshot_bytes:
                # 命名格式: 姓名_证件号_时间_证件名.jpg
                cert_display_name = (
                    (card.fields.get("操作项目") or cert_type)
                    .replace("/", "_")
                    .replace("\\", "_")
                )
                filename = f"{res.name}_{res.id_number}_{query_time_str}_{cert_display_name}.jpg"
                save_path = batch_dir / filename
                with open(save_path, "wb") as f:
                    f.write(card.screenshot_bytes)
                local_img_path = str(save_path.absolute())

            selected_certs[cert_type] = CertificateDetail(
                fields=card.fields, local_path=local_img_path
            )

        detail = PersonQueryDetail(
            record_id=res.record_id,
            name=res.name,
            id_number=res.id_number,
            status=res.status,
            error=res.error,
            records=res.records,
            selected_certificates=selected_certs,
            queried_at=res.queried_at,
        )
        response_items.append(detail)

    # 保存 info.json 到本地
    info_json_path = batch_dir / "results.json"
    with open(info_json_path, "w", encoding="utf-8") as f:
        # 使用 pydantic 的 model_dump 来序列化
        results_data = [item.model_dump() for item in response_items]
        json.dump(results_data, f, ensure_ascii=False, indent=2)

    return OnlyQueryResponse(
        batch_path=str(batch_dir.absolute()),
        total=len(response_items),
        results=response_items,
    )


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 58000))
    uvicorn.run(app, host="0.0.0.0", port=port)
