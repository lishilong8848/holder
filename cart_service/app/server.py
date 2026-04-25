import asyncio
import json
import logging
import os
import sys
import threading

# === 紧急修复：解决 onnxruntime 在 Windows 上的导入挂起问题 ===
os.environ["OMP_WAIT_POLICY"] = "PASSIVE"
os.environ["OMP_NUM_THREADS"] = "1"
# =========================================================

print("\n🚀 [系统准备] 正在加载服务核心组件，请稍候...", flush=True)

import uuid
from datetime import datetime
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
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

LOG_LEVEL_NAME = os.environ.get("LOG_LEVEL", "WARNING").strip().upper()
LOG_LEVEL = getattr(logging, LOG_LEVEL_NAME, logging.WARNING)
logging.basicConfig(level=LOG_LEVEL)
logging.getLogger("uvicorn.access").disabled = True
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
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
    from .task_registry import TASK_REGISTRY, install_memory_log_handler
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
    from app.task_registry import TASK_REGISTRY, install_memory_log_handler
    from app.service import (
        BatchPersonResult,
        BatchRequestCoordinator,
        CertificateService,
        ClientDisconnectedError,
        QueueFullError,
        QueueTimeoutError,
    )

install_memory_log_handler()

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
SERVER_STARTED_AT = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# === 飞书群消息监听器，服务启动时自动运行 ===
_feishu_handler_client = None


def _ensure_feishu_handler_client():
    """初始化并复用群消息/页面触发共用的飞书客户端。"""
    global _feishu_handler_client
    if _feishu_handler_client is not None:
        return _feishu_handler_client

    app_id = os.environ.get("FEISHU_APP_ID", "").strip()
    app_secret = os.environ.get("FEISHU_APP_SECRET", "").strip()
    app_token = os.environ.get("FEISHU_APP_TOKEN", "").strip()
    table_id = os.environ.get("FEISHU_TABLE_ID", "").strip()
    if not app_id or not app_secret:
        return None

    _feishu_handler_client = FeishuTableReader(
        app_id=app_id,
        app_secret=app_secret,
        app_token=app_token,
        table_id=table_id,
    )
    return _feishu_handler_client


def _message_callback(msg: dict) -> None:
    """群消息处理回调：检测 记录ID=xxx 并触发完整业务流程"""
    try:
        from .message_handler import handle_message_async
    except ImportError:
        from app.message_handler import handle_message_async

    feishu_client = _ensure_feishu_handler_client()
    if feishu_client:
        handle_message_async(msg, feishu_client)


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


class UiTriggerRequest(BaseModel):
    record_id: str


class UiConfigUpdateRequest(BaseModel):
    values: Dict[str, str]


SENSITIVE_ENV_KEYWORDS = ("SECRET", "TOKEN", "KEY", "PASSWORD")
RESTART_REQUIRED_ENV_KEYS = {
    "HOST",
    "PORT",
    "FEISHU_APP_ID",
    "FEISHU_APP_SECRET",
    "FEISHU_APP_TOKEN",
    "FEISHU_TABLE_ID",
    "FEISHU_WATCH_GROUPS",
}


def _is_sensitive_env_key(key: str) -> bool:
    upper_key = key.upper()
    return any(keyword in upper_key for keyword in SENSITIVE_ENV_KEYWORDS)


def _mask_value(value: str) -> str:
    value = str(value or "")
    if not value:
        return ""
    if len(value) <= 8:
        return "••••"
    return f"{value[:4]}••••{value[-4:]}"


def _read_env_values() -> Dict[str, str]:
    env_path = loaded_env_path or (project_root / ".env")
    values: Dict[str, str] = {}
    if env_path and Path(env_path).is_file():
        for raw_line in Path(env_path).read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _write_env_values(updates: Dict[str, str]) -> Path:
    env_path = loaded_env_path or (project_root / ".env")
    env_path = Path(env_path)
    lines = env_path.read_text(encoding="utf-8").splitlines() if env_path.exists() else []
    remaining = dict(updates)
    output_lines: List[str] = []

    for raw_line in lines:
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#") or "=" not in raw_line:
            output_lines.append(raw_line)
            continue
        key, _old_value = raw_line.split("=", 1)
        clean_key = key.strip()
        if clean_key in remaining:
            output_lines.append(f"{clean_key}={remaining.pop(clean_key)}")
        else:
            output_lines.append(raw_line)

    if remaining:
        if output_lines and output_lines[-1].strip():
            output_lines.append("")
        output_lines.append("# Web 控制台写入配置")
        for key, value in remaining.items():
            output_lines.append(f"{key}={value}")

    env_path.parent.mkdir(parents=True, exist_ok=True)
    env_path.write_text("\n".join(output_lines) + "\n", encoding="utf-8")
    for key, value in updates.items():
        os.environ[key] = value
    return env_path


def _normalize_record_id(record_id: str) -> str:
    record_id = str(record_id or "").strip()
    if not record_id.startswith("rec"):
        raise HTTPException(status_code=400, detail="record_id 格式不正确，应以 rec 开头")
    return record_id


def get_configured_port() -> int:
    raw_port = os.environ.get("PORT", "").strip()
    if not raw_port:
        raise RuntimeError("未配置 PORT，请在 .env 中设置 PORT")
    try:
        port = int(raw_port)
    except ValueError as exc:
        raise RuntimeError(f"PORT 必须是数字，当前值: {raw_port}") from exc
    if port < 1 or port > 65535:
        raise RuntimeError(f"PORT 超出合法范围 1-65535，当前值: {port}")
    return port


def _start_certificate_task(record_id: str, *, source: str) -> Dict[str, Any]:
    try:
        from .message_handler import (
            WORKFLOW_CERTIFICATE,
            claim_record_processing,
            finish_record_processing,
            process_record_message,
        )
    except ImportError:
        from app.message_handler import (
            WORKFLOW_CERTIFICATE,
            claim_record_processing,
            finish_record_processing,
            process_record_message,
        )

    feishu_client = _ensure_feishu_handler_client()
    if not feishu_client:
        raise HTTPException(status_code=500, detail="飞书客户端未初始化，请检查环境变量配置")

    if not claim_record_processing(record_id, workflow=WORKFLOW_CERTIFICATE):
        return {
            "status": "duplicate_ignored",
            "record_id": record_id,
            "workflow": WORKFLOW_CERTIFICATE,
            "message": "该记录正在处理或近期已处理，已跳过重复触发",
        }

    task = TASK_REGISTRY.create_task(
        workflow=WORKFLOW_CERTIFICATE,
        record_id=record_id,
        source=source,
        current_step="证书查证任务已提交",
    )

    def _run():
        try:
            process_record_message(record_id, feishu_client, task_id=task.task_id)
        except Exception as exc:
            logger.error("[UI触发] 处理证书记录 %s 失败: %s", record_id, exc, exc_info=True)
            TASK_REGISTRY.finish_task(task.task_id, status="failed", current_step="任务异常", error=str(exc))
        finally:
            finish_record_processing(record_id, workflow=WORKFLOW_CERTIFICATE)

    threading.Thread(target=_run, name=f"ui-certificate-{record_id}", daemon=True).start()
    return {"status": "accepted", "record_id": record_id, "workflow": WORKFLOW_CERTIFICATE, "task_id": task.task_id}


def _start_photo_ai_task(record_id: str, *, source: str) -> Dict[str, Any]:
    try:
        from .message_handler import WORKFLOW_PHOTO_AI, claim_record_processing, finish_record_processing
        from .photo_ai_handler import process_photo_ai_record
    except ImportError:
        from app.message_handler import WORKFLOW_PHOTO_AI, claim_record_processing, finish_record_processing
        from app.photo_ai_handler import process_photo_ai_record

    feishu_client = _ensure_feishu_handler_client()
    if not feishu_client:
        raise HTTPException(status_code=500, detail="飞书客户端未初始化，请检查环境变量配置")

    if not claim_record_processing(record_id, workflow=WORKFLOW_PHOTO_AI):
        return {
            "status": "duplicate_ignored",
            "record_id": record_id,
            "workflow": WORKFLOW_PHOTO_AI,
            "message": "该记录正在处理或近期已处理，已跳过重复触发",
        }

    task = TASK_REGISTRY.create_task(
        workflow=WORKFLOW_PHOTO_AI,
        record_id=record_id,
        source=source,
        current_step="照片AI识别任务已提交",
    )

    def _run():
        try:
            process_photo_ai_record(record_id, feishu_client, task_id=task.task_id)
        except Exception as exc:
            logger.error("[UI触发] 处理照片AI记录 %s 失败: %s", record_id, exc, exc_info=True)
            TASK_REGISTRY.finish_task(task.task_id, status="failed", current_step="任务异常", error=str(exc))
        finally:
            finish_record_processing(record_id, workflow=WORKFLOW_PHOTO_AI)

    threading.Thread(target=_run, name=f"ui-photo-ai-{record_id}", daemon=True).start()
    return {"status": "accepted", "record_id": record_id, "workflow": WORKFLOW_PHOTO_AI, "task_id": task.task_id}


@app.post("/api/trigger", summary="飞书多维表格自动化直接触发接口")
async def trigger_by_record_id(req: TriggerRequest):
    """
    供飞书多维表格自动化（HTTP请求节点）直接调用。
    无需解析群消息卡片，直接传入 record_id 触发证书查询回填流程。
    调用示例：POST http://your-server:{PORT}/api/trigger
    Body: {"record_id": "recvgJe0RmBmIM"}
    """
    record_id = _normalize_record_id(req.record_id)
    result = _start_certificate_task(record_id, source="api")
    result["message"] = "任务已接受，正在后台处理" if result["status"] == "accepted" else result.get("message")
    return result


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


@app.get("/", include_in_schema=False)
async def ui_index():
    index_path = project_root / "app" / "static" / "index.html"
    if not index_path.is_file():
        raise HTTPException(status_code=404, detail="前端页面不存在")
    return FileResponse(index_path)


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


@app.get("/api/ui/status")
def ui_status():
    watched_groups = [item.strip() for item in os.environ.get("FEISHU_WATCH_GROUPS", "").split(",") if item.strip()]
    return {
        "service": {
            "status": "ok",
            "started_at": SERVER_STARTED_AT,
            "version": app.version,
            "host": os.environ.get("HOST", "127.0.0.1"),
            "port": get_configured_port(),
        },
        "listener": {
            "configured": bool(os.environ.get("FEISHU_APP_ID") and os.environ.get("FEISHU_APP_SECRET")),
            "active_client": _feishu_handler_client is not None,
            "watched_groups": watched_groups,
        },
        "queue": REQUEST_COORDINATOR.snapshot(),
        "tasks": TASK_REGISTRY.stats(),
        "config_path": str(loaded_env_path or (project_root / ".env")),
    }


@app.get("/api/ui/tasks")
def ui_tasks(limit: int = Query(50, ge=1, le=300)):
    return TASK_REGISTRY.list_tasks(limit=limit)


@app.get("/api/ui/tasks/{task_id}")
def ui_task_detail(task_id: str):
    task = TASK_REGISTRY.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    return task


@app.post("/api/ui/certificate/trigger")
def ui_trigger_certificate(payload: UiTriggerRequest):
    record_id = _normalize_record_id(payload.record_id)
    return _start_certificate_task(record_id, source="ui")


@app.post("/api/ui/photo-ai/trigger")
def ui_trigger_photo_ai(payload: UiTriggerRequest):
    record_id = _normalize_record_id(payload.record_id)
    return _start_photo_ai_task(record_id, source="ui")


@app.get("/api/ui/config")
def ui_config():
    values = _read_env_values()
    items = []
    for key in sorted(values):
        value = values[key]
        sensitive = _is_sensitive_env_key(key)
        items.append(
            {
                "key": key,
                "value": _mask_value(value) if sensitive else value,
                "sensitive": sensitive,
                "restart_required": key in RESTART_REQUIRED_ENV_KEYS,
            }
        )
    return {
        "path": str(loaded_env_path or (project_root / ".env")),
        "items": items,
        "restart_required_keys": sorted(RESTART_REQUIRED_ENV_KEYS),
    }


@app.put("/api/ui/config")
def ui_update_config(payload: UiConfigUpdateRequest):
    updates: Dict[str, str] = {}
    for key, value in payload.values.items():
        clean_key = str(key or "").strip()
        if not clean_key:
            continue
        clean_value = str(value or "").strip()
        if "••" in clean_value:
            continue
        updates[clean_key] = clean_value

    if not updates:
        return {
            "updated": [],
            "restart_required": [],
            "message": "没有需要保存的配置项",
        }

    env_path = _write_env_values(updates)
    restart_required = sorted(key for key in updates if key in RESTART_REQUIRED_ENV_KEYS)
    TASK_REGISTRY.add_event("config", f"已保存配置: {', '.join(sorted(updates))}")
    return {
        "path": str(env_path),
        "updated": sorted(updates),
        "restart_required": restart_required,
        "message": "配置已保存；部分配置可能需要重启服务后完全生效" if restart_required else "配置已保存",
    }


@app.get("/api/ui/logs")
def ui_logs(limit: int = Query(300, ge=1, le=1000), min_level: str = Query("warning")):
    return TASK_REGISTRY.events(limit=limit, min_level=min_level)


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

    port = get_configured_port()
    host = os.environ.get("HOST", "127.0.0.1")
    uvicorn.run(app, host=host, port=port, log_level=LOG_LEVEL_NAME.lower(), access_log=False)
