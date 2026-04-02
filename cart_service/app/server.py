import asyncio
import logging
import os
import sys
import uuid
from pathlib import Path
from typing import Dict, List, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from .service import (
        BatchPersonResult,
        BatchRequestCoordinator,
        CertificateService,
        ClientDisconnectedError,
        QueueFullError,
        QueueTimeoutError,
    )
except ImportError:
    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from app.service import (
        BatchPersonResult,
        BatchRequestCoordinator,
        CertificateService,
        ClientDisconnectedError,
        QueueFullError,
        QueueTimeoutError,
    )


REQUEST_VALIDATION_DETAIL = "\u8bf7\u6c42\u53c2\u6570\u4e0d\u5408\u6cd5"
MAX_BATCH_SIZE = 20
MAX_QUEUE_SIZE = max(int(os.environ.get("MAX_QUEUE_SIZE", 10)), 1)
QUEUE_TIMEOUT_SECONDS = max(int(os.environ.get("QUEUE_TIMEOUT_SECONDS", 600)), 1)

SUPPORTED_CERT_TYPES = (
    "high_voltage",
    "low_voltage",
    "refrigeration",
    "working_at_height",
)

REQUEST_COORDINATOR = BatchRequestCoordinator(
    max_queue_size=MAX_QUEUE_SIZE,
    queue_timeout_seconds=QUEUE_TIMEOUT_SECONDS,
)

app = FastAPI(title="证书查询回填接口", version="4.0.0")


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


@app.exception_handler(RequestValidationError)
async def request_validation_exception_handler(_request: Request, exc: RequestValidationError):
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


def normalize_field_mapping(field_mapping: FieldMappingRequest) -> Dict[str, Dict[str, str]]:
    normalized: Dict[str, Dict[str, str]] = {}
    for cert_type in SUPPORTED_CERT_TYPES:
        mapping = getattr(field_mapping, cert_type)
        if mapping is None:
            continue

        clean_mapping: Dict[str, str] = {}
        for field_name in ("expire_field", "review_due_field", "review_actual_field", "attachment_field"):
            value = getattr(mapping, field_name).strip()
            if not value:
                raise HTTPException(status_code=400, detail=f"field_mapping.{cert_type}.{field_name} 不能为空")
            clean_mapping[field_name] = value
        normalized[cert_type] = clean_mapping

    if not normalized:
        raise HTTPException(status_code=400, detail="field_mapping 至少需要配置一种证书类型")
    return normalized


def normalize_lookup(lookup: Optional[LookupRequest]) -> Optional[Dict[str, Optional[str]]]:
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
        raise HTTPException(status_code=400, detail=f"people 最多支持 {MAX_BATCH_SIZE} 条")

    normalized_people: List[Dict[str, str]] = []
    for index, person in enumerate(people, start=1):
        record_id = (person.record_id or "").strip()
        name = person.name.strip()
        id_number = person.id_number.strip()
        if not name:
            raise HTTPException(status_code=400, detail=f"people[{index}].name 不能为空")
        if not id_number:
            raise HTTPException(status_code=400, detail=f"people[{index}].id_number 不能为空")
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


@app.post("/api/v1/query/batch", response_model=BatchQueryResponse, response_model_exclude_none=True)
async def batch_query(request: Request, payload: BatchQueryRequest):
    request_id = uuid.uuid4().hex
    feishu_config = normalize_feishu_config(payload.feishu)
    lookup = normalize_lookup(payload.lookup)
    field_mapping = normalize_field_mapping(payload.field_mapping)
    people = normalize_people(payload.people)
    if any(not person["record_id"] for person in people) and lookup is None:
        raise HTTPException(status_code=400, detail="未传入 people[].record_id 时，必须提供 lookup 配置")

    if payload.concurrency is not None:
        logger.warning("请求 %s 传入了已废弃的 concurrency=%s，系统将忽略该参数", request_id, payload.concurrency)

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
        logger.error("请求 %s 发生未预期错误：%s", request_id, exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"批量查询失败：{exc}") from exc

    logger.info(
        "请求 %s 已完成：总数=%s 成功=%s 失败=%s 排队耗时=%.2f 秒 执行耗时=%.2f 秒",
        request_id,
        queued_result.result.total,
        queued_result.result.success,
        queued_result.result.failed,
        queued_result.queued_seconds,
        queued_result.execution_seconds,
    )

    return BatchQueryResponse(
        total=queued_result.result.total,
        success=queued_result.result.success,
        failed=queued_result.result.failed,
        results=build_response(queued_result.result.results),
    )


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 58000))
    uvicorn.run(app, host="0.0.0.0", port=port)
