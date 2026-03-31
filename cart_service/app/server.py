import logging
import os
from pathlib import Path
import sys
from typing import Dict, List, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# 加载环境变量
load_dotenv()

# 配置日志
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
    from .service import CertificateService
except ImportError:
    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from app.certificate_query import (
        EFFECTIVE_END_FIELD,
        CertificateQuery,
        ExtractedCertificateCard,
        PersonQueryResult,
    )
    from app.feishu_reader import FeishuTableReader
    from app.service import CertificateService


MAX_BATCH_SIZE = 50  # 提升并发后的批量上限
DEFAULT_CONCURRENCY = int(os.environ.get("DEFAULT_CONCURRENCY", 3))

SUPPORTED_CERT_TYPES = (
    "high_voltage",
    "low_voltage",
    "refrigeration",
    "working_at_height",
)

app = FastAPI(title="证书批量查询回填 API (并发增强版)", version="3.1.0")


class FeishuConfigRequest(BaseModel):
    app_id: str
    app_secret: str
    app_token: str
    table_id: str


class PersonRequest(BaseModel):
    record_id: str
    name: str
    id_number: str


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
    field_mapping: FieldMappingRequest
    people: List[PersonRequest]
    concurrency: Optional[int] = None
    debug: bool = False


class ResultItem(BaseModel):
    record_id: str
    success: bool
    query_status: Optional[str] = None
    query_error: Optional[str] = None
    writeback_error: Optional[str] = None


class BatchQueryResponse(BaseModel):
    total: int
    success: int
    failed: int
    results: List[ResultItem]


@app.exception_handler(RequestValidationError)
async def request_validation_exception_handler(_request, exc: RequestValidationError):
    return JSONResponse(
        status_code=400,
        content={
            "detail": "请求参数不合法",
            "errors": exc.errors(),
        },
    )


def normalize_feishu_config(config: FeishuConfigRequest) -> Dict[str, str]:
    normalized = {}
    for key in ("app_id", "app_secret", "app_token", "table_id"):
        value = getattr(config, key).strip()
        if not value:
            # 允许从环境变量读取默认值
            env_val = os.environ.get(f"FEISHU_{key.upper()}")
            if env_val:
                value = env_val
            else:
                raise HTTPException(status_code=400, detail=f"feishu.{key} 不能为空且未设置环境变量")
        normalized[key] = value
    return normalized


def normalize_field_mapping(field_mapping: FieldMappingRequest) -> Dict[str, Dict[str, str]]:
    normalized: Dict[str, Dict[str, str]] = {}

    for cert_type in SUPPORTED_CERT_TYPES:
        mapping = getattr(field_mapping, cert_type)
        if mapping is None:
            continue

        clean_mapping = {}
        for field_name in ("expire_field", "review_due_field", "review_actual_field", "attachment_field"):
            value = getattr(mapping, field_name).strip()
            if not value:
                raise HTTPException(status_code=400, detail=f"field_mapping.{cert_type}.{field_name} 不能为空")
            clean_mapping[field_name] = value
        normalized[cert_type] = clean_mapping

    if not normalized:
        raise HTTPException(status_code=400, detail="field_mapping 至少需要提供一个证书类型映射")

    return normalized


def normalize_people(people: List[PersonRequest]) -> List[Dict[str, str]]:
    if not people:
        raise HTTPException(status_code=400, detail="people 不能为空")

    if len(people) > MAX_BATCH_SIZE:
        raise HTTPException(status_code=400, detail=f"单次最多支持 {MAX_BATCH_SIZE} 人")

    normalized_people: List[Dict[str, str]] = []
    for person in people:
        record_id = person.record_id.strip()
        name = person.name.strip()
        id_number = person.id_number.strip()
        if not record_id or not name or not id_number:
            continue
        normalized_people.append(
            {
                "record_id": record_id,
                "name": name,
                "id_number": id_number,
            }
        )
    return normalized_people


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.post("/api/v1/query/batch", response_model=BatchQueryResponse, response_model_exclude_none=True)
async def batch_query(request: BatchQueryRequest):
    feishu_config = normalize_feishu_config(request.feishu)
    field_mapping = normalize_field_mapping(request.field_mapping)
    people = normalize_people(request.people)
    
    concurrency = request.concurrency or DEFAULT_CONCURRENCY

    # 初始化服务层
    service = CertificateService(
        feishu_config=feishu_config,
        max_workers=concurrency,
        chrome_bin=os.environ.get("CHROME_BIN"),
        chromedriver_path=os.environ.get("CHROMEDRIVER_PATH")
    )

    # 1. 并发查询
    try:
        query_results = service.run_batch(people=people)
    except Exception as exc:
        logger.error(f"批量查询引擎故障: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"批量查询引擎故障: {exc}")

    # 2. 批量回填
    response_results: List[ResultItem] = []
    success_count = 0
    
    for result in query_results:
        writeback_error: Optional[str] = None
        person_success = False
        
        if result.status == "success":
            try:
                # 构造回填字段并执行更新
                fields = service._build_feishu_fields(result, field_mapping)
                if service.feishu_client.update_record(result.record_id, fields):
                    person_success = True
                else:
                    writeback_error = "飞书接口更新失败"
            except Exception as e:
                logger.warning(f"记录 {result.record_id} 回填异常: {e}")
                writeback_error = str(e)
        else:
            writeback_error = "查询未成功，跳过回填"

        if person_success:
            success_count += 1

        item = ResultItem(
            record_id=result.record_id,
            success=person_success,
        )
        if request.debug:
            item.query_status = result.status
            item.query_error = result.error
            item.writeback_error = writeback_error
            
        response_results.append(item)

    failed_count = len(response_results) - success_count
    return BatchQueryResponse(
        total=len(response_results),
        success=success_count,
        failed=failed_count,
        results=response_results,
    )


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 58000))
    uvicorn.run(app, host="0.0.0.0", port=port)
