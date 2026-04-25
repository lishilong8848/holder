"""
飞书群消息触发的证书查询与回填业务逻辑

流程：
1. 从消息中提取 记录ID
2. 查询飞书表获取记录，下载附件
3. 解析施工人员信息表 Excel
4. 筛选有操作权限的人员，查询证书
5. 将结果写回飞书表
"""

import json
import os
import re
import logging
import threading
import ast
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from queue import Queue
from typing import Any, Dict, List, Optional, Set

import openpyxl

logger = logging.getLogger(__name__)

# 飞书表配置
DEFAULT_SOURCE_TABLE_ID = "tblWHIbp172MNjM1"    # 施工单所在表
DEFAULT_TARGET_TABLE_ID = "tblgJT6SXjjw7iYN"    # 证书查询结果写入表

WORKFLOW_CERTIFICATE = "certificate"
WORKFLOW_PHOTO_AI = "photo_ai"
CERTIFICATE_TITLE_KEYWORD = "特种作业查证"
PHOTO_AI_TITLE_KEYWORD = "照片AI识别"

# 模糊匹配标题关键词
NAME_KEYWORDS = ["姓名", "名字"]
ID_KEYWORDS = ["证件号码", "身份证号"]
PHONE_KEYWORDS = ["手机", "联系方式", "手机号"]
PERMISSION_KEYWORDS = ["特殊作业", "作业权限", "是否有特殊", "特殊作业权限"]
GENDER_KEYWORDS = ["性别"]
JOB_TYPE_KEYWORDS = ["作业类型"]

# 证书类型映射（从证书系统查询结果到中文名）
CERT_TYPE_DISPLAY = {
    "high_voltage": "高压电工作业",
    "low_voltage": "低压电工作业",
    "refrigeration": "制冷与空调设备运行操作作业",
    "working_at_height": "高处安装、维护、拆除作业",
}

FIELD_MAPPING = {
    "high_voltage": {
        "attachment_field": "高压证",
        "expire_field": "高压证-到期日期",
        "review_due_field": "高压证-应复审日期",
        "review_actual_field": "高压证-实际复审日期",
    },
    "low_voltage": {
        "attachment_field": "低压证",
        "expire_field": "低压证-到期日期",
        "review_due_field": "低压证-应复审日期",
        "review_actual_field": "低压证-实际复审日期",
    },
    "refrigeration": {
        "attachment_field": "制冷证",
        "expire_field": "制冷证-到期日期",
        "review_due_field": "制冷证-应复审日期",
        "review_actual_field": "制冷证-实际复审日期",
    },
    "working_at_height": {
        "attachment_field": "登高证",
        "expire_field": "登高证-到期日期",
        "review_due_field": "登高证-应复审日期",
        "review_actual_field": "登高证-实际复审日期",
    },
    "welding": {
        "attachment_field": "焊接证",
        "expire_field": "焊接证-到期日期",
        "review_due_field": "焊接证-应复审日期",
        "review_actual_field": "焊接证-实际复审日期",
    },
}

SUMMARY_MESSAGE_CHUNK_SIZE = 3500
EFFECTIVE_END_FIELD = "有效期结束日期"
REVIEW_ACTUAL_FIELD = "实际复审日期"
DEFAULT_RECORD_DEDUP_TTL_SECONDS = 24 * 60 * 60
QUERY_STATUS_DISPLAY = {
    "success": "查询成功",
    "fail_id": "证件号有误",
    "fail_no_data": "未查询到证件信息",
    "fail_other": "查询失败",
}


@dataclass
class SummaryCertificate:
    label: str
    expire_date: str
    review_actual_date: str


@dataclass
class SummaryPerson:
    name: str
    job_type: str
    write_created: bool
    status: str = ""
    query_error: str = ""
    certificates: List[SummaryCertificate] = field(default_factory=list)


@dataclass
class MessageTrigger:
    workflow: str
    record_id: str
    chat_id: Optional[str]
    title: str


@dataclass
class RecordProcessingContext:
    source_record_id: str
    shigong_code: str
    target_table_id: str
    download_dir: Path
    timestamp: str
    personnel_count: int
    query_required_count: int
    direct_write_count: int
    expected_write_tasks: int
    feishu_client: Any
    service: Any
    chat_id: Optional[str] = None
    task_id: Optional[str] = None
    created_count: int = 0
    completed_write_tasks: int = 0
    query_successes: List[SummaryPerson] = field(default_factory=list)
    query_failures: List[SummaryPerson] = field(default_factory=list)
    direct_writebacks: List[SummaryPerson] = field(default_factory=list)
    done_event: threading.Event = field(default_factory=threading.Event)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def mark_write_completed(
        self,
        *,
        created: bool,
        person: Dict[str, str],
        query_result: Optional[Any],
    ) -> None:
        with self.lock:
            summary_person = build_summary_person(
                person=person,
                write_created=created,
                query_result=query_result,
            )
            if query_result is None:
                self.direct_writebacks.append(summary_person)
            elif is_successful_query_result(query_result):
                self.query_successes.append(summary_person)
            else:
                self.query_failures.append(summary_person)

            if created:
                self.created_count += 1
            self.completed_write_tasks += 1
            update_ui_task(
                self.task_id,
                current_step=f"人员回填进度 {self.completed_write_tasks}/{self.expected_write_tasks}",
                progress_current=self.completed_write_tasks,
                progress_total=self.expected_write_tasks,
                summary={
                    "created_count": self.created_count,
                    "completed_write_tasks": self.completed_write_tasks,
                },
            )
            add_ui_task_detail(
                self.task_id,
                label=str(person.get("name") or "未知人员"),
                status="success" if created else "failed",
                message=summary_person.status or ("回填成功" if created else "回填失败"),
            )
            if self.completed_write_tasks >= self.expected_write_tasks:
                self.done_event.set()

    def build_summary_snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "record_id": self.source_record_id,
                "shigong_code": self.shigong_code,
                "timestamp": self.timestamp,
                "personnel_count": self.personnel_count,
                "query_required_count": self.query_required_count,
                "direct_write_count": self.direct_write_count,
                "created_records": self.created_count,
                "completed_write_tasks": self.completed_write_tasks,
                "query_successes": [asdict(item) for item in self.query_successes],
                "query_failures": [asdict(item) for item in self.query_failures],
                "direct_writebacks": [asdict(item) for item in self.direct_writebacks],
                "download_dir": str(self.download_dir),
            }


@dataclass
class QueryTask:
    context: RecordProcessingContext
    query_record_id: str
    person: Dict[str, str]


@dataclass
class WriteTask:
    context: RecordProcessingContext
    person: Dict[str, str]
    query_result: Optional[Any] = None


def display_job_type(person: Dict[str, str]) -> str:
    return str(person.get("job_type") or "").strip() or "未填写"


def display_query_status(query_result: Any) -> str:
    status = str(getattr(query_result, "status", "") or "").strip()
    if status == "success" and not getattr(query_result, "selected_certificates", None):
        return "查询成功但未提取到证件"
    return QUERY_STATUS_DISPLAY.get(status, status or "未知状态")


def is_successful_query_result(query_result: Any) -> bool:
    return (
        str(getattr(query_result, "status", "") or "").strip() == "success"
        and bool(getattr(query_result, "selected_certificates", None))
    )


def get_certificate_label(cert_type: str) -> str:
    mapping = FIELD_MAPPING.get(cert_type) or {}
    return mapping.get("attachment_field") or cert_type


def get_certificate_field_text(card: Any, field_name: str) -> str:
    fields = getattr(card, "fields", None) or {}
    return str(fields.get(field_name) or "").strip() or "无"


def build_summary_certificates(query_result: Optional[Any]) -> List[SummaryCertificate]:
    selected = getattr(query_result, "selected_certificates", None) or {}
    certificates: List[SummaryCertificate] = []
    for cert_type, card in selected.items():
        certificates.append(
            SummaryCertificate(
                label=get_certificate_label(cert_type),
                expire_date=get_certificate_field_text(card, EFFECTIVE_END_FIELD),
                review_actual_date=get_certificate_field_text(card, REVIEW_ACTUAL_FIELD),
            )
        )
    return certificates


def build_summary_person(
    *,
    person: Dict[str, str],
    write_created: bool,
    query_result: Optional[Any],
) -> SummaryPerson:
    query_error = ""
    if query_result is not None:
        query_error = str(getattr(query_result, "error", "") or "").strip()

    return SummaryPerson(
        name=str(person.get("name") or "").strip(),
        job_type=display_job_type(person),
        write_created=write_created,
        status=display_query_status(query_result) if query_result is not None else "",
        query_error=query_error,
        certificates=build_summary_certificates(query_result),
    )


_GLOBAL_QUERY_QUEUE: Queue = Queue()
_GLOBAL_WRITE_QUEUE: Queue = Queue()
_workers_started = False
_workers_lock = threading.Lock()

RECORD_ID_LABEL_PATTERN = re.compile(
    r"(?:记录\s*ID|record[\s_-]*id)\s*[=＝:：]?\s*(rec[a-zA-Z0-9]{6,40})",
    re.IGNORECASE,
)
RECORD_ID_FALLBACK_PATTERN = re.compile(r"\b(rec[a-zA-Z0-9]{6,40})\b")
DEGRADED_CARD_HINT = "请升级至最新版本客户端，以查看内容"


def get_source_table_id() -> str:
    return os.environ.get("FEISHU_TABLE_ID", DEFAULT_SOURCE_TABLE_ID).strip() or DEFAULT_SOURCE_TABLE_ID


def get_target_table_id() -> str:
    return os.environ.get("FEISHU_TARGET_TABLE_ID", DEFAULT_TARGET_TABLE_ID).strip() or DEFAULT_TARGET_TABLE_ID


def _looks_like_structured_string(value: str) -> bool:
    text = value.strip()
    return bool(text) and text[0] in "[{" and text[-1] in "]}"


def _collect_text_fragments(value: Any, seen: Optional[Set[int]] = None) -> List[str]:
    """
    递归收集消息中所有可能包含记录 ID 的文本片段。
    同时兼容 JSON 字符串、字符串化 dict/list，以及普通嵌套结构。
    """
    if seen is None:
        seen = set()

    fragments: List[str] = []

    if value is None:
        return fragments

    if isinstance(value, (str, bytes, bytearray)):
        text = value.decode("utf-8", errors="ignore") if isinstance(value, (bytes, bytearray)) else value
        if text:
            fragments.append(text)
            structured_text = text.strip()
            if structured_text and _looks_like_structured_string(structured_text):
                for loader in (json.loads, ast.literal_eval):
                    try:
                        parsed = loader(structured_text)
                    except Exception:
                        continue
                    fragments.extend(_collect_text_fragments(parsed, seen))
                    break
        return fragments

    if isinstance(value, (int, float, bool)):
        return [str(value)]

    obj_id = id(value)
    if obj_id in seen:
        return fragments
    seen.add(obj_id)

    if isinstance(value, dict):
        for key, item in value.items():
            fragments.extend(_collect_text_fragments(key, seen))
            fragments.extend(_collect_text_fragments(item, seen))
        return fragments

    if isinstance(value, (list, tuple, set)):
        for item in value:
            fragments.extend(_collect_text_fragments(item, seen))
        return fragments

    return [str(value)]


def _normalize_search_text(parts: List[str]) -> str:
    text = " ".join(part for part in parts if part)
    text = re.sub(r"[\u200b-\u200d\ufeff]", "", text)
    text = text.replace("\\n", "\n").replace("\\r", "\r")
    return text


def _is_degraded_interactive_card(msg_data: Dict[str, Any], full_text: str) -> bool:
    if (msg_data.get("msg_type") or "").strip() != "interactive":
        return False
    return DEGRADED_CARD_HINT in full_text


def _extract_card_title_text(msg_data: Dict[str, Any]) -> str:
    content = msg_data.get("content")
    if isinstance(content, dict):
        title = str(content.get("title") or "").strip()
        if title:
            return title

    content_raw = msg_data.get("content_raw")
    if isinstance(content_raw, str) and content_raw.strip():
        for loader in (json.loads, ast.literal_eval):
            try:
                parsed = loader(content_raw)
            except Exception:
                continue
            if isinstance(parsed, dict):
                title = str(parsed.get("title") or "").strip()
                if title:
                    return title
            break

    return ""


def extract_record_id(msg_data: Dict[str, Any]) -> Optional[str]:
    """
    从消息内容中提取 记录ID。
    策略：直接在所有可能的字符串内容上跑正则，不依赖 JSON 解析结构。
    """
    priority_parts = _collect_text_fragments(
        {
            "content_raw": msg_data.get("content_raw", ""),
            "display_text": msg_data.get("display_text", ""),
            "content": msg_data.get("content", ""),
        }
    )
    full_parts = priority_parts + _collect_text_fragments(msg_data)
    full_text = _normalize_search_text(full_parts)
    title_text = _extract_card_title_text(msg_data)

    if title_text:
        title_match = RECORD_ID_LABEL_PATTERN.search(title_text) or RECORD_ID_FALLBACK_PATTERN.search(title_text)
        if title_match:
            rec_id = title_match.group(1).strip()
            print(f"[消息处理] 从卡片标题中提取到记录ID: {rec_id}", flush=True)
            return rec_id

    m = RECORD_ID_LABEL_PATTERN.search(full_text)
    if m:
        rec_id = m.group(1).strip()
        print(f"[消息处理] 从消息中提取到记录ID: {rec_id}", flush=True)
        return rec_id

    m2 = RECORD_ID_FALLBACK_PATTERN.search(full_text)
    if m2:
        rec_id = m2.group(1).strip()
        print(f"[消息处理] 从消息中匹配到疑似记录ID: {rec_id}", flush=True)
        return rec_id

    if _is_degraded_interactive_card(msg_data, full_text):
        title = ""
        content = msg_data.get("content")
        if isinstance(content, dict):
            title = str(content.get("title") or "").strip()
        title = title or "未知卡片"
        print(
            (
                f"[消息处理] 卡片《{title}》未返回原始业务载荷，当前仅收到降级占位内容。"
                "这类飞书卡片无法从群消息接口恢复 record_id，请改为在多维表格自动化中直接调用 "
                "POST /api/trigger 并传入 record_id。"
            ),
            flush=True,
        )
        return None

    print(f"[消息处理] 未找到记录ID，消息预览: {full_text[:200]}", flush=True)
    return None


def _extract_trigger_title_text(msg_data: Dict[str, Any]) -> str:
    title = _extract_card_title_text(msg_data)
    if title:
        return title

    parts = _collect_text_fragments(
        {
            "display_text": msg_data.get("display_text", ""),
            "content_raw": msg_data.get("content_raw", ""),
            "content": msg_data.get("content", ""),
        }
    )
    return _normalize_search_text(parts).strip()


def _extract_record_id_from_trigger_title(title: str, keyword: str) -> Optional[str]:
    if keyword not in title:
        return None

    tail = title.split(keyword, 1)[1]
    match = RECORD_ID_LABEL_PATTERN.search(tail) or RECORD_ID_FALLBACK_PATTERN.search(tail)
    if not match:
        match = RECORD_ID_LABEL_PATTERN.search(title) or RECORD_ID_FALLBACK_PATTERN.search(title)
    if not match:
        return None
    return match.group(1).strip()


def parse_message_trigger(msg_data: Dict[str, Any]) -> Optional[MessageTrigger]:
    """按卡片标题关键字解析业务触发，避免任意 rec 文本误触发。"""
    title = _extract_trigger_title_text(msg_data)
    if not title:
        return None

    workflow = ""
    keyword = ""
    if CERTIFICATE_TITLE_KEYWORD in title:
        workflow = WORKFLOW_CERTIFICATE
        keyword = CERTIFICATE_TITLE_KEYWORD
    elif PHOTO_AI_TITLE_KEYWORD in title:
        workflow = WORKFLOW_PHOTO_AI
        keyword = PHOTO_AI_TITLE_KEYWORD
    else:
        return None

    record_id = _extract_record_id_from_trigger_title(title, keyword)
    if not record_id:
        print(f"[消息处理] 标题包含 {keyword}，但未找到记录ID，标题: {title[:200]}", flush=True)
        return None

    return MessageTrigger(
        workflow=workflow,
        record_id=record_id,
        chat_id=str(msg_data.get("chat_id") or "").strip() or None,
        title=title,
    )


def process_photo_ai_record_message(
    record_id: str,
    feishu_client,
    chat_id: Optional[str] = None,
    task_id: Optional[str] = None,
) -> None:
    from .photo_ai_handler import process_photo_ai_record

    process_photo_ai_record(record_id, feishu_client, chat_id=chat_id, task_id=task_id)


def create_ui_task(*, workflow: str, record_id: str, source: str, current_step: str = "已创建") -> Optional[str]:
    try:
        from .task_registry import TASK_REGISTRY

        task = TASK_REGISTRY.create_task(
            workflow=workflow,
            record_id=record_id,
            source=source,
            current_step=current_step,
        )
        return task.task_id
    except Exception as exc:
        print(f"[任务中心] 创建任务失败: {exc}", flush=True)
        return None


def update_ui_task(task_id: Optional[str], **kwargs) -> None:
    if not task_id:
        return
    try:
        from .task_registry import TASK_REGISTRY

        TASK_REGISTRY.update_task(task_id, **kwargs)
    except Exception as exc:
        print(f"[任务中心] 更新任务失败: {exc}", flush=True)


def add_ui_task_detail(task_id: Optional[str], **kwargs) -> None:
    if not task_id:
        return
    try:
        from .task_registry import TASK_REGISTRY

        TASK_REGISTRY.add_detail(task_id, **kwargs)
    except Exception as exc:
        print(f"[任务中心] 写入任务明细失败: {exc}", flush=True)


def finish_ui_task(task_id: Optional[str], **kwargs) -> None:
    if not task_id:
        return
    try:
        from .task_registry import TASK_REGISTRY

        TASK_REGISTRY.finish_task(task_id, **kwargs)
    except Exception as exc:
        print(f"[任务中心] 完成任务失败: {exc}", flush=True)



def _fuzzy_match_column(header: str, keywords: List[str]) -> bool:
    """模糊匹配列标题"""
    header_clean = header.strip()
    return any(kw in header_clean for kw in keywords)


def should_query_certificate(job_type_raw: Any) -> bool:
    """
    根据“作业类型”判定是否需要查询证书。

    规则：
    - 空值 / 缺失：不查询
    - 精确等于“一般作业”：不查询
    - 其他非空值：查询
    """
    job_type = str(job_type_raw or "").strip()
    if not job_type:
        return False
    return job_type != "一般作业"


def _find_column_indices(headers: List[str]) -> Dict[str, Optional[int]]:
    """根据模糊匹配找到各字段对应的列索引"""
    mapping = {
        "name": None,
        "id_number": None,
        "phone": None,
        "permission": None,
        "gender": None,
        "job_type": None,
    }

    for i, header in enumerate(headers):
        if not header:
            continue
        h = str(header).strip()
        if mapping["name"] is None and _fuzzy_match_column(h, NAME_KEYWORDS):
            mapping["name"] = i
        elif mapping["id_number"] is None and _fuzzy_match_column(h, ID_KEYWORDS):
            mapping["id_number"] = i
        elif mapping["phone"] is None and _fuzzy_match_column(h, PHONE_KEYWORDS):
            mapping["phone"] = i
        elif mapping["gender"] is None and _fuzzy_match_column(h, GENDER_KEYWORDS):
            mapping["gender"] = i
        elif mapping["job_type"] is None and _fuzzy_match_column(h, JOB_TYPE_KEYWORDS):
            mapping["job_type"] = i
        elif mapping["permission"] is None and _fuzzy_match_column(h, PERMISSION_KEYWORDS):
            mapping["permission"] = i
        elif mapping["gender"] is None and _fuzzy_match_column(h, GENDER_KEYWORDS):
            mapping["gender"] = i

    return mapping


def parse_excel_for_personnel(file_path: str) -> List[Dict[str, str]]:
    """
    解析施工人员信息表 Excel，返回全部人员列表
    每个人员: {"name": ..., "id_number": ..., "phone": ..., "gender": ..., "has_permission": True/False}
    has_permission 表示根据“作业类型”规则是否需要查询证书
    """
    wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
    ws = wb.active

    rows = list(ws.iter_rows(values_only=True))
    if len(rows) < 2:
        print("[消息处理] Excel 无有效数据行")
        return []

    # 第一行为标题
    headers = [str(cell or "").strip() for cell in rows[0]]
    col_map = _find_column_indices(headers)

    print(f"[消息处理] Excel 标题: {headers}")
    print(f"[消息处理] 列映射: {col_map}")

    if col_map["name"] is None or col_map["id_number"] is None:
        print("[消息处理] 未找到姓名或身份证列，跳过")
        return []

    personnel = []
    for row in rows[1:]:
        name = str(row[col_map["name"]] or "").strip()
        id_number = str(row[col_map["id_number"]] or "").strip()
        phone = ""
        if col_map["phone"] is not None:
            phone = str(row[col_map["phone"]] or "").strip()
            
        gender = ""
        if col_map["gender"] is not None:
            gender = str(row[col_map["gender"]] or "").strip()

        job_type_raw = ""
        if col_map["job_type"] is not None:
            job_type_raw = str(row[col_map["job_type"]] or "").strip()
        has_permission = should_query_certificate(job_type_raw)

        if name and id_number:
            personnel.append({
                "name": name,
                "id_number": id_number,
                "phone": phone,
                "gender": gender,
                "job_type": job_type_raw,
                "has_permission": has_permission,
            })

    wb.close()
    perm_count = sum(1 for p in personnel if p["has_permission"])
    print(f"[消息处理] 解析到 {len(personnel)} 名人员, 其中 {perm_count} 人根据作业类型需查证书")
    return personnel


def split_job_types(job_type_raw: str) -> List[str]:
    return [job_type.strip() for job_type in (job_type_raw or "").split("/") if job_type.strip()]


def merge_job_type_values(*job_type_values: str) -> str:
    merged: List[str] = []
    seen: Set[str] = set()
    for value in job_type_values:
        for job_type in split_job_types(value):
            if job_type in seen:
                continue
            seen.add(job_type)
            merged.append(job_type)
    return "/".join(merged)


def deduplicate_personnel(personnel: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """按姓名+身份证号合并人员，避免同一人在重复行或多个附件中重复回填。"""
    deduped: List[Dict[str, str]] = []
    by_key: Dict[tuple[str, str], Dict[str, str]] = {}

    for person in personnel:
        name = str(person.get("name") or "").strip()
        id_number = str(person.get("id_number") or "").strip().upper()
        if not name or not id_number:
            continue

        key = (name, id_number)
        existing = by_key.get(key)
        if existing is None:
            normalized = dict(person)
            normalized["name"] = name
            normalized["id_number"] = id_number
            normalized["phone"] = str(normalized.get("phone") or "").strip()
            normalized["gender"] = str(normalized.get("gender") or "").strip()
            normalized["job_type"] = str(normalized.get("job_type") or "").strip()
            normalized["has_permission"] = should_query_certificate(normalized["job_type"])
            by_key[key] = normalized
            deduped.append(normalized)
            continue

        if not existing.get("phone") and person.get("phone"):
            existing["phone"] = str(person.get("phone") or "").strip()
        if not existing.get("gender") and person.get("gender"):
            existing["gender"] = str(person.get("gender") or "").strip()

        existing["job_type"] = merge_job_type_values(
            existing.get("job_type") or "",
            str(person.get("job_type") or "").strip(),
        )
        existing["has_permission"] = bool(existing.get("has_permission")) or should_query_certificate(
            person.get("job_type")
        )

    duplicate_count = len(personnel) - len(deduped)
    if duplicate_count > 0:
        print(f"[消息处理] 已按姓名+身份证号去重 {duplicate_count} 条重复人员记录")

    return deduped


def build_basic_person_fields(
    *,
    source_record_id: str,
    shigong_code: str,
    person: Dict[str, str],
) -> Dict[str, Any]:
    return {
        "关联施工单": [source_record_id],
        "施工编码": shigong_code,
        "姓名": person["name"],
        "身份证号": person["id_number"],
        "手机号": person["phone"],
        "作业类型": split_job_types(person.get("job_type") or ""),
    }


def write_basic_person_record(
    *,
    feishu_client,
    target_table_id: str,
    source_record_id: str,
    shigong_code: str,
    person: Dict[str, str],
) -> bool:
    try:
        fields = build_basic_person_fields(
            source_record_id=source_record_id,
            shigong_code=shigong_code,
            person=person,
        )
        cleaned_fields = {key: value for key, value in fields.items() if value is not None}
        new_record_id = feishu_client.create_record(fields=cleaned_fields, table_id=target_table_id)
        if new_record_id:
            print(f"[消息处理] ✅ 已写入人员记录: {person['name']}")
            return True
        print(f"[消息处理] ❌ 写入人员记录失败: {person['name']}")
    except Exception as exc:
        print(f"[消息处理] ❌ 写入人员记录异常: {person['name']} - {exc}")
    return False


def write_query_result_record(
    *,
    feishu_client,
    service,
    target_table_id: str,
    source_record_id: str,
    shigong_code: str,
    person: Dict[str, str],
    query_result,
    download_dir: Path,
) -> bool:
    if query_result.status != "success" or not query_result.selected_certificates:
        print(f"[消息处理] {person['name']}: 查询状态={query_result.status}, 跳过回写")
        return False

    try:
        new_fields = service.build_feishu_fields(
            query_result,
            FIELD_MAPPING,
            record_reference=f"{source_record_id}_{person['id_number']}",
            save_dir=str(download_dir),
        )
        new_fields.update(
            build_basic_person_fields(
                source_record_id=source_record_id,
                shigong_code=shigong_code,
                person=person,
            )
        )

        cleaned_fields = {key: value for key, value in new_fields.items() if value is not None}
        new_record_id = feishu_client.create_record(fields=cleaned_fields, table_id=target_table_id)
        if new_record_id:
            print(f"[消息处理] ✅ 已创建证书记录: {person['name']}")
            return True
        print(f"[消息处理] ❌ 创建证书记录失败: {person['name']}")
    except Exception as exc:
        import traceback
        traceback.print_exc()
        print(f"[消息处理] ❌ 构建记录异常: {person['name']} - {exc}")
    return False


def display_timestamp(timestamp: str) -> str:
    try:
        return datetime.strptime(timestamp, "%Y%m%d_%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return timestamp


def display_write_status(write_created: bool) -> str:
    return "成功" if write_created else "失败"


def format_table_cell(value: Any) -> str:
    text = str(value or "").strip() or "无"
    return text.replace("|", "/").replace("\r", " ").replace("\n", " ")


def format_certificate_line(certificate: Dict[str, Any]) -> str:
    label = format_table_cell(certificate.get("label") or "未知证件")
    expire_date = format_table_cell(certificate.get("expire_date") or "无")
    review_actual_date = format_table_cell(certificate.get("review_actual_date") or "无")
    return f"{label} 到期:{expire_date} 实复:{review_actual_date}"


def format_stats_table(
    *,
    personnel_count: int,
    query_required_count: int,
    query_success_count: int,
    query_failure_count: int,
    direct_write_count: int,
    writeback_success_count: int,
    writeback_failure_count: int,
) -> List[str]:
    return [
        f"总人数 ：{personnel_count}",
        f"需查证件 ：{query_required_count}",
        f"查询成功 ：{query_success_count}",
        f"查询失败 ：{query_failure_count}",
        f"未查询直回填：{direct_write_count}",
        f"回填成功 ：{writeback_success_count}",
        f"回填失败 ：{writeback_failure_count}",
    ]


def format_success_people_table(people: List[Dict[str, Any]]) -> List[str]:
    if not people:
        return ["无"]

    lines = [
        "| 序号 | 姓名 | 作业类型 | 证件信息 |",
        "| ---: | --- | --- | --- |",
    ]
    for index, item in enumerate(people, start=1):
        name = format_table_cell(item.get("name") or "未知姓名")
        job_type = format_table_cell(item.get("job_type") or "未填写")
        certificates = item.get("certificates") or []
        certificate_lines = [format_certificate_line(cert) for cert in certificates] or ["无"]
        prefix = f"| {index} | {name} | {job_type} | "
        lines.append(f"{prefix}{certificate_lines[0]}")
        continuation_indent = " " * len(prefix)
        for certificate_line in certificate_lines[1:]:
            lines.append(f"{continuation_indent}{certificate_line}")
    return lines


def format_summary_section(
    *,
    people: List[Dict[str, Any]],
    mode: str,
) -> List[str]:
    if not people:
        return ["无"]

    if mode == "failure":
        lines = [
            "| 序号 | 姓名 | 作业类型 | 状态 |",
            "| ---: | --- | --- | --- |",
        ]
    else:
        lines = [
            "| 序号 | 姓名 | 作业类型 | 回填 |",
            "| ---: | --- | --- | --- |",
        ]

    for index, item in enumerate(people, start=1):
        name = format_table_cell(item.get("name") or "未知姓名")
        job_type = format_table_cell(item.get("job_type") or "未填写")
        if mode == "failure":
            lines.append(
                f"| {index} | {name} | {job_type} | {format_table_cell(item.get('status') or '未知状态')} |"
            )
        else:
            lines.append(
                f"| {index} | {name} | {job_type} | {display_write_status(bool(item.get('write_created')))} |"
            )
    return lines


def build_processing_summary_text(context: RecordProcessingContext) -> str:
    snapshot = context.build_summary_snapshot()
    completed_count = int(snapshot.get("completed_write_tasks") or 0)
    created_count = int(snapshot.get("created_records") or 0)
    writeback_failed_count = max(completed_count - created_count, 0)
    query_successes = snapshot.get("query_successes") or []
    query_failures = snapshot.get("query_failures") or []
    direct_writebacks = snapshot.get("direct_writebacks") or []

    lines = [
        "特种作业查证结果汇总",
        f"施工编码：{snapshot.get('shigong_code') or context.shigong_code or '未填写'}",
        f"处理时间：{display_timestamp(str(snapshot.get('timestamp') or context.timestamp))}",
        "",
        "统计汇总",
        *format_stats_table(
            personnel_count=int(snapshot.get("personnel_count") or 0),
            query_required_count=int(snapshot.get("query_required_count") or 0),
            query_success_count=len(query_successes),
            query_failure_count=len(query_failures),
            direct_write_count=int(snapshot.get("direct_write_count") or len(direct_writebacks)),
            writeback_success_count=created_count,
            writeback_failure_count=writeback_failed_count,
        ),
    ]
    return "\n".join(lines)


def split_summary_message(
    message: str,
    *,
    max_chars: int = SUMMARY_MESSAGE_CHUNK_SIZE,
) -> List[str]:
    message = message.strip()
    if not message:
        return []
    if len(message) <= max_chars:
        return [message]

    lines = message.splitlines()
    title = lines[0] if lines else "特种作业查证结果汇总"
    body_lines = lines[1:] if len(lines) > 1 else []
    reserved_chars = len(title) + 50
    body_limit = max(20, max_chars - reserved_chars)

    chunks: List[List[str]] = []
    current: List[str] = []
    current_len = 0

    def flush_current() -> None:
        nonlocal current, current_len
        if current:
            chunks.append(current)
            current = []
            current_len = 0

    for line in body_lines:
        if len(line) + 1 > body_limit:
            part_size = max(body_limit - 1, 1)
            line_parts = [line[i:i + part_size] for i in range(0, len(line), part_size)]
        else:
            line_parts = [line]

        for part in line_parts:
            part_len = len(part) + 1
            if current and current_len + part_len > body_limit:
                flush_current()
            current.append(part)
            current_len += part_len

    flush_current()
    if not chunks:
        chunks = [[]]

    total = len(chunks)
    return [
        f"{title}（第 {index}/{total} 段）\n" + "\n".join(chunk).strip()
        for index, chunk in enumerate(chunks, start=1)
    ]


def build_processing_summary_messages(
    context: RecordProcessingContext,
    *,
    max_chars: int = SUMMARY_MESSAGE_CHUNK_SIZE,
) -> List[str]:
    return split_summary_message(build_processing_summary_text(context), max_chars=max_chars)


def send_processing_summary_to_chat(context: RecordProcessingContext) -> None:
    chat_id = str(context.chat_id or "").strip()
    if not chat_id:
        print("[消息处理] 未获取到群 chat_id，跳过发送汇总消息")
        return

    send_text_message = getattr(context.feishu_client, "send_text_message", None)
    if not callable(send_text_message):
        print("[消息处理] 飞书客户端不支持发送文本消息，跳过发送汇总消息")
        return

    messages = build_processing_summary_messages(context)
    if not messages:
        return

    for index, message in enumerate(messages, start=1):
        try:
            sent = send_text_message(chat_id, message, receive_id_type="chat_id")
        except Exception as exc:
            print(f"[消息处理] 发送汇总消息异常: {exc}")
            return
        if not sent:
            print(f"[消息处理] 发送汇总消息失败: 第 {index}/{len(messages)} 段")
            return

    print(f"[消息处理] 已发送查询结果汇总到群: {chat_id}")


def query_person_worker(
    *,
    chrome_bin: Optional[str],
    chromedriver_path: Optional[str],
) -> None:
    from .certificate_query import CertificateQuery, PersonQueryResult

    print("[消息处理] 全局查询线程已启动")
    query_engine = None

    try:
        while True:
            task = _GLOBAL_QUERY_QUEUE.get()
            context = task.context
            person = task.person
            try:
                if query_engine is None:
                    query_engine = CertificateQuery(
                        chrome_bin=chrome_bin,
                        chromedriver_path=chromedriver_path,
                    )

                result = query_engine.query_person(
                    record_id=task.query_record_id,
                    name=person["name"],
                    id_number=person["id_number"],
                )
            except Exception as exc:
                if query_engine is not None:
                    try:
                        query_engine.close()
                    except Exception:
                        pass
                    query_engine = None

                error_text = f"查询线程异常: {exc}"
                if "Chrome 启动失败" in str(exc) or "目标站点首页未能在预期时间内就绪" in str(exc):
                    error_text = f"查询引擎初始化失败: {exc}"

                result = PersonQueryResult(
                    record_id=task.query_record_id,
                    name=person["name"],
                    id_number=person["id_number"],
                    status="fail_other",
                    error=error_text,
                )

            print(f"[消息处理] 查询线程完成: {context.source_record_id} / {person['name']} - {result.status}")
            _GLOBAL_WRITE_QUEUE.put(WriteTask(context=context, person=person, query_result=result))
    finally:
        if query_engine is not None:
            query_engine.close()
        print("[消息处理] 全局查询线程已结束")


def write_record_worker() -> None:
    print("[消息处理] 全局写入线程已启动")

    while True:
        task = _GLOBAL_WRITE_QUEUE.get()
        context = task.context

        if task.query_result is None:
            created = write_basic_person_record(
                feishu_client=context.feishu_client,
                target_table_id=context.target_table_id,
                source_record_id=context.source_record_id,
                shigong_code=context.shigong_code,
                person=task.person,
            )
        else:
            created = write_query_result_record(
                feishu_client=context.feishu_client,
                service=context.service,
                target_table_id=context.target_table_id,
                source_record_id=context.source_record_id,
                shigong_code=context.shigong_code,
                person=task.person,
                query_result=task.query_result,
                download_dir=context.download_dir,
            )

        context.mark_write_completed(
            created=created,
            person=task.person,
            query_result=task.query_result,
        )


def ensure_global_workers_started() -> None:
    global _workers_started

    with _workers_lock:
        if _workers_started:
            return

        query_thread = threading.Thread(
            target=query_person_worker,
            kwargs={
                "chrome_bin": os.environ.get("CHROME_BIN"),
                "chromedriver_path": os.environ.get("CHROMEDRIVER_PATH"),
            },
            name="global-query-worker",
            daemon=True,
        )
        query_thread.start()

        write_thread = threading.Thread(
            target=write_record_worker,
            name="global-write-worker",
            daemon=True,
        )
        write_thread.start()

        _workers_started = True
        print("[消息处理] 已启动全局查询/写入工作线程")


def process_record_message(
    record_id: str,
    feishu_client,
    chat_id: Optional[str] = None,
    task_id: Optional[str] = None,
) -> None:
    """
    完整的消息处理流程（在后台线程中执行）

    Args:
        record_id: 飞书表记录 ID
        feishu_client: FeishuTableReader 实例
        chat_id: 触发消息所在群 ID，用于处理完成后发送汇总
    """
    from .service import CertificateService

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    project_root = Path(__file__).resolve().parents[1]
    source_table_id = get_source_table_id()
    target_table_id = get_target_table_id()

    print(f"\n{'#'*60}")
    print(f"[消息处理] 开始处理记录: {record_id}")
    print(f"{'#'*60}")
    print(f"[消息处理] 源表ID: {source_table_id}")
    print(f"[消息处理] 回填表ID: {target_table_id}")

    # === 步骤 1：获取飞书记录 ===
    update_ui_task(task_id, current_step="查询飞书记录")
    print(f"[消息处理] 步骤1: 查询飞书记录...")
    fields = feishu_client.get_record(record_id, table_id=source_table_id)
    if not fields:
        message = f"未找到记录 {record_id}"
        print(f"[消息处理] {message}，退出")
        finish_ui_task(task_id, status="failed", current_step=message, error=message)
        return

    # 获取施工编码（源表公式字段，可能返回 dict / str化的dict / 纯文本）
    shigong_code_raw = fields.get("施工编码", "")
    print(f"[消息处理] 施工编码原始值: {repr(shigong_code_raw)}")
    if isinstance(shigong_code_raw, dict):
        shigong_code = shigong_code_raw.get("text", shigong_code_raw.get("value", ""))
    elif isinstance(shigong_code_raw, list):
        item = shigong_code_raw[0] if shigong_code_raw else ""
        shigong_code = item.get("text", item) if isinstance(item, dict) else str(item)
    elif isinstance(shigong_code_raw, str) and shigong_code_raw.startswith("{"):
        # 飞书有时把公式结果序列化为字符串化的字典，如 "{'text': 'SG02-1005', ...}"
        import ast
        try:
            parsed = ast.literal_eval(shigong_code_raw)
            shigong_code = parsed.get("text", parsed.get("value", shigong_code_raw)) if isinstance(parsed, dict) else str(parsed)
        except Exception:
            shigong_code = shigong_code_raw
    else:
        shigong_code = str(shigong_code_raw) if shigong_code_raw else ""
    print(f"[消息处理] 施工编码解析结果: {shigong_code}")

    print(f"[消息处理] 记录字段名: {list(fields.keys())}")

    # === 步骤 2：下载所有附件和图片 ===
    download_dir = project_root / "output" / "records" / f"{record_id}_{timestamp}"
    download_dir.mkdir(parents=True, exist_ok=True)

    excel_files = []

    for field_name, field_value in fields.items():
        # 附件字段是一个列表，每个元素有 file_token, name, type 等
        if not isinstance(field_value, list):
            continue

        for item in field_value:
            if not isinstance(item, dict):
                continue

            file_token = item.get("file_token")
            file_name = item.get("name", "unknown")
            direct_url = item.get("url") or item.get("tmp_url")
            
            if not file_token:
                continue

            file_bytes = feishu_client.download_media(
                file_token=file_token,
                table_id=source_table_id,
                record_id=record_id,
                field_id_or_name=field_name,
                direct_url=direct_url
            )
            if not file_bytes:
                print(f"[消息处理] 下载失败: {file_name}")
                continue

            # 保存到本地
            local_name = f"{record_id}_{timestamp}_{file_name}"
            local_path = download_dir / local_name
            with open(local_path, "wb") as f:
                f.write(file_bytes)
            print(f"[消息处理] 已保存: {local_path}")

            # 收集 Excel 文件
            if file_name.lower().endswith((".xlsx", ".xls")):
                excel_files.append(str(local_path))

    if not excel_files:
        message = "未找到 Excel 附件"
        print(f"[消息处理] {message}，退出")
        finish_ui_task(task_id, status="failed", current_step=message, error=message)
        return

    print(f"[消息处理] 找到 {len(excel_files)} 个 Excel 文件")

    # === 步骤 3：解析 Excel ===
    update_ui_task(task_id, current_step="解析 Excel 人员表")
    all_personnel = []
    for excel_path in excel_files:
        print(f"[消息处理] 解析 Excel: {excel_path}")
        try:
            personnel = parse_excel_for_personnel(excel_path)
            all_personnel.extend(personnel)
        except Exception as e:
            print(f"[消息处理] 解析 Excel 失败: {e}")

    if not all_personnel:
        message = "未解析到有效人员"
        print(f"[消息处理] {message}，退出")
        finish_ui_task(task_id, status="failed", current_step=message, error=message)
        return

    all_personnel = deduplicate_personnel(all_personnel)
    if not all_personnel:
        message = "人员去重后无有效人员"
        print(f"[消息处理] {message}，退出")
        finish_ui_task(task_id, status="failed", current_step=message, error=message)
        return

    # 按作业类型分组
    perm_personnel = [p for p in all_personnel if p["has_permission"]]
    no_perm_personnel = [p for p in all_personnel if not p["has_permission"]]
    print(f"[消息处理] 共 {len(all_personnel)} 人: {len(perm_personnel)} 人需查证书, {len(no_perm_personnel)} 人直接写入")
    update_ui_task(
        task_id,
        current_step="已解析人员，等待查询/回填",
        progress_current=0,
        progress_total=len(all_personnel),
        summary={
            "personnel_count": len(all_personnel),
            "query_required_count": len(perm_personnel),
            "direct_write_count": len(no_perm_personnel),
            "shigong_code": shigong_code,
        },
    )

    service = CertificateService(
        feishu_config={"app_id": "", "app_secret": "", "app_token": "", "table_id": ""},
        max_workers=1,
        chrome_bin=os.environ.get("CHROME_BIN"),
        chromedriver_path=os.environ.get("CHROMEDRIVER_PATH"),
    )
    service.feishu_client = feishu_client

    context = RecordProcessingContext(
        source_record_id=record_id,
        shigong_code=shigong_code,
        target_table_id=target_table_id,
        download_dir=download_dir,
        timestamp=timestamp,
        personnel_count=len(all_personnel),
        query_required_count=len(perm_personnel),
        direct_write_count=len(no_perm_personnel),
        expected_write_tasks=len(all_personnel),
        feishu_client=feishu_client,
        service=service,
        chat_id=chat_id,
        task_id=task_id,
    )

    ensure_global_workers_started()
    print("[消息处理] 步骤4: 主线程投递任务到全局写入/查询队列...")

    if no_perm_personnel:
        print(f"[消息处理] 投递 {len(no_perm_personnel)} 名普通人员到全局写入队列...")
        for person in no_perm_personnel:
            _GLOBAL_WRITE_QUEUE.put(WriteTask(context=context, person=person))

    if perm_personnel:
        print(f"[消息处理] 投递 {len(perm_personnel)} 名证书查询人员到全局查询队列...")
        for index, person in enumerate(perm_personnel):
            _GLOBAL_QUERY_QUEUE.put(
                QueryTask(
                    context=context,
                    query_record_id=f"msg_{record_id}_{index}",
                    person=person,
                )
            )

    context.done_event.wait()
    created_count = context.created_count

    # === 完成 ===
    print(f"\n{'#'*60}")
    print(f"[消息处理] 处理完成! 共创建 {created_count} 条记录 (普通人员 {len(no_perm_personnel)} + 证书人员 {len(perm_personnel)})")
    print(f"[消息处理] 附件保存目录: {download_dir}")
    print(f"{'#'*60}\n")

    # 保存处理结果到本地 JSON
    result_summary = context.build_summary_snapshot()
    summary_path = download_dir / "summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(result_summary, f, ensure_ascii=False, indent=2)

    send_processing_summary_to_chat(context)
    finish_ui_task(
        task_id,
        status="success",
        current_step="处理完成",
        summary={
            "created_count": created_count,
            "download_dir": str(download_dir),
            "summary_path": str(summary_path),
        },
    )


# 用于记录正在处理中和近期已处理的 ID，防止同一记录重复触发导致重复插入
_processing_ids = set()
_recent_processed_records: Dict[str, float] = {}
_lock = threading.Lock()


def build_processing_key(record_id: str, workflow: str = WORKFLOW_CERTIFICATE) -> str:
    record_id = str(record_id or "").strip()
    workflow = str(workflow or WORKFLOW_CERTIFICATE).strip() or WORKFLOW_CERTIFICATE
    return f"{workflow}:{record_id}"


def get_record_dedup_ttl_seconds() -> int:
    raw_value = os.environ.get("FEISHU_RECORD_DEDUP_TTL_SECONDS", "")
    if not raw_value:
        return DEFAULT_RECORD_DEDUP_TTL_SECONDS
    try:
        return max(0, int(raw_value))
    except ValueError:
        return DEFAULT_RECORD_DEDUP_TTL_SECONDS


def _prune_recent_processed_records(now: float) -> None:
    ttl_seconds = get_record_dedup_ttl_seconds()
    if ttl_seconds <= 0:
        _recent_processed_records.clear()
        return

    expired_ids = [
        record_id
        for record_id, processed_at in _recent_processed_records.items()
        if now - processed_at > ttl_seconds
    ]
    for record_id in expired_ids:
        _recent_processed_records.pop(record_id, None)


def claim_record_processing(record_id: str, *, workflow: str = WORKFLOW_CERTIFICATE) -> bool:
    record_id = str(record_id or "").strip()
    if not record_id:
        return False
    processing_key = build_processing_key(record_id, workflow)

    with _lock:
        now = time.time()
        _prune_recent_processed_records(now)
        if processing_key in _processing_ids:
            print(f"[消息处理] 记录 {processing_key} 正在处理中，跳过重复触发")
            return False
        if processing_key in _recent_processed_records:
            print(f"[消息处理] 记录 {processing_key} 已在近期处理过，跳过重复触发")
            return False

        _processing_ids.add(processing_key)
        return True


def finish_record_processing(
    record_id: str,
    *,
    workflow: str = WORKFLOW_CERTIFICATE,
    remember: bool = True,
) -> None:
    record_id = str(record_id or "").strip()
    if not record_id:
        return
    processing_key = build_processing_key(record_id, workflow)

    with _lock:
        _processing_ids.discard(processing_key)
        if remember and get_record_dedup_ttl_seconds() > 0:
            _recent_processed_records[processing_key] = time.time()


def handle_message_async(msg_data: Dict[str, Any], feishu_client) -> None:
    """
    异步处理消息（在新线程中执行，避免阻塞监听器）

    Args:
        msg_data: 飞书消息数据字典
        feishu_client: FeishuTableReader 实例
    """
    trigger = parse_message_trigger(msg_data)
    if not trigger:
        return

    if not claim_record_processing(trigger.record_id, workflow=trigger.workflow):
        return

    task_id = create_ui_task(
        workflow=trigger.workflow,
        record_id=trigger.record_id,
        source="group",
        current_step="群消息已触发",
    )
    print(
        (
            f"[消息处理] 检测到触发: workflow={trigger.workflow}, record_id={trigger.record_id} "
            f"(来源: {msg_data.get('msg_type', 'text')})，启动任务..."
        ),
        flush=True,
    )

    def _task_wrapper():
        try:
            if trigger.workflow == WORKFLOW_PHOTO_AI:
                process_photo_ai_record_message(
                    trigger.record_id,
                    feishu_client,
                    chat_id=trigger.chat_id,
                    task_id=task_id,
                )
            else:
                process_record_message(
                    trigger.record_id,
                    feishu_client,
                    chat_id=trigger.chat_id,
                    task_id=task_id,
                )
        except Exception as e:
            print(f"[消息处理] 处理记录 {trigger.workflow}:{trigger.record_id} 时发生未捕获异常: {e}")
            finish_ui_task(task_id, status="failed", current_step="任务异常", error=str(e))
        finally:
            finish_record_processing(trigger.record_id, workflow=trigger.workflow)

    thread = threading.Thread(
        target=_task_wrapper,
        name=f"msg-handler-{trigger.workflow}-{trigger.record_id}",
        daemon=True,
    )
    thread.start()
