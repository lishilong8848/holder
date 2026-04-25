from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .photo_ai_recognition import (
    DEFAULT_QWEN_BASE_URL,
    DEFAULT_QWEN_MODEL,
    call_qwen_vision,
    get_qwen_api_key,
    summarize_recognition_results,
)
from .task_registry import TASK_REGISTRY, TASK_STATUS_FAILED, TASK_STATUS_SUCCESS


DEFAULT_PHOTO_AI_TABLE_ID = "tbl4exCKodfhYCXQ"
DEFAULT_PHOTO_AI_REQUIREMENT_APP_TOKEN = "X9CwbB3zhaLK7JsQZVZcFR9fnTf"
DEFAULT_PHOTO_AI_REQUIREMENT_TABLE_ID = "tblSoK80m57QrVLj"
PROCESS_FEEDBACK_FIELD = "AI识别反馈（过程）"
FINAL_FEEDBACK_FIELD = "AI识别反馈（收尾）"
PROCESS_PHOTO_FIELD = "施工过程照片"
FINAL_PHOTO_FIELD = "施工收尾照片"
PHOTO_AI_JOB_TYPE_FIELD = "作业类型"
REQUIREMENT_JOB_TYPE_FIELD = "作业类型"
REQUIREMENT_PROCESS_FIELD = "施工过程要求"
REQUIREMENT_FINAL_FIELD = "施工结束收尾"
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".bmp"}


@dataclass(frozen=True)
class PhotoAiRequirement:
    job_type: str
    process_requirement: str
    final_requirement: str


def get_photo_ai_table_id() -> str:
    return os.environ.get("FEISHU_PHOTO_AI_TABLE_ID", DEFAULT_PHOTO_AI_TABLE_ID).strip() or DEFAULT_PHOTO_AI_TABLE_ID


def get_photo_ai_requirement_app_token() -> str:
    return (
        os.environ.get("FEISHU_PHOTO_AI_REQUIREMENT_APP_TOKEN", DEFAULT_PHOTO_AI_REQUIREMENT_APP_TOKEN).strip()
        or DEFAULT_PHOTO_AI_REQUIREMENT_APP_TOKEN
    )


def get_photo_ai_requirement_table_id() -> str:
    return (
        os.environ.get("FEISHU_PHOTO_AI_REQUIREMENT_TABLE_ID", DEFAULT_PHOTO_AI_REQUIREMENT_TABLE_ID).strip()
        or DEFAULT_PHOTO_AI_REQUIREMENT_TABLE_ID
    )


def field_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("text", "value"):
            if value.get(key) is not None:
                return field_text(value.get(key))
        return str(value).strip()
    if isinstance(value, list):
        parts = []
        for item in value:
            text = field_text(item)
            if text:
                parts.append(text)
        return "\n".join(parts).strip()
    return str(value).strip()


def iter_image_attachments(field_value: Any) -> List[Dict[str, Any]]:
    if not isinstance(field_value, list):
        return []

    images: List[Dict[str, Any]] = []
    for item in field_value:
        if not isinstance(item, dict):
            continue
        file_token = str(item.get("file_token") or "").strip()
        if not file_token:
            continue

        filename = str(item.get("name") or "").strip()
        file_type = str(item.get("type") or "").strip().lower()
        suffix = Path(filename).suffix.lower()
        if file_type.startswith("image/") or suffix in IMAGE_EXTENSIONS:
            images.append(item)
    return images


def safe_filename(filename: str, fallback: str) -> str:
    cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", str(filename or "").strip())
    cleaned = cleaned.strip(". ")
    return cleaned or fallback


def select_photo_ai_fields(fields: Dict[str, Any]) -> Tuple[str, str, str]:
    process_feedback = field_text(fields.get(PROCESS_FEEDBACK_FIELD))
    if process_feedback:
        return FINAL_PHOTO_FIELD, FINAL_FEEDBACK_FIELD, "收尾"
    return PROCESS_PHOTO_FIELD, PROCESS_FEEDBACK_FIELD, "过程"


def _failure_text(photo_field: str, reason: str) -> str:
    return f"照片AI识别失败：{photo_field} {reason}"


def normalize_job_type(value: Any) -> str:
    return re.sub(r"\s+", "", field_text(value))


def extract_job_types(value: Any) -> List[str]:
    raw_items: List[str] = []
    if isinstance(value, list):
        for item in value:
            text = field_text(item)
            if text:
                raw_items.append(text)
    else:
        text = field_text(value)
        if text:
            raw_items.append(text)

    job_types: List[str] = []
    seen = set()
    for raw_item in raw_items:
        for part in re.split(r"[\n\r,，;；]+", raw_item):
            job_type = part.strip()
            if not job_type:
                continue
            normalized = normalize_job_type(job_type)
            if normalized in seen:
                continue
            seen.add(normalized)
            job_types.append(job_type)
    return job_types


def _record_fields(record: Any) -> Dict[str, Any]:
    if isinstance(record, dict):
        fields = record.get("fields", record)
        return fields if isinstance(fields, dict) else {}
    fields = getattr(record, "fields", None)
    return fields if isinstance(fields, dict) else {}


def _build_requirement_reader(feishu_client: Any):
    app_token = get_photo_ai_requirement_app_token()
    raw_app_token = str(getattr(feishu_client, "raw_app_token", "") or "")
    if not app_token or app_token == raw_app_token:
        return feishu_client

    app_id = str(getattr(feishu_client, "app_id", "") or "")
    app_secret = str(getattr(feishu_client, "app_secret", "") or "")
    if not app_id or not app_secret:
        return feishu_client

    from .feishu_reader import FeishuTableReader

    return FeishuTableReader(
        app_id=app_id,
        app_secret=app_secret,
        app_token=app_token,
        table_id=get_photo_ai_requirement_table_id(),
    )


def load_photo_ai_requirements(feishu_client: Any) -> List[PhotoAiRequirement]:
    table_id = get_photo_ai_requirement_table_id()
    reader = _build_requirement_reader(feishu_client)
    if not hasattr(reader, "list_records"):
        print("[照片AI] 飞书客户端不支持读取作业类型规范表")
        return []

    try:
        records = reader.list_records(
            field_names=[REQUIREMENT_JOB_TYPE_FIELD, REQUIREMENT_PROCESS_FIELD, REQUIREMENT_FINAL_FIELD],
            table_id=table_id,
        )
    except TypeError:
        records = reader.list_records(
            field_names=[REQUIREMENT_JOB_TYPE_FIELD, REQUIREMENT_PROCESS_FIELD, REQUIREMENT_FINAL_FIELD],
        )
    except Exception as exc:
        print(f"[照片AI] 读取作业类型规范表失败: {exc}")
        return []

    requirements: List[PhotoAiRequirement] = []
    for record in records:
        fields = _record_fields(record)
        job_type = field_text(fields.get(REQUIREMENT_JOB_TYPE_FIELD))
        if not job_type:
            continue
        requirements.append(
            PhotoAiRequirement(
                job_type=job_type,
                process_requirement=field_text(fields.get(REQUIREMENT_PROCESS_FIELD)),
                final_requirement=field_text(fields.get(REQUIREMENT_FINAL_FIELD)),
            )
        )
    print(f"[照片AI] 已读取作业类型规范 {len(requirements)} 条")
    return requirements


def match_photo_ai_requirement(job_type: str, requirements: List[PhotoAiRequirement]) -> Optional[PhotoAiRequirement]:
    normalized_job_type = normalize_job_type(job_type)
    if not normalized_job_type:
        return None

    for item in requirements:
        if normalize_job_type(item.job_type) == normalized_job_type:
            return item

    for item in requirements:
        normalized_item = normalize_job_type(item.job_type)
        if not normalized_item:
            continue
        if normalized_item in normalized_job_type or normalized_job_type in normalized_item:
            return item
    return None


def match_photo_ai_requirements(
    job_types: List[str],
    requirements: List[PhotoAiRequirement],
) -> List[PhotoAiRequirement]:
    matched: List[PhotoAiRequirement] = []
    seen = set()
    for job_type in job_types:
        requirement = match_photo_ai_requirement(job_type, requirements)
        if not requirement:
            continue
        normalized = normalize_job_type(requirement.job_type)
        if normalized in seen:
            continue
        seen.add(normalized)
        matched.append(requirement)
    return matched


def combine_photo_ai_requirements(
    job_types: List[str],
    requirements: List[PhotoAiRequirement],
) -> PhotoAiRequirement:
    process_blocks = []
    final_blocks = []
    for requirement in requirements:
        if requirement.process_requirement:
            process_blocks.append(f"【{requirement.job_type}】\n{requirement.process_requirement}")
        if requirement.final_requirement:
            final_blocks.append(f"【{requirement.job_type}】\n{requirement.final_requirement}")

    return PhotoAiRequirement(
        job_type="、".join(job_types),
        process_requirement="\n\n".join(process_blocks),
        final_requirement="\n\n".join(final_blocks),
    )


def process_photo_ai_record(
    record_id: str,
    feishu_client,
    *,
    chat_id: Optional[str] = None,
    project_root: Optional[Path] = None,
    task_id: Optional[str] = None,
) -> None:
    table_id = get_photo_ai_table_id()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = Path(project_root) if project_root else Path(__file__).resolve().parents[1]
    download_dir = root / "output" / "photo_ai" / f"{record_id}_{timestamp}"
    download_dir.mkdir(parents=True, exist_ok=True)

    print(f"[照片AI] 开始处理记录: {record_id}")
    print(f"[照片AI] 表ID: {table_id}")
    TASK_REGISTRY.update_task(task_id, current_step="读取照片AI记录")

    fields = feishu_client.get_record(record_id, table_id=table_id)
    if not fields:
        message = f"未找到记录 {record_id}"
        print(f"[照片AI] {message}，退出")
        TASK_REGISTRY.finish_task(task_id, status=TASK_STATUS_FAILED, current_step=message, error=message)
        return

    photo_field, target_field, phase = select_photo_ai_fields(fields)
    attachments = iter_image_attachments(fields.get(photo_field))
    print(f"[照片AI] 阶段: {phase}, 图片字段: {photo_field}, 回填字段: {target_field}, 图片数: {len(attachments)}")
    TASK_REGISTRY.update_task(
        task_id,
        current_step=f"准备识别{phase}照片",
        progress_current=0,
        progress_total=len(attachments) + 2 if attachments else 0,
        summary={
            "phase": phase,
            "photo_field": photo_field,
            "target_field": target_field,
            "image_count": len(attachments),
        },
    )

    if not attachments:
        feedback = _failure_text(photo_field, "未找到可识别图片。")
        feishu_client.update_record(record_id, {target_field: feedback}, table_id=table_id)
        print(f"[照片AI] {feedback}")
        TASK_REGISTRY.finish_task(task_id, status=TASK_STATUS_FAILED, current_step="未找到可识别图片", error=feedback)
        return

    api_key = get_qwen_api_key()
    if not api_key:
        feedback = "照片AI识别失败：未配置 QWEN_API_KEY 或 DASHSCOPE_API_KEY。"
        feishu_client.update_record(record_id, {target_field: feedback}, table_id=table_id)
        print(f"[照片AI] {feedback}")
        TASK_REGISTRY.finish_task(task_id, status=TASK_STATUS_FAILED, current_step="缺少千问 API Key", error=feedback)
        return

    image_results: List[Tuple[str, str]] = []
    failed_files: List[str] = []
    job_types = extract_job_types(fields.get(PHOTO_AI_JOB_TYPE_FIELD))
    matched_requirements: List[PhotoAiRequirement] = []
    combined_requirement: Optional[PhotoAiRequirement] = None
    if job_types:
        requirement_records = load_photo_ai_requirements(feishu_client)
        matched_requirements = match_photo_ai_requirements(job_types, requirement_records)
        combined_requirement = combine_photo_ai_requirements(job_types, matched_requirements)
        matched_names = "、".join(item.job_type for item in matched_requirements) or "未匹配到规范"
        print(f"[照片AI] 记录作业类型: {'、'.join(job_types)}")
        print(f"[照片AI] 匹配作业类型规范: {matched_names}")
    else:
        print("[照片AI] 记录未填写作业类型，按通用规则识别")

    for index, item in enumerate(attachments, start=1):
        file_token = str(item.get("file_token") or "").strip()
        original_name = str(item.get("name") or f"photo_{index}.jpg").strip()
        filename = safe_filename(original_name, f"photo_{index}.jpg")
        direct_url = item.get("url") or item.get("tmp_url")

        file_bytes = feishu_client.download_media(
            file_token=file_token,
            table_id=table_id,
            record_id=record_id,
            field_id_or_name=photo_field,
            direct_url=direct_url,
        )
        if not file_bytes:
            failed_files.append(original_name)
            print(f"[照片AI] 图片下载失败: {original_name}")
            TASK_REGISTRY.add_detail(
                task_id,
                label=original_name,
                status="failed",
                message="图片下载失败",
            )
            TASK_REGISTRY.update_task(task_id, current_step=f"图片处理 {index}/{len(attachments)}", progress_current=index)
            continue

        local_path = download_dir / filename
        local_path.write_bytes(file_bytes)
        print(f"[照片AI] 已保存图片: {local_path}")

        try:
            result = call_qwen_vision(
                api_key=api_key,
                image_path=local_path,
                phase=phase,
                job_type=combined_requirement.job_type if combined_requirement else "",
                process_requirement=combined_requirement.process_requirement if combined_requirement else "",
                final_requirement=combined_requirement.final_requirement if combined_requirement else "",
                model=os.environ.get("QWEN_VISION_MODEL", DEFAULT_QWEN_MODEL).strip() or DEFAULT_QWEN_MODEL,
                base_url=os.environ.get("QWEN_BASE_URL", DEFAULT_QWEN_BASE_URL).strip() or DEFAULT_QWEN_BASE_URL,
            )
        except Exception as exc:
            failed_files.append(original_name)
            print(f"[照片AI] 图片识别失败: {original_name} - {exc}")
            TASK_REGISTRY.add_detail(
                task_id,
                label=original_name,
                status="failed",
                message=f"图片识别失败: {exc}",
            )
            TASK_REGISTRY.update_task(task_id, current_step=f"图片处理 {index}/{len(attachments)}", progress_current=index)
            continue

        image_results.append((original_name, result))
        print(f"[照片AI] 图片识别完成: {original_name}")
        TASK_REGISTRY.add_detail(
            task_id,
            label=original_name,
            status="success",
            message="图片识别完成",
            extra={"local_path": str(local_path)},
        )
        TASK_REGISTRY.update_task(task_id, current_step=f"图片处理 {index}/{len(attachments)}", progress_current=index)

    if not image_results:
        feedback = _failure_text(photo_field, "图片全部识别失败。")
        feishu_client.update_record(record_id, {target_field: feedback}, table_id=table_id)
        print(f"[照片AI] {feedback}")
        TASK_REGISTRY.finish_task(task_id, status=TASK_STATUS_FAILED, current_step="图片全部识别失败", error=feedback)
        return

    try:
        TASK_REGISTRY.update_task(
            task_id,
            current_step="融合多图识别结果",
            progress_current=len(attachments) + 1,
        )
        feedback = summarize_recognition_results(
            image_results,
            api_key=api_key,
            phase=phase,
            job_type=combined_requirement.job_type if combined_requirement else "",
            process_requirement=combined_requirement.process_requirement if combined_requirement else "",
            final_requirement=combined_requirement.final_requirement if combined_requirement else "",
            model=os.environ.get("QWEN_VISION_MODEL", DEFAULT_QWEN_MODEL).strip() or DEFAULT_QWEN_MODEL,
            base_url=os.environ.get("QWEN_BASE_URL", DEFAULT_QWEN_BASE_URL).strip() or DEFAULT_QWEN_BASE_URL,
        )
    except Exception as exc:
        feedback = "\n\n".join(f"{filename}\n{result}" for filename, result in image_results)
        print(f"[照片AI] 多图汇总失败，改用逐图结果拼接: {exc}")

    if failed_files:
        feedback = f"{feedback}\n\n未成功识别图片：{', '.join(failed_files)}"

    TASK_REGISTRY.update_task(task_id, current_step=f"回填 {target_field}")
    updated = feishu_client.update_record(record_id, {target_field: feedback}, table_id=table_id)
    if updated:
        print(f"[照片AI] 已回填 {target_field}: {record_id}")
        TASK_REGISTRY.finish_task(
            task_id,
            status=TASK_STATUS_SUCCESS,
            current_step="处理完成",
            summary={
                "recognized_images": len(image_results),
                "failed_images": len(failed_files),
                "target_field": target_field,
                "job_types": job_types,
                "requirement_job_types": [item.job_type for item in matched_requirements],
            },
        )
    else:
        print(f"[照片AI] 回填失败 {target_field}: {record_id}")
        TASK_REGISTRY.finish_task(
            task_id,
            status=TASK_STATUS_FAILED,
            current_step="回填失败",
            error=f"回填失败: {target_field}",
            summary={
                "recognized_images": len(image_results),
                "failed_images": len(failed_files),
                "target_field": target_field,
                "job_types": job_types,
                "requirement_job_types": [item.job_type for item in matched_requirements],
            },
        )

    if chat_id:
        print(f"[照片AI] 触发群: {chat_id}")
