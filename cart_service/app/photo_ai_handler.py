from __future__ import annotations

import os
import re
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
PROCESS_FEEDBACK_FIELD = "AI识别反馈（过程）"
FINAL_FEEDBACK_FIELD = "AI识别反馈（收尾）"
PROCESS_PHOTO_FIELD = "施工过程照片"
FINAL_PHOTO_FIELD = "施工收尾照片"
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".bmp"}


def get_photo_ai_table_id() -> str:
    return os.environ.get("FEISHU_PHOTO_AI_TABLE_ID", DEFAULT_PHOTO_AI_TABLE_ID).strip() or DEFAULT_PHOTO_AI_TABLE_ID


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
            },
        )

    if chat_id:
        print(f"[照片AI] 触发群: {chat_id}")
