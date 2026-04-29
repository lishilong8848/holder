from __future__ import annotations

import base64
import json
import mimetypes
import os
import socket
import ssl
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Iterable, List, Tuple


DEFAULT_QWEN_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
DEFAULT_QWEN_MODEL = "qwen-vl-max-latest"
DEFAULT_QWEN_RETRY_ATTEMPTS = 3
DEFAULT_QWEN_RETRY_BASE_DELAY_SECONDS = 1.0


def get_qwen_api_key(api_key: str = "") -> str:
    return (api_key or os.environ.get("QWEN_API_KEY") or os.environ.get("DASHSCOPE_API_KEY") or "").strip()


def image_to_data_url(image_path: Path) -> str:
    mime_type = mimetypes.guess_type(image_path.name)[0] or "image/jpeg"
    encoded = base64.b64encode(image_path.read_bytes()).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def _env_int(name: str, default: int, *, minimum: int = 1, maximum: int = 10) -> int:
    raw_value = os.environ.get(name, "").strip()
    if not raw_value:
        return default
    try:
        value = int(raw_value)
    except ValueError:
        return default
    return max(minimum, min(maximum, value))


def _env_float(name: str, default: float, *, minimum: float = 0.0, maximum: float = 30.0) -> float:
    raw_value = os.environ.get(name, "").strip()
    if not raw_value:
        return default
    try:
        value = float(raw_value)
    except ValueError:
        return default
    return max(minimum, min(maximum, value))


def get_qwen_retry_attempts() -> int:
    return _env_int("QWEN_API_RETRY_ATTEMPTS", DEFAULT_QWEN_RETRY_ATTEMPTS, minimum=1, maximum=10)


def get_qwen_retry_base_delay_seconds() -> float:
    return _env_float(
        "QWEN_API_RETRY_BASE_DELAY_SECONDS",
        DEFAULT_QWEN_RETRY_BASE_DELAY_SECONDS,
        minimum=0.0,
        maximum=30.0,
    )


def _is_retryable_http_status(status_code: int) -> bool:
    return status_code in {408, 409, 425, 429} or 500 <= status_code <= 599


def _is_retryable_url_error(exc: urllib.error.URLError) -> bool:
    reason = getattr(exc, "reason", exc)
    if isinstance(reason, (ssl.SSLError, socket.timeout, TimeoutError, ConnectionResetError)):
        return True
    text = str(reason).lower()
    retryable_fragments = (
        "eof occurred",
        "unexpected_eof",
        "timed out",
        "timeout",
        "connection reset",
        "remote end closed",
        "temporarily unavailable",
        "tlsv1_alert_internal_error",
        "ssl",
    )
    return any(fragment in text for fragment in retryable_fragments)


def _retry_delay_seconds(attempt: int) -> float:
    return get_qwen_retry_base_delay_seconds() * (2 ** max(0, attempt - 1))


def _sleep_before_retry(*, attempt: int, max_attempts: int, reason: str) -> None:
    delay = _retry_delay_seconds(attempt)
    print(f"[千问] 请求失败，{delay:g} 秒后重试 ({attempt}/{max_attempts - 1}): {reason}")
    if delay > 0:
        time.sleep(delay)


def _build_requirement_block(
    *,
    job_type: str = "",
    process_requirement: str = "",
    final_requirement: str = "",
) -> str:
    if not any((job_type, process_requirement, final_requirement)):
        return ""

    parts = ["\n规范依据："]
    if job_type:
        parts.append(f"作业类型：{job_type}")
    if process_requirement:
        parts.append(f"施工过程要求：\n{process_requirement}")
    if final_requirement:
        parts.append(f"施工结束收尾要求：\n{final_requirement}")
    parts.append("只能依据上述规范进行合规判断；不得补充、扩展或引用未出现在上述规范中的其他作业类型要求。")
    return "\n".join(parts)


def build_image_prompt(
    image_path: Path,
    *,
    phase: str = "过程",
    job_type: str = "",
    process_requirement: str = "",
    final_requirement: str = "",
) -> str:
    requirement_block = _build_requirement_block(
        job_type=job_type,
        process_requirement=process_requirement,
        final_requirement=final_requirement,
    )
    job_type_instruction = f"已知作业类型：{job_type}" if job_type else "记录未提供作业类型。"
    focus_job_type_line = "2. 只检查照片内容是否满足上方规范文本，不要检查其它作业类型的要求。"
    output_job_type_line = f"作业类型：{job_type}" if job_type else "作业类型：未填写"
    return f"""
请识别这张施工{phase}照片中的可见内容，并只根据下方规范文本做合规判断。

图片文件：{image_path.name}
{job_type_instruction}
{requirement_block}

判断边界：
1. 只描述图片中能看见、且与上方规范文本相关的人员、设备、工具、环境和动作。
{focus_job_type_line}
3. 上方规范未提到的内容不要回答，不要主动补充其它特种作业安全要求。
4. 照片看不清或未出现的规范项，写“未确认”或“未看到”，不要臆测。

请按以下格式输出中文结果：

图片内容概述：
{output_job_type_line}
按规范已确认：
按规范未确认/未看到：
按规范存在的问题：
结论：
""".strip()


def _post_chat_completion(
    *,
    api_key: str,
    base_url: str,
    payload: dict[str, Any],
    timeout: int,
) -> dict[str, Any]:
    url = base_url.rstrip("/") + "/chat/completions"
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )

    max_attempts = get_qwen_retry_attempts()
    for attempt in range(1, max_attempts + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            if attempt < max_attempts and _is_retryable_http_status(int(exc.code)):
                _sleep_before_retry(
                    attempt=attempt,
                    max_attempts=max_attempts,
                    reason=f"HTTP {exc.code}",
                )
                continue
            raise RuntimeError(f"千问接口返回 HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            if attempt < max_attempts and _is_retryable_url_error(exc):
                _sleep_before_retry(
                    attempt=attempt,
                    max_attempts=max_attempts,
                    reason=str(getattr(exc, "reason", exc)),
                )
                continue
            raise RuntimeError(f"千问接口请求失败: {exc.reason}") from exc

    raise RuntimeError("千问接口请求失败: 重试次数已耗尽")


def _extract_message_content(payload: dict[str, Any]) -> str:
    choices = payload.get("choices") or []
    if not choices:
        raise RuntimeError(f"千问接口未返回 choices: {json.dumps(payload, ensure_ascii=False)}")

    message = choices[0].get("message") or {}
    content = message.get("content")
    if isinstance(content, str):
        return content.strip()
    return json.dumps(content, ensure_ascii=False, indent=2)


def call_qwen_vision(
    *,
    api_key: str,
    image_path: Path,
    phase: str = "过程",
    job_type: str = "",
    process_requirement: str = "",
    final_requirement: str = "",
    model: str = DEFAULT_QWEN_MODEL,
    base_url: str = DEFAULT_QWEN_BASE_URL,
    timeout: int = 120,
) -> str:
    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": "你是施工现场安全合规巡检助手。必须只依据用户提供的规范文本进行判断，不得补充其它作业类型要求。",
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": build_image_prompt(
                            image_path,
                            phase=phase,
                            job_type=job_type,
                            process_requirement=process_requirement,
                            final_requirement=final_requirement,
                        ),
                    },
                    {"type": "image_url", "image_url": {"url": image_to_data_url(image_path)}},
                ],
            },
        ],
        "temperature": 0.1,
        "max_tokens": 1200,
    }
    return _extract_message_content(
        _post_chat_completion(api_key=api_key, base_url=base_url, payload=payload, timeout=timeout)
    )


def build_summary_prompt(
    results: Iterable[Tuple[str, str]],
    *,
    phase: str,
    job_type: str = "",
    process_requirement: str = "",
    final_requirement: str = "",
) -> str:
    blocks: List[str] = []
    for index, (filename, result) in enumerate(results, start=1):
        blocks.append(f"照片{index}：{filename}\n{result}")
    requirement_block = _build_requirement_block(
        job_type=job_type,
        process_requirement=process_requirement,
        final_requirement=final_requirement,
    )

    return f"""
下面是多张施工{phase}照片的逐图 AI 识别结果。请只根据下方规范文本与逐图结果融合、去重、简化为一段适合回填到多维表文本字段的中文反馈。
{requirement_block}

要求：
1. 不要逐字复述每张图片的原始识别结果。
2. 只回答当前作业类型匹配到的规范内容及其照片可见情况。
3. 不要补充其它特种作业类型的要求，不要泛化到规范文本外的安全要求。
4. 如果某项无法从照片确认，写“未确认”，不要臆测。
5. 输出控制在 800 字以内。

输出格式：
施工{phase}照片识别反馈：
作业类型：
现场可见内容：
按规范已确认：
按规范未确认/未看到：
按规范存在的问题：
处理建议：

逐图识别结果：
{chr(10).join(blocks)}
""".strip()


def summarize_recognition_results(
    results: List[Tuple[str, str]],
    *,
    api_key: str,
    phase: str,
    job_type: str = "",
    process_requirement: str = "",
    final_requirement: str = "",
    model: str = DEFAULT_QWEN_MODEL,
    base_url: str = DEFAULT_QWEN_BASE_URL,
    timeout: int = 120,
) -> str:
    if not results:
        return "照片AI识别失败：没有可用于汇总的图片识别结果。"

    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": "你是施工现场安全合规巡检助手。必须只依据用户提供的规范文本融合照片结果，不得补充其它作业类型要求。",
            },
            {
                "role": "user",
                "content": build_summary_prompt(
                    results,
                    phase=phase,
                    job_type=job_type,
                    process_requirement=process_requirement,
                    final_requirement=final_requirement,
                ),
            },
        ],
        "temperature": 0.1,
        "max_tokens": 1000,
    }
    return _extract_message_content(
        _post_chat_completion(api_key=api_key, base_url=base_url, payload=payload, timeout=timeout)
    )
