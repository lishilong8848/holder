from __future__ import annotations

import base64
import json
import mimetypes
import os
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Iterable, List, Tuple


DEFAULT_QWEN_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
DEFAULT_QWEN_MODEL = "qwen-vl-max-latest"


def get_qwen_api_key(api_key: str = "") -> str:
    return (api_key or os.environ.get("QWEN_API_KEY") or os.environ.get("DASHSCOPE_API_KEY") or "").strip()


def image_to_data_url(image_path: Path) -> str:
    mime_type = mimetypes.guess_type(image_path.name)[0] or "image/jpeg"
    encoded = base64.b64encode(image_path.read_bytes()).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def build_image_prompt(image_path: Path, *, phase: str = "过程") -> str:
    return f"""
请识别这张施工{phase}照片中的可见内容，并做施工安全合规初判。

图片文件：{image_path.name}

重点关注：
1. 图片里有哪些人员、设备、工具、作业环境和明显动作。
2. 是否像登高作业、配电/电气作业、动火作业、吊装作业、有限空间作业等。
3. 如涉及登高作业，判断是否能看到安全帽、安全带/安全绳、挂点、防坠措施。
4. 如涉及配电或电气作业，判断是否能看到绝缘手套、绝缘鞋、验电/断电/警示隔离等防护。
5. 只根据图片可见信息判断；看不清或图片中没有出现的内容，必须写“不确定”或“未看到”，不要臆测。

请按以下格式输出中文结果：

图片内容概述：
作业类型初判：
可见合规项：
可见风险/不合规项：
不确定项：
综合结论：
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

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"千问接口返回 HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"千问接口请求失败: {exc.reason}") from exc


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
    model: str = DEFAULT_QWEN_MODEL,
    base_url: str = DEFAULT_QWEN_BASE_URL,
    timeout: int = 120,
) -> str:
    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": "你是施工现场安全合规巡检助手，擅长从施工照片中识别作业内容和可见安全风险。",
            },
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": build_image_prompt(image_path, phase=phase)},
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


def build_summary_prompt(results: Iterable[Tuple[str, str]], *, phase: str) -> str:
    blocks: List[str] = []
    for index, (filename, result) in enumerate(results, start=1):
        blocks.append(f"照片{index}：{filename}\n{result}")

    return f"""
下面是多张施工{phase}照片的逐图 AI 识别结果。请把它们融合、去重、简化为一段适合回填到多维表文本字段的中文反馈。

要求：
1. 不要逐字复述每张图片的原始识别结果。
2. 合并相同风险，只保留关键现场内容、合规情况、风险问题和整改建议。
3. 如果某项无法从照片确认，写“未确认”，不要臆测。
4. 输出控制在 800 字以内。

输出格式：
施工{phase}照片AI识别反馈：
现场内容：
合规情况：
风险问题：
整改建议：

逐图识别结果：
{chr(10).join(blocks)}
""".strip()


def summarize_recognition_results(
    results: List[Tuple[str, str]],
    *,
    api_key: str,
    phase: str,
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
                "content": "你是施工现场安全合规巡检助手，负责把多张照片识别结果融合成简洁、可执行的现场反馈。",
            },
            {"role": "user", "content": build_summary_prompt(results, phase=phase)},
        ],
        "temperature": 0.1,
        "max_tokens": 1000,
    }
    return _extract_message_content(
        _post_chat_completion(api_key=api_key, base_url=base_url, payload=payload, timeout=timeout)
    )
