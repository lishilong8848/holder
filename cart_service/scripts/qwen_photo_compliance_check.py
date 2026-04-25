from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.photo_ai_recognition import (  # noqa: E402
    DEFAULT_QWEN_BASE_URL,
    DEFAULT_QWEN_MODEL,
    call_qwen_vision,
)


DEFAULT_IMAGES = [
    r"D:\下载\20260424-092136.jpg",
    r"D:\下载\20260424-092131.jpg",
]


def load_project_env() -> None:
    """Load cart_service/.env if python-dotenv is available; fall back to a small parser."""
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.exists():
        return

    try:
        from dotenv import load_dotenv

        load_dotenv(env_path, override=False)
        return
    except Exception:
        pass

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="使用千问视觉模型识别施工过程照片，并输出可见内容与安全合规判断。"
    )
    parser.add_argument(
        "images",
        nargs="*",
        default=DEFAULT_IMAGES,
        help="图片路径。未传入时默认读取两张测试图片。",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("QWEN_API_KEY") or os.getenv("DASHSCOPE_API_KEY"),
        help="千问/DashScope API Key。建议使用环境变量 QWEN_API_KEY 或 DASHSCOPE_API_KEY。",
    )
    parser.add_argument("--model", default=DEFAULT_QWEN_MODEL, help="千问视觉模型名称。")
    parser.add_argument(
        "--base-url",
        default=DEFAULT_QWEN_BASE_URL,
        help="OpenAI 兼容模式 base URL。",
    )
    parser.add_argument("--timeout", type=int, default=120, help="请求超时时间，单位秒。")
    return parser


def main() -> int:
    load_project_env()
    parser = build_parser()
    args = parser.parse_args()

    if not args.api_key:
        print(
            "未找到千问 API Key。请设置环境变量 QWEN_API_KEY 或 DASHSCOPE_API_KEY，"
            "或使用 --api-key 参数。",
            file=sys.stderr,
        )
        return 2

    for image in args.images:
        image_path = Path(image)
        print("=" * 80)
        print(f"图片: {image_path}")
        print(f"模型: {args.model}")

        if not image_path.exists():
            print(f"错误: 图片不存在: {image_path}")
            continue
        if not image_path.is_file():
            print(f"错误: 不是文件: {image_path}")
            continue

        try:
            result = call_qwen_vision(
                api_key=args.api_key,
                image_path=image_path,
                model=args.model,
                base_url=args.base_url,
                timeout=args.timeout,
            )
        except Exception as exc:
            print(f"识别失败: {exc}")
            continue

        print("识别结果:")
        print(result)

    print("=" * 80)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
