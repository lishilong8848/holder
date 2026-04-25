import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PAYLOAD_CANDIDATES = (
    PROJECT_ROOT / "payload.local.json",
    PROJECT_ROOT / "payload.json",
)


def load_env_port() -> str:
    port = os.environ.get("PORT", "").strip()
    if port:
        return port

    env_path = PROJECT_ROOT / ".env"
    if env_path.is_file():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            clean_line = line.strip()
            if not clean_line or clean_line.startswith("#") or "=" not in clean_line:
                continue
            key, value = clean_line.split("=", 1)
            if key.strip() == "PORT":
                port = value.strip().strip('"').strip("'")
                if port:
                    return port

    raise RuntimeError("未配置 PORT，请在 .env 中设置 PORT")


def default_url() -> str:
    return f"http://127.0.0.1:{load_env_port()}/api/v1/query/batch"


def resolve_default_payload_path() -> Path:
    for candidate in DEFAULT_PAYLOAD_CANDIDATES:
        if candidate.exists():
            return candidate
    return DEFAULT_PAYLOAD_CANDIDATES[0]


def load_payload(payload_file: str | None = None):
    path = Path(payload_file) if payload_file else resolve_default_payload_path()
    if not path.exists():
        raise FileNotFoundError(
            f"请求体文件不存在：{path}。"
            f"请先基于示例文件创建 {DEFAULT_PAYLOAD_CANDIDATES[0].name}。"
        )
    return json.loads(path.read_text(encoding="utf-8"))


def send_request(url, payload, timeout):
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=timeout) as response:
        response_body = response.read().decode("utf-8")
        return response.status, json.loads(response_body)


def print_summary(response_json):
    print(json.dumps(response_json, ensure_ascii=False, indent=2))

    if not isinstance(response_json, dict) or "results" not in response_json:
        return

    print("\n摘要：")
    print(f"  总数   : {response_json.get('total')}")
    print(f"  成功   : {response_json.get('success')}")
    print(f"  失败   : {response_json.get('failed')}")

    for index, item in enumerate(response_json.get("results", []), 1):
        debug_parts = []
        if "query_status" in item:
            debug_parts.append(f"查询状态={item.get('query_status')}")
        if item.get("query_error"):
            debug_parts.append(f"查询错误={item.get('query_error')}")
        if item.get("writeback_error"):
            debug_parts.append(f"回填错误={item.get('writeback_error')}")

        debug_text = ""
        if debug_parts:
            debug_text = " | " + " | ".join(debug_parts)
        identifier = item.get("record_id") or f"{item.get('name')} / {item.get('id_number')}"
        print(f"  [{index}] {identifier} | 是否成功={item.get('success')}{debug_text}")


def main():
    resolved_default_url = default_url()
    parser = argparse.ArgumentParser(description="批量查询接口手工联调脚本")
    parser.add_argument("--url", default=resolved_default_url, help=f"接口地址，默认：{resolved_default_url}")
    parser.add_argument(
        "--payload-file",
        help=f"自定义请求体 JSON，默认会按顺序尝试：{', '.join(str(path) for path in DEFAULT_PAYLOAD_CANDIDATES)}",
    )
    parser.add_argument("--timeout", type=int, default=180, help="请求超时时间，单位秒，默认：180")
    parser.add_argument("--save-response", help="可选，保存响应 JSON 的输出路径")
    args = parser.parse_args()

    try:
        payload = load_payload(args.payload_file)
        print("请求地址：")
        print(args.url)
        print("\n请求体：")
        print(json.dumps(payload, ensure_ascii=False, indent=2))

        status_code, response_json = send_request(url=args.url, payload=payload, timeout=args.timeout)

        print(f"\nHTTP {status_code}\n")
        print_summary(response_json)

        if args.save_response:
            save_path = Path(args.save_response)
            save_path.write_text(json.dumps(response_json, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"\n响应已保存到：{save_path.resolve()}")

    except urllib.error.HTTPError as exc:
        error_text = exc.read().decode("utf-8", errors="replace")
        print(f"HTTP 错误：{exc.code}")
        print(error_text)
        sys.exit(1)
    except urllib.error.URLError as exc:
        print(f"请求失败：{exc}")
        sys.exit(1)
    except Exception as exc:
        print(f"执行失败：{exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
