import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path


DEFAULT_URL = "http://127.0.0.1:58000/api/v1/query/batch"
DEFAULT_PAYLOAD_PATH = Path(__file__).resolve().parents[1] / "payload.json"


def load_payload(payload_file=None):
    if not payload_file:
        payload_file = str(DEFAULT_PAYLOAD_PATH)

    path = Path(payload_file)
    if not path.exists():
        raise FileNotFoundError(f"payload 文件不存在: {path}")

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

    print("\n摘要:")
    print(f"  total   : {response_json.get('total')}")
    print(f"  success : {response_json.get('success')}")
    print(f"  failed  : {response_json.get('failed')}")

    for index, item in enumerate(response_json.get("results", []), 1):
        debug_parts = []
        if "query_status" in item:
            debug_parts.append(f"query_status={item.get('query_status')}")
        if item.get("query_error"):
            debug_parts.append(f"query_error={item.get('query_error')}")
        if item.get("writeback_error"):
            debug_parts.append(f"writeback_error={item.get('writeback_error')}")

        debug_text = ""
        if debug_parts:
            debug_text = " | " + " | ".join(debug_parts)
        print(f"  [{index}] {item.get('record_id')} | success={item.get('success')}{debug_text}")


def main():
    parser = argparse.ArgumentParser(description="手工测试批量查询回填 API")
    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help=f"接口地址，默认: {DEFAULT_URL}",
    )
    parser.add_argument(
        "--payload-file",
        help=f"自定义请求体 JSON 文件路径，不传则默认读取 {DEFAULT_PAYLOAD_PATH}",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=180,
        help="请求超时时间（秒），默认 180",
    )
    parser.add_argument(
        "--save-response",
        help="可选：把响应完整保存到指定 JSON 文件",
    )
    args = parser.parse_args()

    try:
        payload = load_payload(args.payload_file)
        print("请求地址:")
        print(args.url)
        print("\n请求体:")
        print(json.dumps(payload, ensure_ascii=False, indent=2))

        status_code, response_json = send_request(
            url=args.url,
            payload=payload,
            timeout=args.timeout,
        )

        print(f"\nHTTP {status_code}\n")
        print_summary(response_json)

        if args.save_response:
            save_path = Path(args.save_response)
            save_path.write_text(
                json.dumps(response_json, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            print(f"\n响应已保存到: {save_path.resolve()}")

    except urllib.error.HTTPError as exc:
        error_text = exc.read().decode("utf-8", errors="replace")
        print(f"HTTPError: {exc.code}")
        print(error_text)
        sys.exit(1)
    except urllib.error.URLError as exc:
        print(f"请求失败: {exc}")
        sys.exit(1)
    except Exception as exc:
        print(f"执行失败: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
