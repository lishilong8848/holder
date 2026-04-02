import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path


DEFAULT_URL = "http://127.0.0.1:58000/api/v1/query/batch"
DEFAULT_PAYLOAD_CANDIDATES = (
    Path(__file__).resolve().parents[1] / "payload.local.json",
    Path(__file__).resolve().parents[1] / "payload.json",
)


def resolve_default_payload_path() -> Path:
    for candidate in DEFAULT_PAYLOAD_CANDIDATES:
        if candidate.exists():
            return candidate
    return DEFAULT_PAYLOAD_CANDIDATES[0]


def load_payload(payload_file: str | None = None):
    path = Path(payload_file) if payload_file else resolve_default_payload_path()
    if not path.exists():
        raise FileNotFoundError(
            f"payload file does not exist: {path}. "
            f"Create {DEFAULT_PAYLOAD_CANDIDATES[0].name} from the example payload first."
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

    print("\nSummary:")
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
        identifier = item.get("record_id") or f"{item.get('name')} / {item.get('id_number')}"
        print(f"  [{index}] {identifier} | success={item.get('success')}{debug_text}")


def main():
    parser = argparse.ArgumentParser(description="Manual smoke test for the batch query API")
    parser.add_argument("--url", default=DEFAULT_URL, help=f"API URL, default: {DEFAULT_URL}")
    parser.add_argument(
        "--payload-file",
        help=f"Custom payload JSON. Default candidates: {', '.join(str(path) for path in DEFAULT_PAYLOAD_CANDIDATES)}",
    )
    parser.add_argument("--timeout", type=int, default=180, help="Request timeout in seconds, default: 180")
    parser.add_argument("--save-response", help="Optional output JSON file path")
    args = parser.parse_args()

    try:
        payload = load_payload(args.payload_file)
        print("Request URL:")
        print(args.url)
        print("\nRequest Body:")
        print(json.dumps(payload, ensure_ascii=False, indent=2))

        status_code, response_json = send_request(url=args.url, payload=payload, timeout=args.timeout)

        print(f"\nHTTP {status_code}\n")
        print_summary(response_json)

        if args.save_response:
            save_path = Path(args.save_response)
            save_path.write_text(json.dumps(response_json, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"\nResponse saved to: {save_path.resolve()}")

    except urllib.error.HTTPError as exc:
        error_text = exc.read().decode("utf-8", errors="replace")
        print(f"HTTPError: {exc.code}")
        print(error_text)
        sys.exit(1)
    except urllib.error.URLError as exc:
        print(f"Request failed: {exc}")
        sys.exit(1)
    except Exception as exc:
        print(f"Execution failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
