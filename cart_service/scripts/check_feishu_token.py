import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PAYLOAD_PATHS = (
    PROJECT_ROOT / "payload.local.json",
    PROJECT_ROOT / "payload.json",
)

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.feishu_reader import FeishuTableReader


def resolve_payload_path() -> Path:
    for candidate in DEFAULT_PAYLOAD_PATHS:
        if candidate.exists():
            return candidate
    return DEFAULT_PAYLOAD_PATHS[0]


def main():
    payload_path = resolve_payload_path()
    if not payload_path.exists():
        raise FileNotFoundError(
            f"payload file does not exist: {payload_path}. "
            f"Create {DEFAULT_PAYLOAD_PATHS[0].name} from the example payload first."
        )

    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    feishu_cfg = payload["feishu"]

    reader = FeishuTableReader(
        app_id=feishu_cfg["app_id"],
        app_secret=feishu_cfg["app_secret"],
        app_token=feishu_cfg["app_token"],
        table_id=feishu_cfg["table_id"],
    )

    print("Requesting Feishu token...")
    reader.refresh_token()
    print("Success: tenant_access_token was obtained.")


if __name__ == "__main__":
    main()
