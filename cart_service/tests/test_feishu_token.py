"""快速测试飞书凭据是否有效"""
import json
import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

from app.feishu_reader import FeishuTableReader

# 从 payload.json 读取凭据
payload_path = project_root / "payload.json"
payload = json.loads(payload_path.read_text(encoding="utf-8"))
feishu_cfg = payload["feishu"]

print(f"App ID:     {feishu_cfg['app_id']}")
print(f"App Secret: {feishu_cfg['app_secret'][:6]}...{feishu_cfg['app_secret'][-4:]}")
print(f"App Token:  {feishu_cfg['app_token']}")
print(f"Table ID:   {feishu_cfg['table_id']}")
print()

reader = FeishuTableReader(
    app_id=feishu_cfg["app_id"],
    app_secret=feishu_cfg["app_secret"],
    app_token=feishu_cfg["app_token"],
    table_id=feishu_cfg["table_id"],
)

print("正在请求飞书 Token...")
try:
    token = reader.refresh_token()
    print(f"✅ 成功! tenant_access_token = {token[:10]}...")
except Exception as e:
    print(f"❌ 失败! 错误: {e}")
    import traceback
    traceback.print_exc()
