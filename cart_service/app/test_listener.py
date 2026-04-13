"""最简测试脚本：验证飞书长连接消息监听是否正常"""
import os
import sys
import json
from pathlib import Path

# 加载 .env
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

import lark_oapi as lark
from lark_oapi.api.im.v1 import P2ImMessageReceiveV1


def on_message(data: P2ImMessageReceiveV1) -> None:
    print(f"\n{'='*60}")
    print(">>> 收到消息事件!")
    try:
        event = data.event
        msg = event.message
        print(f"  chat_id: {msg.chat_id}")
        print(f"  chat_type: {msg.chat_type}")
        print(f"  msg_type: {msg.message_type}")
        print(f"  content: {msg.content}")
        if event.sender and event.sender.sender_id:
            print(f"  sender: {event.sender.sender_id.open_id}")
    except Exception as e:
        print(f"  解析异常: {e}")
    print(f"{'='*60}\n")


app_id = os.environ.get("FEISHU_APP_ID", "")
app_secret = os.environ.get("FEISHU_APP_SECRET", "")

print(f"App ID: {app_id[:10]}...")
print(f"App Secret: {app_secret[:4]}...")

event_handler = (
    lark.EventDispatcherHandler.builder("", "")
    .register_p2_im_message_receive_v1(on_message)
    .build()
)

client = lark.ws.Client(
    app_id=app_id,
    app_secret=app_secret,
    event_handler=event_handler,
    log_level=lark.LogLevel.DEBUG,
)

print("启动 WebSocket 监听... (在飞书群中发消息测试)")
client.start()
