import os
import sys
import json
from pathlib import Path

# 加载 .env
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

import lark_oapi as lark
from lark_oapi.api.im.v1 import ListMessageRequest

def diagnose():
    app_id = os.environ.get("FEISHU_APP_ID")
    app_secret = os.environ.get("FEISHU_APP_SECRET")
    
    client = lark.Client.builder().app_id(app_id).app_secret(app_secret).build()
    
    # 获取第一个监控群的 ID
    chat_id = "oc_b9c97a9f6535957156da900baef40574" # 从您的日志中看到的 ID
    
    print(f"--- 诊断群消息顺序 (Chat ID: {chat_id}) ---")
    
    # 测试 1: 直接获取
    req = ListMessageRequest.builder().container_id_type("chat").container_id(chat_id).page_size(5).build()
    res = client.im.v1.message.list(req)
    
    if res.success() and res.data and res.data.items:
        print("\n[测试 1] 默认返回顺序 (前 5 条):")
        for i, item in enumerate(res.data.items):
            content = item.body.content if item.body else ""
            print(f"位置 {i}: {item.create_time} | {content[:50]}")
            
    # 测试 2: 使用排序参数 (如果支持)
    # 飞书 API 文档中 list 接口默认是倒序(最新在前)，但让我们验证一下
    
diagnose()
