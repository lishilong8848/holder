"""
飞书群消息监听模块

支持两种模式：
1. WebSocket 长连接：实时接收用户消息（受飞书限制，无法接收其他机器人消息）。
2. 定时轮询：通过 API 获取群消息记录，可以获取机器人消息。
"""

import concurrent.futures
import json
import logging
import os
import re
import threading
import time
from typing import Callable, Dict, List, Optional, Set

import lark_oapi as lark
from lark_oapi.api.im.v1 import (
    GetChatRequest,
    GetMessageRequest,
    ListChatRequest,
    ListMessageRequest,
    P2ImMessageReceiveV1,
)

logger = logging.getLogger(__name__)

# 模块级变量，保持引用不被回收
_ws_client = None
_chat_name_cache: Dict[str, str] = {}
_rest_client = None
_watched_groups: List[str] = []
_user_callback: Optional[Callable] = None

# 轮询相关变量
_processed_msg_ids: Set[str] = set()
_max_cached_ids = 200
_poll_interval = 5  # 轮询间隔 (秒)
_card_log_limit = int(os.environ.get("FEISHU_CARD_LOG_LIMIT", "2000"))
_message_sort_desc = "ByCreateTimeDesc"
_preload_page_size = 50
_latest_page_size = 1


def _get_chat_name(chat_id: str) -> Optional[str]:
    """查询群名，结果缓存"""
    global _rest_client, _chat_name_cache
    if chat_id in _chat_name_cache:
        return _chat_name_cache[chat_id]

    if not _rest_client:
        return None

    try:
        request = GetChatRequest.builder().chat_id(chat_id).build()
        response = _rest_client.im.v1.chat.get(request)
        if response.success() and response.data:
            name = response.data.name
            if name:
                _chat_name_cache[chat_id] = name
                return name
        else:
            print(f"[飞书监听] 查询群信息失败: chat_id={chat_id}, code={response.code}")
    except Exception as e:
        print(f"[飞书监听] 查询群名异常: {e}")

    return None


def _handle_common_logic(msg_data: Dict) -> None:
    """内部通用的消息处理分发逻辑"""
    global _user_callback, _processed_msg_ids

    msg_id = msg_data.get("message_id")
    if msg_id in _processed_msg_ids:
        return
    
    # 记录已处理
    _processed_msg_ids.add(msg_id)
    if len(_processed_msg_ids) > _max_cached_ids:
        # 清理旧的记录，保留最近的
        _processed_msg_ids.clear()
        _processed_msg_ids.add(msg_id)

    # 打印到控制台
    print(f"\n{'='*60}")
    print(f"📩 [飞书群消息] ({msg_data.get('sender_type', 'unknown')})")
    print(f"   群名: {msg_data['chat_name']}")
    print(f"   类型: {msg_data['msg_type']}")
    print(f"   发送者: {msg_data['sender_id']}")
    print(f"   内容: {msg_data['display_text'][:500]}")
    if msg_data.get("msg_type") == "interactive" and msg_data.get("content_raw"):
        raw_content = str(msg_data.get("content_raw") or "")
        print(f"   卡片载荷: {raw_content[:_card_log_limit]}")
    print(f"{'='*60}\n")

    # 调用外部回调
    if _user_callback:
        try:
            _user_callback(msg_data)
        except Exception as e:
            print(f"[飞书监听] 回调处理异常: {e}")


def _on_message(data: P2ImMessageReceiveV1) -> None:
    """WebSocket 收到消息的回调"""
    global _watched_groups

    try:
        event = data.event
        if not event or not event.message:
            return

        message = event.message
        chat_id = message.chat_id or ""
        chat_type = message.chat_type or ""
        msg_type = message.message_type or ""
        
        sender_type = ""
        sender_id = ""
        if event.sender:
            sender_type = event.sender.sender_type or ""
            if event.sender.sender_id:
                sender_id = event.sender.sender_id.open_id or ""

        if chat_type != "group":
            return

        chat_name = _get_chat_name(chat_id) or chat_id
        if _watched_groups and chat_name not in _watched_groups:
            return

        content_raw = message.content or "{}"
        try:
            content = json.loads(content_raw)
        except json.JSONDecodeError:
            content = {"raw": content_raw}

        display_text = content.get("text", json.dumps(content, ensure_ascii=False))

        msg_data = {
            "message_id": message.message_id or "",
            "chat_id": chat_id,
            "chat_name": chat_name,
            "chat_type": chat_type,
            "msg_type": msg_type,
            "sender_type": sender_type,
            "sender_id": sender_id,
            "content": content,
            "display_text": display_text,
            "content_raw": content_raw,
            "create_time": message.create_time or "",
        }

        _handle_common_logic(msg_data)

    except Exception as e:
        print(f"[飞书监听] WebSocket 处理异常: {e}")


def _find_monitored_chat_ids() -> List[str]:
    """通过 API 获取机器人加入的群，并匹配监控列表中的 chat_id"""
    global _rest_client, _watched_groups, _chat_name_cache
    if not _rest_client:
        return []

    matched_ids = []
    try:
        req = ListChatRequest.builder().page_size(100).build()
        res = _rest_client.im.v1.chat.list(req)
        if res.success() and res.data and res.data.items:
            for chat in res.data.items:
                name = chat.name or ""
                cid = chat.chat_id or ""
                _chat_name_cache[cid] = name
                
                # 如果没设置监控群，则监控搜寻到的所有群
                if not _watched_groups or name in _watched_groups:
                    matched_ids.append(cid)
    except Exception as e:
        print(f"[飞书监听] 获取群列表异常: {e}")
    
    return matched_ids


def _build_message_list_request(
    chat_id: str,
    *,
    page_size: int,
    sort_type: str = _message_sort_desc,
    page_token: Optional[str] = None,
) -> ListMessageRequest:
    builder = (
        ListMessageRequest.builder()
        .container_id_type("chat")
        .container_id(chat_id)
        .page_size(page_size)
    )
    if sort_type:
        builder = builder.sort_type(sort_type)
    if page_token:
        builder = builder.page_token(page_token)
    return builder.build()


def _poll_loop():
    """消息轮询线程主循环"""
    print("[飞书监听] 消息轮询线程正在初始化...")
    
    # 第一次运行，先扫描一次消息 ID，避免把历史消息当新消息
    monitored_ids = _find_monitored_chat_ids()
    print(f"[飞书监听] 初始找到可监控的群 ID: {monitored_ids}")
    
    for cid in monitored_ids:
        try:
            req = _build_message_list_request(cid, page_size=_preload_page_size)
            res = _rest_client.im.v1.message.list(req)
            if res.success() and res.data and res.data.items:
                print(f"[飞书监听] 群 [{cid}] 预加载了 {len(res.data.items)} 条历史消息 ID")
                for item in res.data.items:
                    _processed_msg_ids.add(item.message_id)
        except Exception as e:
            print(f"[飞书监听] 预加载异常: {e}")

    print("[飞书监听] 轮询线程进入循环状态")
    while True:
        try:
            time.sleep(_poll_interval)
            
            # 定时更新监控的 ID
            monitored_ids = _find_monitored_chat_ids()
            if not monitored_ids:
                print("[飞书轮询] 未发现符合条件的群，请检查机器人是否在群内或群名配置")
                continue
            
            for cid in monitored_ids:
                chat_name = _chat_name_cache.get(cid, cid)
                
                req = _build_message_list_request(cid, page_size=_latest_page_size)
                res = _rest_client.im.v1.message.list(req)
                
                if not res.success():
                    print(f"[飞书轮询] {chat_name} 请求失败: {res.code}")
                    continue

                if res.data and res.data.items:
                    item = res.data.items[0]
                    
                    is_new = item.message_id not in _processed_msg_ids
                    sender_type = item.sender.sender_type if item.sender else "sys"
                    content_raw = item.body.content if (item.body and item.body.content) else "{}"
                    
                    # 判断是否过期（超过 5 分钟）
                    is_expired = False
                    try:
                        msg_time_ms = int(item.create_time)
                        now_ms = int(time.time() * 1000)
                        if now_ms - msg_time_ms > 300000:  # 5分钟
                            is_expired = True
                    except Exception:
                        pass

                    # 输出日志（所有消息都打）
                    print(f"\n{'='*50}", flush=True)
                    print(f"🕒 [飞书轮询] {chat_name} 最新动态", flush=True)
                    status_tag = "🚀 [NEW]" if is_new else "🔹 [WATCHING]"
                    if is_expired: status_tag += " (已过期)"
                    print(f"状态: {status_tag} | ID: {item.message_id}", flush=True)
                    print(f"类型: {item.msg_type} ({sender_type})", flush=True)
                    print(f"原始内容预览: {content_raw[:150]}", flush=True)
                    print(f"{'='*50}\n", flush=True)

                    if not is_new or is_expired:
                        if is_expired and is_new:
                            _processed_msg_ids.add(item.message_id)
                            print(f"[飞书轮询] 跳过过期消息", flush=True)
                        continue

                    # 标记已处理
                    _processed_msg_ids.add(item.message_id)

                    # 对 interactive 卡片用 message.get 拉取完整内容（加超时保护防挂起）
                    if item.msg_type == "interactive":
                        def _fetch_detail(msg_id):
                            try:
                                req = GetMessageRequest.builder().message_id(msg_id).build()
                                resp = _rest_client.im.v1.message.get(req)
                                if resp.success() and resp.data and resp.data.items:
                                    return resp.data.items[0].body.content
                            except Exception as ex:
                                print(f"[飞书轮询] 详情接口异常: {ex}", flush=True)
                            return None

                        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                            future = executor.submit(_fetch_detail, item.message_id)
                            try:
                                detail_body = future.result(timeout=5)
                                if detail_body:
                                    content_raw = detail_body
                                    print(f"[飞书轮询] 卡片详情获取成功", flush=True)
                                else:
                                    print(f"[飞书轮询] 详情接口无内容，使用原始内容", flush=True)
                            except concurrent.futures.TimeoutError:
                                print(f"[飞书轮询] 详情接口超时(5s)，使用原始内容继续", flush=True)

                    # 尝试解析 JSON
                    content_json = {}
                    try:
                        content_json = json.loads(content_raw)
                    except Exception:
                        content_json = {"text": content_raw}

                    # 提取显示文本
                    display_text = content_raw  # 默认直接用原文本，确保不丢任何信息
                    if item.msg_type != "interactive":
                        display_text = content_json.get("text", content_raw)

                    sender_id = ""
                    if item.sender and item.sender.id:
                        sender_id = item.sender.id

                    msg_data = {
                        "message_id": item.message_id,
                        "chat_id": cid,
                        "chat_name": chat_name,
                        "chat_type": "group",
                        "msg_type": item.msg_type,
                        "sender_type": sender_type,
                        "sender_id": sender_id,
                        "content": content_json,
                        "display_text": display_text,
                        "content_raw": content_raw,  # 传递原始内容，下游可直接使用 re 搜寽
                        "create_time": item.create_time,
                    }

                    if item.msg_type == "interactive":
                        print(f"[飞书轮询] 卡片完整载荷预览: {content_raw[:_card_log_limit]}", flush=True)
                    
                    if _user_callback:
                        try:
                            _user_callback(msg_data)
                        except Exception as e:
                            print(f"[飞书轮询] 回调分发异常: {e}", flush=True)
        except Exception as e:
            print(f"[飞书监听] 轮询周期异常: {e}", flush=True)


def start_listener(
    app_id: str,
    app_secret: str,
    watched_group_names: Optional[List[str]] = None,
    on_message: Optional[Callable[[Dict], None]] = None,
) -> None:
    """在后台线程中启动飞书消息监听"""
    global _ws_client, _rest_client, _watched_groups, _user_callback

    _watched_groups = watched_group_names or []
    _user_callback = on_message

    # REST 客户端
    _rest_client = (
        lark.Client.builder()
        .app_id(app_id)
        .app_secret(app_secret)
        .log_level(lark.LogLevel.ERROR)
        .build()
    )

    # 1. 启动 WebSocket 监听线程 (用于低延迟处理普通消息)
    def _run_ws():
        global _ws_client
        event_handler = (
            lark.EventDispatcherHandler.builder("", "")
            .register_p2_im_message_receive_v1(_on_message)
            .build()
        )
        _ws_client = lark.ws.Client(
            app_id=app_id,
            app_secret=app_secret,
            event_handler=event_handler,
            log_level=lark.LogLevel.INFO,
        )
        print(f"[飞书监听] WebSocket 启动中...")
        _ws_client.start()

    ws_thread = threading.Thread(target=_run_ws, name="feishu-ws", daemon=True)
    ws_thread.start()

    # 2. 启动轮询线程 (用于获取机器人消息)
    poll_thread = threading.Thread(target=_poll_loop, name="feishu-poll", daemon=True)
    poll_thread.start()
    
    print("[飞书监听] WebSocket 与 轮询 线程均已启动")


def create_listener_from_env(
    on_message: Optional[Callable[[Dict], None]] = None,
) -> bool:
    """从环境变量读取配置并启动监听"""
    app_id = os.environ.get("FEISHU_APP_ID", "").strip()
    app_secret = os.environ.get("FEISHU_APP_SECRET", "").strip()
    watched_groups_str = os.environ.get("FEISHU_WATCH_GROUPS", "").strip()

    if not app_id or not app_secret:
        logger.warning("未配置 FEISHU_APP_ID / FEISHU_APP_SECRET，跳过飞书消息监听")
        return False

    watched_groups = [g.strip() for g in watched_groups_str.split(",") if g.strip()] if watched_groups_str else []

    start_listener(
        app_id=app_id,
        app_secret=app_secret,
        watched_group_names=watched_groups,
        on_message=on_message,
    )
    return True
