"""
飞书群消息触发的证书查询与回填业务逻辑

流程：
1. 从消息中提取 记录ID
2. 查询飞书表获取记录，下载附件
3. 解析施工人员信息表 Excel
4. 筛选有操作权限的人员，查询证书
5. 将结果写回飞书表
"""

import json
import os
import re
import logging
import threading
import ast
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import openpyxl

logger = logging.getLogger(__name__)

# 飞书表配置
DEFAULT_SOURCE_TABLE_ID = "tblWHIbp172MNjM1"    # 施工单所在表
DEFAULT_TARGET_TABLE_ID = "tblgJT6SXjjw7iYN"    # 证书查询结果写入表

# 模糊匹配标题关键词
NAME_KEYWORDS = ["姓名", "名字"]
ID_KEYWORDS = ["证件号码", "身份证号"]
PHONE_KEYWORDS = ["手机", "联系方式", "手机号"]
PERMISSION_KEYWORDS = ["特殊作业", "作业权限", "是否有特殊", "特殊作业权限"]
GENDER_KEYWORDS = ["性别"]
JOB_TYPE_KEYWORDS = ["作业类型"]

# 证书类型映射（从证书系统查询结果到中文名）
CERT_TYPE_DISPLAY = {
    "high_voltage": "高压电工作业",
    "low_voltage": "低压电工作业",
    "refrigeration": "制冷与空调设备运行操作作业",
    "working_at_height": "高处安装、维护、拆除作业",
}

RECORD_ID_LABEL_PATTERN = re.compile(
    r"(?:记录\s*ID|record[\s_-]*id)\s*[=＝:：]?\s*(rec[a-zA-Z0-9]{6,40})",
    re.IGNORECASE,
)
RECORD_ID_FALLBACK_PATTERN = re.compile(r"\b(rec[a-zA-Z0-9]{6,40})\b")
DEGRADED_CARD_HINT = "请升级至最新版本客户端，以查看内容"


def get_source_table_id() -> str:
    return os.environ.get("FEISHU_TABLE_ID", DEFAULT_SOURCE_TABLE_ID).strip() or DEFAULT_SOURCE_TABLE_ID


def get_target_table_id() -> str:
    return os.environ.get("FEISHU_TARGET_TABLE_ID", DEFAULT_TARGET_TABLE_ID).strip() or DEFAULT_TARGET_TABLE_ID


def _looks_like_structured_string(value: str) -> bool:
    text = value.strip()
    return bool(text) and text[0] in "[{" and text[-1] in "]}"


def _collect_text_fragments(value: Any, seen: Optional[Set[int]] = None) -> List[str]:
    """
    递归收集消息中所有可能包含记录 ID 的文本片段。
    同时兼容 JSON 字符串、字符串化 dict/list，以及普通嵌套结构。
    """
    if seen is None:
        seen = set()

    fragments: List[str] = []

    if value is None:
        return fragments

    if isinstance(value, (str, bytes, bytearray)):
        text = value.decode("utf-8", errors="ignore") if isinstance(value, (bytes, bytearray)) else value
        if text:
            fragments.append(text)
            structured_text = text.strip()
            if structured_text and _looks_like_structured_string(structured_text):
                for loader in (json.loads, ast.literal_eval):
                    try:
                        parsed = loader(structured_text)
                    except Exception:
                        continue
                    fragments.extend(_collect_text_fragments(parsed, seen))
                    break
        return fragments

    if isinstance(value, (int, float, bool)):
        return [str(value)]

    obj_id = id(value)
    if obj_id in seen:
        return fragments
    seen.add(obj_id)

    if isinstance(value, dict):
        for key, item in value.items():
            fragments.extend(_collect_text_fragments(key, seen))
            fragments.extend(_collect_text_fragments(item, seen))
        return fragments

    if isinstance(value, (list, tuple, set)):
        for item in value:
            fragments.extend(_collect_text_fragments(item, seen))
        return fragments

    return [str(value)]


def _normalize_search_text(parts: List[str]) -> str:
    text = " ".join(part for part in parts if part)
    text = re.sub(r"[\u200b-\u200d\ufeff]", "", text)
    text = text.replace("\\n", "\n").replace("\\r", "\r")
    return text


def _is_degraded_interactive_card(msg_data: Dict[str, Any], full_text: str) -> bool:
    if (msg_data.get("msg_type") or "").strip() != "interactive":
        return False
    return DEGRADED_CARD_HINT in full_text


def _extract_card_title_text(msg_data: Dict[str, Any]) -> str:
    content = msg_data.get("content")
    if isinstance(content, dict):
        title = str(content.get("title") or "").strip()
        if title:
            return title

    content_raw = msg_data.get("content_raw")
    if isinstance(content_raw, str) and content_raw.strip():
        for loader in (json.loads, ast.literal_eval):
            try:
                parsed = loader(content_raw)
            except Exception:
                continue
            if isinstance(parsed, dict):
                title = str(parsed.get("title") or "").strip()
                if title:
                    return title
            break

    return ""


def extract_record_id(msg_data: Dict[str, Any]) -> Optional[str]:
    """
    从消息内容中提取 记录ID。
    策略：直接在所有可能的字符串内容上跑正则，不依赖 JSON 解析结构。
    """
    priority_parts = _collect_text_fragments(
        {
            "content_raw": msg_data.get("content_raw", ""),
            "display_text": msg_data.get("display_text", ""),
            "content": msg_data.get("content", ""),
        }
    )
    full_parts = priority_parts + _collect_text_fragments(msg_data)
    full_text = _normalize_search_text(full_parts)
    title_text = _extract_card_title_text(msg_data)

    if title_text:
        title_match = RECORD_ID_LABEL_PATTERN.search(title_text) or RECORD_ID_FALLBACK_PATTERN.search(title_text)
        if title_match:
            rec_id = title_match.group(1).strip()
            print(f"[消息处理] 从卡片标题中提取到记录ID: {rec_id}", flush=True)
            return rec_id

    m = RECORD_ID_LABEL_PATTERN.search(full_text)
    if m:
        rec_id = m.group(1).strip()
        print(f"[消息处理] 从消息中提取到记录ID: {rec_id}", flush=True)
        return rec_id

    m2 = RECORD_ID_FALLBACK_PATTERN.search(full_text)
    if m2:
        rec_id = m2.group(1).strip()
        print(f"[消息处理] 从消息中匹配到疑似记录ID: {rec_id}", flush=True)
        return rec_id

    if _is_degraded_interactive_card(msg_data, full_text):
        title = ""
        content = msg_data.get("content")
        if isinstance(content, dict):
            title = str(content.get("title") or "").strip()
        title = title or "未知卡片"
        print(
            (
                f"[消息处理] 卡片《{title}》未返回原始业务载荷，当前仅收到降级占位内容。"
                "这类飞书卡片无法从群消息接口恢复 record_id，请改为在多维表格自动化中直接调用 "
                "POST /api/trigger 并传入 record_id。"
            ),
            flush=True,
        )
        return None

    print(f"[消息处理] 未找到记录ID，消息预览: {full_text[:200]}", flush=True)
    return None



def _fuzzy_match_column(header: str, keywords: List[str]) -> bool:
    """模糊匹配列标题"""
    header_clean = header.strip()
    return any(kw in header_clean for kw in keywords)


def _find_column_indices(headers: List[str]) -> Dict[str, Optional[int]]:
    """根据模糊匹配找到各字段对应的列索引"""
    mapping = {
        "name": None,
        "id_number": None,
        "phone": None,
        "permission": None,
        "gender": None,
        "job_type": None,
    }

    for i, header in enumerate(headers):
        if not header:
            continue
        h = str(header).strip()
        if mapping["name"] is None and _fuzzy_match_column(h, NAME_KEYWORDS):
            mapping["name"] = i
        elif mapping["id_number"] is None and _fuzzy_match_column(h, ID_KEYWORDS):
            mapping["id_number"] = i
        elif mapping["phone"] is None and _fuzzy_match_column(h, PHONE_KEYWORDS):
            mapping["phone"] = i
        elif mapping["gender"] is None and _fuzzy_match_column(h, GENDER_KEYWORDS):
            mapping["gender"] = i
        elif mapping["job_type"] is None and _fuzzy_match_column(h, JOB_TYPE_KEYWORDS):
            mapping["job_type"] = i
        elif mapping["permission"] is None and _fuzzy_match_column(h, PERMISSION_KEYWORDS):
            mapping["permission"] = i
        elif mapping["gender"] is None and _fuzzy_match_column(h, GENDER_KEYWORDS):
            mapping["gender"] = i

    return mapping


def parse_excel_for_personnel(file_path: str) -> List[Dict[str, str]]:
    """
    解析施工人员信息表 Excel，返回全部人员列表
    每个人员: {"name": ..., "id_number": ..., "phone": ..., "gender": ..., "has_permission": True/False}
    has_permission 表示是否有特殊作业权限（需要查询证书）
    """
    wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
    ws = wb.active

    rows = list(ws.iter_rows(values_only=True))
    if len(rows) < 2:
        print("[消息处理] Excel 无有效数据行")
        return []

    # 第一行为标题
    headers = [str(cell or "").strip() for cell in rows[0]]
    col_map = _find_column_indices(headers)

    print(f"[消息处理] Excel 标题: {headers}")
    print(f"[消息处理] 列映射: {col_map}")

    if col_map["name"] is None or col_map["id_number"] is None:
        print("[消息处理] 未找到姓名或身份证列，跳过")
        return []

    personnel = []
    for row in rows[1:]:
        # 检查操作权限标记
        has_permission = False
        if col_map["permission"] is not None:
            perm_val = str(row[col_map["permission"]] or "").strip()
            has_permission = (perm_val == "是")

        name = str(row[col_map["name"]] or "").strip()
        id_number = str(row[col_map["id_number"]] or "").strip()
        phone = ""
        if col_map["phone"] is not None:
            phone = str(row[col_map["phone"]] or "").strip()
            
        gender = ""
        if col_map["gender"] is not None:
            gender = str(row[col_map["gender"]] or "").strip()

        job_type_raw = ""
        if col_map["job_type"] is not None:
            job_type_raw = str(row[col_map["job_type"]] or "").strip()

        if name and id_number:
            personnel.append({
                "name": name,
                "id_number": id_number,
                "phone": phone,
                "gender": gender,
                "job_type": job_type_raw,
                "has_permission": has_permission,
            })

    wb.close()
    perm_count = sum(1 for p in personnel if p["has_permission"])
    print(f"[消息处理] 解析到 {len(personnel)} 名人员, 其中 {perm_count} 人有特殊作业权限需查证书")
    return personnel


def process_record_message(record_id: str, feishu_client) -> None:
    """
    完整的消息处理流程（在后台线程中执行）

    Args:
        record_id: 飞书表记录 ID
        feishu_client: FeishuTableReader 实例
    """
    from .service import CertificateService

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    project_root = Path(__file__).resolve().parents[1]
    source_table_id = get_source_table_id()
    target_table_id = get_target_table_id()

    print(f"\n{'#'*60}")
    print(f"[消息处理] 开始处理记录: {record_id}")
    print(f"{'#'*60}")
    print(f"[消息处理] 源表ID: {source_table_id}")
    print(f"[消息处理] 回填表ID: {target_table_id}")

    # === 步骤 1：获取飞书记录 ===
    print(f"[消息处理] 步骤1: 查询飞书记录...")
    fields = feishu_client.get_record(record_id, table_id=source_table_id)
    if not fields:
        print(f"[消息处理] 未找到记录 {record_id}，退出")
        return

    # 获取施工编码（源表公式字段，可能返回 dict / str化的dict / 纯文本）
    shigong_code_raw = fields.get("施工编码", "")
    print(f"[消息处理] 施工编码原始值: {repr(shigong_code_raw)}")
    if isinstance(shigong_code_raw, dict):
        shigong_code = shigong_code_raw.get("text", shigong_code_raw.get("value", ""))
    elif isinstance(shigong_code_raw, list):
        item = shigong_code_raw[0] if shigong_code_raw else ""
        shigong_code = item.get("text", item) if isinstance(item, dict) else str(item)
    elif isinstance(shigong_code_raw, str) and shigong_code_raw.startswith("{"):
        # 飞书有时把公式结果序列化为字符串化的字典，如 "{'text': 'SG02-1005', ...}"
        import ast
        try:
            parsed = ast.literal_eval(shigong_code_raw)
            shigong_code = parsed.get("text", parsed.get("value", shigong_code_raw)) if isinstance(parsed, dict) else str(parsed)
        except Exception:
            shigong_code = shigong_code_raw
    else:
        shigong_code = str(shigong_code_raw) if shigong_code_raw else ""
    print(f"[消息处理] 施工编码解析结果: {shigong_code}")

    print(f"[消息处理] 记录字段名: {list(fields.keys())}")

    # === 步骤 2：下载所有附件和图片 ===
    download_dir = project_root / "output" / "records" / f"{record_id}_{timestamp}"
    download_dir.mkdir(parents=True, exist_ok=True)

    excel_files = []

    for field_name, field_value in fields.items():
        # 附件字段是一个列表，每个元素有 file_token, name, type 等
        if not isinstance(field_value, list):
            continue

        for item in field_value:
            if not isinstance(item, dict):
                continue

            file_token = item.get("file_token")
            file_name = item.get("name", "unknown")
            direct_url = item.get("url") or item.get("tmp_url")
            
            if not file_token:
                continue

            file_bytes = feishu_client.download_media(
                file_token=file_token,
                table_id=source_table_id,
                record_id=record_id,
                field_id_or_name=field_name,
                direct_url=direct_url
            )
            if not file_bytes:
                print(f"[消息处理] 下载失败: {file_name}")
                continue

            # 保存到本地
            local_name = f"{record_id}_{timestamp}_{file_name}"
            local_path = download_dir / local_name
            with open(local_path, "wb") as f:
                f.write(file_bytes)
            print(f"[消息处理] 已保存: {local_path}")

            # 收集 Excel 文件
            if file_name.lower().endswith((".xlsx", ".xls")):
                excel_files.append(str(local_path))

    if not excel_files:
        print(f"[消息处理] 未找到 Excel 附件，退出")
        return

    print(f"[消息处理] 找到 {len(excel_files)} 个 Excel 文件")

    # === 步骤 3：解析 Excel ===
    all_personnel = []
    for excel_path in excel_files:
        print(f"[消息处理] 解析 Excel: {excel_path}")
        try:
            personnel = parse_excel_for_personnel(excel_path)
            all_personnel.extend(personnel)
        except Exception as e:
            print(f"[消息处理] 解析 Excel 失败: {e}")

    if not all_personnel:
        print(f"[消息处理] 未解析到有效人员，退出")
        return

    # 按权限分组
    perm_personnel = [p for p in all_personnel if p["has_permission"]]
    no_perm_personnel = [p for p in all_personnel if not p["has_permission"]]
    print(f"[消息处理] 共 {len(all_personnel)} 人: {len(perm_personnel)} 人需查证书, {len(no_perm_personnel)} 人直接写入")

    created_count = 0

    # === 步骤 4A：无权限人员直接写入多维表（仅基础信息） ===
    if no_perm_personnel:
        print(f"[消息处理] 步骤4A: 写入 {len(no_perm_personnel)} 名普通人员...")
        for person in no_perm_personnel:
            try:
                # 处理作业类型：按 / 分割
                job_types = [jt.strip() for jt in (person.get("job_type") or "").split("/") if jt.strip()]
                
                basic_fields = {
                    "关联施工单": [record_id],
                    "施工编码": shigong_code,
                    "姓名": person["name"],
                    "身份证号": person["id_number"],
                    "手机号": person["phone"],
                    "作业类型": job_types,
                }
                cleaned = {k: v for k, v in basic_fields.items() if v is not None}
                new_record_id = feishu_client.create_record(
                    fields=cleaned,
                    table_id=target_table_id,
                )
                if new_record_id:
                    created_count += 1
                    print(f"[消息处理] ✅ 已写入普通人员: {person['name']}")
                else:
                    print(f"[消息处理] ❌ 写入普通人员失败: {person['name']}")
            except Exception as e:
                print(f"[消息处理] ❌ 普通人员写入异常: {person['name']} - {e}")

    # === 步骤 4B：有权限人员查询证书 ===
    if perm_personnel:
        people_for_query = [
            {
                "record_id": f"msg_{record_id}_{i}",
                "name": p["name"],
                "id_number": p["id_number"],
            }
            for i, p in enumerate(perm_personnel)
        ]

        service = CertificateService(
            feishu_config={"app_id": "", "app_secret": "", "app_token": "", "table_id": ""},
            max_workers=min(3, len(people_for_query)),
            chrome_bin=os.environ.get("CHROME_BIN"),
            chromedriver_path=os.environ.get("CHROMEDRIVER_PATH"),
        )
        service.feishu_client = feishu_client

        print(f"[消息处理] 步骤4B: 开始证书查询 ({len(perm_personnel)} 人)...")
        query_results = service.run_batch(people=people_for_query)

        # === 步骤 5：将证书查询结果写入飞书表 ===
        print(f"[消息处理] 步骤5: 写入证书查询结果到飞书表 {target_table_id}...")

        FIELD_MAPPING = {
            "high_voltage": {
                "attachment_field": "高压证",
                "expire_field": "高压证-到期日期",
                "review_due_field": "高压证-应复审日期",
                "review_actual_field": "高压证-实际复审日期",
            },
            "low_voltage": {
                "attachment_field": "低压证",
                "expire_field": "低压证-到期日期",
                "review_due_field": "低压证-应复审日期",
                "review_actual_field": "低压证-实际复审日期",
            },
            "refrigeration": {
                "attachment_field": "制冷证",
                "expire_field": "制冷证-到期日期",
                "review_due_field": "制冷证-应复审日期",
                "review_actual_field": "制冷证-实际复审日期",
            },
            "working_at_height": {
                "attachment_field": "登高证",
                "expire_field": "登高证-到期日期",
                "review_due_field": "登高证-应复审日期",
                "review_actual_field": "登高证-实际复审日期",
            },
            "welding": {
                "attachment_field": "焊接证",
                "expire_field": "焊接证-到期日期",
                "review_due_field": "焊接证-应复审日期",
                "review_actual_field": "焊接证-实际复审日期",
            },
        }

        for i, result in enumerate(query_results):
            person = perm_personnel[i]

            if result.status != "success" or not result.selected_certificates:
                print(f"[消息处理] {person['name']}: 查询状态={result.status}, 跳过回写")
                continue

            try:
                new_fields = service.build_feishu_fields(result, FIELD_MAPPING, save_dir=download_dir)

                # 处理作业类型：按 / 分割
                job_types = [jt.strip() for jt in (person.get("job_type") or "").split("/") if jt.strip()]

                new_fields.update({
                    "关联施工单": [record_id],
                    "施工编码": shigong_code,
                    "姓名": person["name"],
                    "身份证号": person["id_number"],
                    "手机号": person["phone"],
                    "作业类型": job_types,
                })

                cleaned_fields = {k: v for k, v in new_fields.items() if v is not None}

                new_record_id = feishu_client.create_record(
                    fields=cleaned_fields,
                    table_id=target_table_id,
                )

                if new_record_id:
                    created_count += 1
                    print(f"[消息处理] ✅ 已创建证书记录: {person['name']}")
                else:
                    print(f"[消息处理] ❌ 创建证书记录失败: {person['name']}")

            except Exception as e:
                import traceback
                traceback.print_exc()
                print(f"[消息处理] ❌ 构建记录异常: {person['name']} - {e}")

    # === 完成 ===
    print(f"\n{'#'*60}")
    print(f"[消息处理] 处理完成! 共创建 {created_count} 条记录 (普通人员 {len(no_perm_personnel)} + 证书人员 {len(perm_personnel)})")
    print(f"[消息处理] 附件保存目录: {download_dir}")
    print(f"{'#'*60}\n")

    # 保存处理结果到本地 JSON
    result_summary = {
        "record_id": record_id,
        "timestamp": timestamp,
        "personnel_count": len(all_personnel),
        "created_records": created_count,
        "download_dir": str(download_dir),
    }
    summary_path = download_dir / "summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(result_summary, f, ensure_ascii=False, indent=2)


# 用于记录正在处理中的 ID，防止并发冲突
_processing_ids = set()
_lock = threading.Lock()

def handle_message_async(msg_data: Dict[str, Any], feishu_client) -> None:
    """
    异步处理消息（在新线程中执行，避免阻塞监听器）

    Args:
        msg_data: 飞书消息数据字典
        feishu_client: FeishuTableReader 实例
    """
    record_id = extract_record_id(msg_data)
    if not record_id:
        return

    # 检查是否正在处理中，防止重复触发
    with _lock:
        if record_id in _processing_ids:
            return
        _processing_ids.add(record_id)

    print(f"[消息处理] 检测到记录ID: {record_id} (来源: {msg_data.get('msg_type', 'text')})，启动任务...")

    def _task_wrapper():
        try:
            process_record_message(record_id, feishu_client)
        except Exception as e:
            print(f"[消息处理] 处理记录 {record_id} 时发生未捕获异常: {e}")
        finally:
            with _lock:
                if record_id in _processing_ids:
                    _processing_ids.remove(record_id)

    thread = threading.Thread(
        target=_task_wrapper,
        name=f"msg-handler-{record_id}",
        daemon=True,
    )
    thread.start()
