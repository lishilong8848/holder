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
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import openpyxl

logger = logging.getLogger(__name__)

# 飞书表配置
SOURCE_TABLE_ID = "tblWHIbp172MNjM1"    # 施工单所在表
TARGET_TABLE_ID = "tblgJT6SXjjw7iYN"    # 证书查询结果写入表

# 模糊匹配标题关键词
NAME_KEYWORDS = ["姓名", "名字"]
ID_KEYWORDS = ["证件号码", "身份证号"]
PHONE_KEYWORDS = ["手机", "联系方式", "手机号"]
PERMISSION_KEYWORDS = ["特殊作业", "作业权限", "是否有特殊", "特殊作业权限"]
GENDER_KEYWORDS = ["性别"]

# 证书类型映射（从证书系统查询结果到中文名）
CERT_TYPE_DISPLAY = {
    "high_voltage": "高压电工作业",
    "low_voltage": "低压电工作业",
    "refrigeration": "制冷与空调设备运行操作作业",
    "working_at_height": "高处安装、维护、拆除作业",
}


def extract_record_id(msg_data: Dict[str, Any]) -> Optional[str]:
    """
    从消息内容中提取 记录ID。
    支持：
    1. 文本中的 "记录ID=recXXX"
    2. 文本中直接出现的 "recXXX"
    3. 卡片消息 (interactive) 的 title 或 content
    """
    content_json = msg_data.get("content", {})
    msg_type = msg_data.get("msg_type", "")
    
    # 待搜索的所有文本源
    text_sources = []
    
    if msg_type == "interactive":
        # 飞书消息卡片
        title = content_json.get("title", "")
        text_sources.append(str(title))
        # 尝试从 elements 中提取 text 
        elements = content_json.get("elements", [])
        for row in elements:
            if isinstance(row, list):
                for item in row:
                    if isinstance(item, dict) and item.get("tag") == "text":
                        text_sources.append(str(item.get("text", "")))
    else:
        # 普通文本
        text_sources.append(msg_data.get("display_text", ""))
        text_sources.append(content_json.get("text", ""))

    combined_text = " ".join(text_sources)
    
    # 优先匹配 "记录ID=recXXXX"
    match_explicit = re.search(r"记录ID[=＝]\s*(rec[a-zA-Z0-9]+)", combined_text)
    if match_explicit:
        return match_explicit.group(1).strip()
    
    # 其次匹配任意出现的 "recXXXX" (通常是 14 位左右)
    match_direct = re.search(r"\b(rec[a-zA-Z0-9]{10,25})\b", combined_text)
    if match_direct:
        return match_direct.group(1).strip()
        
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
        elif mapping["permission"] is None and _fuzzy_match_column(h, PERMISSION_KEYWORDS):
            mapping["permission"] = i
        elif mapping["gender"] is None and _fuzzy_match_column(h, GENDER_KEYWORDS):
            mapping["gender"] = i

    return mapping


def parse_excel_for_personnel(file_path: str) -> List[Dict[str, str]]:
    """
    解析施工人员信息表 Excel，返回有操作权限的人员列表
    每个人员: {"name": ..., "id_number": ..., "phone": ...}
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
        # 检查操作权限（如果有该列）
        if col_map["permission"] is not None:
            perm_val = str(row[col_map["permission"]] or "").strip()
            if perm_val != "是":
                continue  # 不是“是”（即没有特殊作业权限），跳过

        name = str(row[col_map["name"]] or "").strip()
        id_number = str(row[col_map["id_number"]] or "").strip()
        phone = ""
        if col_map["phone"] is not None:
            phone = str(row[col_map["phone"]] or "").strip()
            
        gender = ""
        if col_map["gender"] is not None:
            gender = str(row[col_map["gender"]] or "").strip()

        if name and id_number:
            personnel.append({
                "name": name,
                "id_number": id_number,
                "phone": phone,
                "gender": gender,
            })

    wb.close()
    print(f"[消息处理] 解析到 {len(personnel)} 名有操作权限的人员")
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

    print(f"\n{'#'*60}")
    print(f"[消息处理] 开始处理记录: {record_id}")
    print(f"{'#'*60}")

    # === 步骤 1：获取飞书记录 ===
    print(f"[消息处理] 步骤1: 查询飞书记录...")
    fields = feishu_client.get_record(record_id, table_id=SOURCE_TABLE_ID)
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
                table_id=SOURCE_TABLE_ID,
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

    print(f"[消息处理] 共 {len(all_personnel)} 人需要查询证书")

    # === 步骤 4：查询证书 ===
    people_for_query = [
        {
            "record_id": f"msg_{record_id}_{i}",
            "name": p["name"],
            "id_number": p["id_number"],
        }
        for i, p in enumerate(all_personnel)
    ]

    service = CertificateService(
        feishu_config={"app_id": "", "app_secret": "", "app_token": "", "table_id": ""},  # 填充占位符，下方立刻用外部完整的 client 接管
        max_workers=min(3, len(people_for_query)),
        chrome_bin=os.environ.get("CHROME_BIN"),
        chromedriver_path=os.environ.get("CHROMEDRIVER_PATH"),
    )
    service.feishu_client = feishu_client  # 绑定以允许上传图片附件

    print(f"[消息处理] 步骤4: 开始证书查询...")
    query_results = service.run_batch(people=people_for_query)

    # === 步骤 5：将结果写入飞书表 ===
    print(f"[消息处理] 步骤5: 写入飞书表 {TARGET_TABLE_ID}...")

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
    }

    created_count = 0
    for i, result in enumerate(query_results):
        person = all_personnel[i]

        if result.status != "success" or not result.selected_certificates:
            print(f"[消息处理] {person['name']}: 查询状态={result.status}, 跳过回写")
            continue

        try:
            # 自动上传附件及拼装全部映射的证书日期
            new_fields = service.build_feishu_fields(result, FIELD_MAPPING)
            
            # 加入外键与该人员的基础属性
            new_fields.update({
                "关联施工单": [record_id],
                "施工编码": shigong_code,
                "姓名": person["name"],
                "身份证号": person["id_number"],
                "手机号": person["phone"],
            })

            # 清理 new_fields 中所有的 None 值和空列表
            cleaned_fields = {k: v for k, v in new_fields.items() if v is not None}

            # 调试：打印每个字段的类型和值
            print(f"[消息处理] 即将提交的字段 ({person['name']}):")
            for k, v in cleaned_fields.items():
                print(f"  {k}: type={type(v).__name__}, value={repr(v)[:100]}")

            new_record_id = feishu_client.create_record(
                fields=cleaned_fields,
                table_id=TARGET_TABLE_ID,
            )

            if new_record_id:
                created_count += 1
                print(f"[消息处理] ✅ 已创建/更新多证合一记录: {person['name']}")
            else:
                print(f"[消息处理] ❌ 创建/更新多证合一记录失败: {person['name']}")

        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"[消息处理] ❌ 构建记录异常: {person['name']} - {e}")

    # === 完成 ===
    print(f"\n{'#'*60}")
    print(f"[消息处理] 处理完成! 共创建 {created_count} 条记录")
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
