import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *
from typing import List, Dict, Any, Optional

class BitableClient:
    """飞书多维表格（Bitable）单文件精简版客户端 - 修改专用"""

    def __init__(self, app_id: str, app_secret: str):
        self.client = lark.Client.builder() \
            .app_id(app_id) \
            .app_secret(app_secret) \
            .log_level(lark.LogLevel.ERROR) \
            .build()

    def update_record(self, app_token: str, table_id: str, record_id: str, fields: Dict[str, Any]) -> bool:
        """更新指定记录（需要知道 record_id）"""
        request = UpdateAppTableRecordRequest.builder() \
            .app_token(app_token) \
            .table_id(table_id) \
            .record_id(record_id) \
            .request_body(AppTableRecord.builder().fields(fields).build()) \
            .build()
        response = self.client.bitable.v1.app_table_record.update(request)
        self._check(response, "更新记录")
        return True

    def batch_update_records(self, app_token: str, table_id: str, updates: List[Dict[str, Any]]) -> list:
        """批量更新记录（每项需包含 record_id 和 fields）"""
        records = [AppTableRecord.builder().record_id(item["record_id"]).fields(item["fields"]).build() for item in updates]
        request = BatchUpdateAppTableRecordRequest.builder() \
            .app_token(app_token) \
            .table_id(table_id) \
            .request_body(BatchUpdateAppTableRecordRequestBody.builder().records(records).build()) \
            .build()
        response = self.client.bitable.v1.app_table_record.batch_update(request)
        self._check(response, "批量更新记录")
        return response.data.records

    def smart_update(self, app_token: str, table_id: str, field_name: str, field_value: Any, new_fields: Dict[str, Any]) -> int:
        """按字段值修改：把所有 字段名=字段值 的记录更新为新内容 (new_fields)"""
        return self.smart_update_by_conditions(app_token, table_id, {field_name: field_value}, new_fields)

    def smart_update_by_conditions(self, app_token: str, table_id: str, conditions: Dict[str, Any], new_fields: Dict[str, Any]) -> int:
        """按多条件修改：查找满足条件的记录并批量更新"""
        parts = []
        for name, value in conditions.items():
            escaped = str(value).replace('"', '\\"')
            parts.append(f'CurrentValue.[{name}]="{escaped}"')
        formula = parts[0] if len(parts) == 1 else f'AND({", ".join(parts)})'

        matched = self.search_records(app_token, table_id, filter_formula=formula)
        if not matched: return 0

        updates = [{"record_id": r.record_id, "fields": new_fields} for r in matched]
        for i in range(0, len(updates), 500):
            self.batch_update_records(app_token, table_id, updates[i:i + 500])
        return len(updates)

    def update_by_index(self, app_token: str, table_id: str, indices: Any, new_fields: Dict[str, Any]) -> int:
        """按序号更新：对应飞书表格左侧序号（1-based）"""
        all_records = self.list_all_records(app_token, table_id)
        if not all_records: return 0

        if isinstance(indices, int): idx_list = [indices]
        elif isinstance(indices, list): idx_list = indices
        else: raise ValueError("indices 必须是整数或列表")

        updates = [{"record_id": all_records[idx - 1].record_id, "fields": new_fields} for idx in idx_list if 1 <= idx <= len(all_records)]
        if not updates: return 0

        for i in range(0, len(updates), 500):
            self.batch_update_records(app_token, table_id, updates[i:i + 500])
        return len(updates)

    def search_records(self, app_token: str, table_id: str, filter_formula: str):
        """筛选查询记录"""
        request = SearchAppTableRecordRequest.builder() \
            .app_token(app_token) \
            .table_id(table_id) \
            .request_body(SearchAppTableRecordRequestBody.builder().filter(filter_formula).build()) \
            .build()
        response = self.client.bitable.v1.app_table_record.search(request)
        self._check(response, "查询记录")
        return response.data.items or []

    def list_all_records(self, app_token: str, table_id: str) -> list:
        """获取全部记录"""
        all_items, page_token = [], None
        while True:
            data = self.list_records(app_token, table_id, page_token=page_token)
            if data.items: all_items.extend(data.items)
            if not data.has_more: break
            page_token = data.page_token
        return all_items

    def list_records(self, app_token: str, table_id: str, page_token: str = None):
        """列出记录（单页）"""
        builder = ListAppTableRecordRequest.builder().app_token(app_token).table_id(table_id).page_size(500)
        if page_token: builder.page_token(page_token)
        response = self.client.bitable.v1.app_table_record.list(builder.build())
        self._check(response, "列出记录")
        return response.data

    @staticmethod
    def _check(response, action: str):
        if not response.success():
            raise Exception(f"[飞书API] {action}失败: Code={response.code}, Msg={response.msg}")

# ============================================================================
# 配置区 - 请替换为你的实际凭据
# ============================================================================
APP_ID = "替换为你的_APP_ID"
APP_SECRET = "替换为你的_APP_SECRET"
APP_TOKEN = "替换为你的_APP_TOKEN"  # 从多维表格 URL 获取
TABLE_ID = "替换为你的_TABLE_ID"    # 从多维表格 URL 获取

# ============================================================================
# 示例用法
# ============================================================================
if __name__ == "__main__":
    client = BitableClient(APP_ID, APP_SECRET)

    print("-" * 30)
    print("🚀 开始演示飞书表格修改操作")
    print("-" * 30)

    # --- 方式 1: 智能修改 (按姓名更新年龄) ---
    print("\n[方式 1] 正在执行智能修改 (把姓名是'张三'的年龄改为30)...")
    # count = client.smart_update(APP_TOKEN, TABLE_ID, "姓名", "张三", {"年龄": 30})
    # print(f"✅ 修改完成，实际更新条数: {count}")

    # --- 方式 2: 按序号修改 (把第 1 行的状态改为'已完成') ---
    print("\n[方式 2] 正在按序号修改 (修改第 1 行)...")
    # count = client.update_by_index(APP_TOKEN, TABLE_ID, 1, {"状态": "已完成"})
    # print(f"✅ 修改完成，实际更新条数: {count}")

    # --- 方式 3: 基础 ID 修改 ---
    print("\n[方式 3] 正在根据 Record ID 修改记录...")
    # client.update_record(APP_TOKEN, TABLE_ID, "recXXXXXXX", {"备注": "手动更新内容"})
    # print("✅ 指定 ID 记录更新成功")

    print("\n提示：请先在脚本中填写 APP_ID 等配置信息，并取消注释对应的修改逻辑。")
