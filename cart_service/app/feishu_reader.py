import json
import logging
import time
import uuid
from io import BytesIO
from typing import Any, Dict, List, Optional

import lark_oapi as lark
from lark_oapi.api.auth.v3 import (
    InternalTenantAccessTokenRequest,
    InternalTenantAccessTokenRequestBody,
)
from lark_oapi.api.bitable.v1 import (
    AppTableRecord,
    CreateAppTableRecordRequest,
    GetAppTableRecordRequest,
    ListAppTableRecordRequest,
    UpdateAppTableRecordRequest,
)
from lark_oapi.api.bitable.v1.model.attachment import Attachment as BitableAttachment
from lark_oapi.api.drive.v1 import UploadAllMediaRequest, UploadAllMediaRequestBody
from lark_oapi.api.im.v1 import CreateMessageRequest, CreateMessageRequestBody
from lark_oapi.api.wiki.v2 import GetNodeSpaceRequest


logger = logging.getLogger(__name__)

BITABLE_MEDIA_PARENT_TYPE = "bitable_image"
WIKI_NODE_OBJ_TYPE = "wiki"
BITABLE_OBJ_TYPE = "bitable"
DATA_NOT_READY_CODE = "1254607"
GET_RECORD_DATA_NOT_READY_RETRY_DELAYS = (1, 2, 3, 5, 8)


def _is_data_not_ready_response(response: Any) -> bool:
    code = str(getattr(response, "code", "") or "")
    msg = str(getattr(response, "msg", "") or "")
    return code == DATA_NOT_READY_CODE or "Data not ready" in msg


class FeishuTableReader:
    """精简版飞书多维表客户端，负责刷新 token、上传图片和更新记录。"""

    def __init__(self, app_id: str, app_secret: str, app_token: str, table_id: str):
        self.app_id = app_id
        self.app_secret = app_secret
        self.raw_app_token = app_token
        self.app_token = app_token
        self.table_id = table_id
        self.tenant_token: Optional[str] = None
        self.token_expire_time = 0
        self._resolved_app_token: Optional[str] = None
        self.client = (
            lark.Client.builder()
            .app_id(self.app_id)
            .app_secret(self.app_secret)
            .enable_set_token(True)
            .log_level(lark.LogLevel.ERROR)
            .build()
        )

    def refresh_token(self) -> str:
        request = (
            InternalTenantAccessTokenRequest.builder()
            .request_body(
                InternalTenantAccessTokenRequestBody.builder()
                .app_id(self.app_id)
                .app_secret(self.app_secret)
                .build()
            )
            .build()
        )
        response = self.client.auth.v3.tenant_access_token.internal(request)
        if not response.success():
            raise RuntimeError(f"获取飞书 tenant_access_token 失败：{response.code} - {response.msg}")

        payload = json.loads(response.raw.content)
        self.tenant_token = payload["tenant_access_token"]
        expire_in = payload.get("expire", 7200)
        self.token_expire_time = int(time.time()) + expire_in - 300
        return self.tenant_token

    def ensure_token(self) -> str:
        if not self.tenant_token or int(time.time()) >= self.token_expire_time:
            self.refresh_token()
        if self._resolved_app_token is None:
            self.resolve_app_token()
        return self.tenant_token

    def _request_option(self):
        self.ensure_token()
        return self._request_option_from_current_token()

    def _request_option_from_current_token(self):
        return lark.RequestOption.builder().tenant_access_token(self.tenant_token).build()

    def resolve_app_token(self) -> str:
        if self._resolved_app_token is not None:
            return self._resolved_app_token

        request = (
            GetNodeSpaceRequest.builder()
            .token(self.raw_app_token)
            .obj_type(WIKI_NODE_OBJ_TYPE)
            .build()
        )
        response = self.client.wiki.v2.space.get_node(request, self._request_option_from_current_token())
        node = getattr(getattr(response, "data", None), "node", None)
        resolved_token = getattr(node, "obj_token", None)
        resolved_obj_type = (getattr(node, "obj_type", None) or "").lower()

        if response.success() and resolved_token:
            if resolved_obj_type and resolved_obj_type != BITABLE_OBJ_TYPE:
                raise RuntimeError(f"wiki token 指向的对象不是多维表：{resolved_obj_type}")
            self._resolved_app_token = resolved_token
            self.app_token = resolved_token
            return resolved_token

        self._resolved_app_token = self.raw_app_token
        self.app_token = self.raw_app_token
        return self._resolved_app_token

    def send_text_message(
        self,
        receive_id: str,
        text: str,
        *,
        receive_id_type: str = "chat_id",
    ) -> bool:
        """发送普通文本消息，默认发送到群 chat_id。"""
        receive_id = str(receive_id or "").strip()
        if not receive_id:
            return False
        if not text:
            return False

        if not self.tenant_token or int(time.time()) >= self.token_expire_time:
            self.refresh_token()

        request = (
            CreateMessageRequest.builder()
            .receive_id_type(receive_id_type)
            .request_body(
                CreateMessageRequestBody.builder()
                .receive_id(receive_id)
                .msg_type("text")
                .content(json.dumps({"text": text}, ensure_ascii=False))
                .uuid(str(uuid.uuid4()))
                .build()
            )
            .build()
        )
        response = self.client.im.v1.message.create(request, self._request_option_from_current_token())
        if not response.success():
            print(f"[飞书] 发送文本消息失败: {response.code} - {response.msg}")
            return False
        return True

    def upload_image(self, filename: str, content: bytes) -> str:
        if not content:
            raise RuntimeError("上传飞书附件失败：图片内容为空")

        request = (
            UploadAllMediaRequest.builder()
            .request_body(
                UploadAllMediaRequestBody.builder()
                .file_name(filename)
                .parent_type(BITABLE_MEDIA_PARENT_TYPE)
                .parent_node(self.resolve_app_token())
                .size(len(content))
                .file((filename, BytesIO(content), "image/jpeg"))
                .build()
            )
            .build()
        )

        response = self.client.drive.v1.media.upload_all(request, self._request_option())
        file_token = getattr(getattr(response, "data", None), "file_token", None)
        if not response.success() or not file_token:
            raise RuntimeError(f"上传飞书附件失败：{response.code} - {response.msg}")
        return file_token

    def list_records(self, *, field_names: Optional[List[str]] = None, page_size: int = 500) -> List[AppTableRecord]:
        try:
            return self._list_records(field_names=field_names, page_size=page_size)
        except RuntimeError:
            if not field_names:
                raise
            logger.warning("按字段过滤拉取飞书记录失败，将自动重试全量拉取")
            return self._list_records(field_names=None, page_size=page_size)

    def _list_records(self, *, field_names: Optional[List[str]], page_size: int) -> List[AppTableRecord]:
        records: List[AppTableRecord] = []
        page_token: Optional[str] = None
        resolved_app_token = self.resolve_app_token()

        while True:
            builder = (
                ListAppTableRecordRequest.builder()
                .app_token(resolved_app_token)
                .table_id(self.table_id)
                .page_size(page_size)
            )
            if field_names:
                builder = builder.field_names(json.dumps(field_names, ensure_ascii=False))
            if page_token:
                builder = builder.page_token(page_token)

            request = builder.build()
            response = self.client.bitable.v1.app_table_record.list(request, self._request_option())
            if not response.success():
                raise RuntimeError(f"拉取飞书记录失败：{response.code} - {response.msg}")

            data = getattr(response, "data", None)
            records.extend(list(getattr(data, "items", None) or []))

            if not getattr(data, "has_more", False):
                break
            page_token = getattr(data, "page_token", None)
            if not page_token:
                break

        return records

    @staticmethod
    def build_attachment_field(file_token: str, filename: str, size: int, mime_type: str) -> List[Any]:
        return [
            BitableAttachment.builder()
            .file_token(file_token)
            .name(filename)
            .size(size)
            .type(mime_type)
            .build()
        ]

    def update_record(self, record_id: str, fields: Dict[str, Any]) -> bool:
        if not record_id:
            return False
        if not fields:
            return True

        request = (
            UpdateAppTableRecordRequest.builder()
            .app_token(self.resolve_app_token())
            .table_id(self.table_id)
            .record_id(record_id)
            .request_body(
                AppTableRecord.builder()
                .fields(fields)
                .build()
            )
            .build()
        )
        response = self.client.bitable.v1.app_table_record.update(request, self._request_option())
        return response.success()

    def get_record(
        self,
        record_id: str,
        table_id: Optional[str] = None,
        retry_delays: Optional[List[float]] = None,
    ) -> Optional[Dict[str, Any]]:
        """获取单条记录，返回 fields 字典"""
        delays = GET_RECORD_DATA_NOT_READY_RETRY_DELAYS if retry_delays is None else tuple(retry_delays)
        max_attempts = len(delays) + 1

        for attempt in range(1, max_attempts + 1):
            request = (
                GetAppTableRecordRequest.builder()
                .app_token(self.resolve_app_token())
                .table_id(table_id or self.table_id)
                .record_id(record_id)
                .build()
            )
            response = self.client.bitable.v1.app_table_record.get(request, self._request_option())
            if response.success():
                record = getattr(getattr(response, "data", None), "record", None)
                if record is None:
                    return None
                return getattr(record, "fields", None)

            if _is_data_not_ready_response(response) and attempt < max_attempts:
                delay = delays[attempt - 1]
                print(
                    (
                        f"[飞书] 记录 {record_id} 暂未就绪，"
                        f"{delay} 秒后重试 ({attempt}/{max_attempts - 1})"
                    )
                )
                time.sleep(delay)
                continue

            print(f"[飞书] 获取记录失败: {response.code} - {response.msg}")
            return None

        return None

    def create_record(self, fields: Dict[str, Any], table_id: Optional[str] = None) -> Optional[str]:
        """在指定表中创建一条记录，返回 record_id"""
        request = (
            CreateAppTableRecordRequest.builder()
            .app_token(self.resolve_app_token())
            .table_id(table_id or self.table_id)
            .request_body(
                AppTableRecord.builder()
                .fields(fields)
                .build()
            )
            .build()
        )
        response = self.client.bitable.v1.app_table_record.create(request, self._request_option())
        if not response.success():
            print(f"[飞书] 创建记录失败: {response.code} - {response.msg}")
            return None
        record = getattr(getattr(response, "data", None), "record", None)
        return getattr(record, "record_id", None) if record else None

    def _get_field_id_by_name(self, table_id: str, field_name: str) -> str:
        if field_name.startswith("fld") and len(field_name) >= 10:
            return field_name
            
        if not hasattr(self, "_field_cache"):
            self._field_cache = {}
            
        if table_id not in self._field_cache:
            import requests
            app_token = self.resolve_app_token()
            token = self.ensure_token()
            url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
            headers = {"Authorization": f"Bearer {token}"}
            try:
                res = requests.get(url, headers=headers, timeout=10)
                if res.status_code == 200:
                    items = res.json().get("data", {}).get("items", [])
                    self._field_cache[table_id] = {item["field_name"]: item["field_id"] for item in items}
                else:
                    print(f"[飞书] 获取字段映射失败: {res.text}")
                    self._field_cache[table_id] = {}
            except Exception as e:
                print(f"[飞书] 获取字段映射异常: {e}")
                self._field_cache[table_id] = {}
                
        return self._field_cache.get(table_id, {}).get(field_name, field_name)

    def download_media(self, file_token: str, table_id: str, record_id: str, field_id_or_name: str, direct_url: str = None) -> Optional[bytes]:
        """
        下载多维表附件内容。
        尝试多种路径以解决 404/权限问题：
        0. Record Direct URL 模式 (最高优先级，自带 extra 鉴权)
        1. Drive 模式 (通用 SDK 方式)
        2. Bitable REST 模式 (备用)
        """
        import requests
        from lark_oapi.api.drive.v1 import DownloadMediaRequest
        
        token = self.ensure_token()
        headers = {"Authorization": f"Bearer {token}"}

        # --- 策略 0: 使用 Bitable 下发自带的高级鉴权 URL ---
        if direct_url:
            try:
                res_direct = requests.get(direct_url, headers=headers, timeout=60)
                if res_direct.status_code == 200:
                    return res_direct.content
                print(f"[飞书] Direct URL 下载失败: HTTP {res_direct.status_code} - {res_direct.text[:100]}")
            except Exception as e:
                print(f"[飞书] Direct URL 下载异常: {e}")

        # --- 策略 1: Drive 下载 (SDK 常用方式) ---
        req_drive = DownloadMediaRequest.builder().file_token(file_token).build()
        res_drive = self.client.drive.v1.media.download(req_drive, self._request_option_from_current_token())
        
        if res_drive.success():
            if hasattr(res_drive, "file") and res_drive.file:
                return res_drive.file.read()
            if hasattr(res_drive, "raw") and res_drive.raw:
                return res_drive.raw.content

        # --- 策略 2: Bitable REST 直接下载 (专门解决某些记录的 404 问题) ---
        app_token = self.resolve_app_token()
        
        # 将传入的字段名转换为飞书真实 field_id
        real_field_id = self._get_field_id_by_name(table_id, field_id_or_name)
        
        url_bitable = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}/fields/{real_field_id}/file/{file_token}/download"
        
        try:
            res_rest = requests.get(url_bitable, headers=headers, timeout=60)
            if res_rest.status_code == 200:
                return res_rest.content
            
            # 如果都失败了，打印详细日志
            print(f"[飞书] 下载附件彻底失败 (token={file_token[:10]}...)")
            print(f"      Bitable响应: HTTP {res_rest.status_code} ({url_bitable})")
            
        except Exception as e:
            print(f"[飞书] 下载过程出现异常: {e}")
            
        return None
