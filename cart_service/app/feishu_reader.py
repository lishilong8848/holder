import json
import time
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
    UpdateAppTableRecordRequest,
)
from lark_oapi.api.bitable.v1.model.attachment import Attachment as BitableAttachment
from lark_oapi.api.drive.v1 import UploadAllMediaRequest, UploadAllMediaRequestBody
from lark_oapi.api.wiki.v2 import GetNodeSpaceRequest


BITABLE_MEDIA_PARENT_TYPE = "bitable_image"
WIKI_NODE_OBJ_TYPE = "wiki"
BITABLE_OBJ_TYPE = "bitable"


class FeishuTableReader:
    """Minimal Feishu bitable client for token refresh, media upload, and record update."""

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
            raise RuntimeError(f"获取飞书 tenant_access_token 失败: {response.code} - {response.msg}")

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
                raise RuntimeError(f"wiki token 指向的对象不是多维表: {resolved_obj_type}")
            self._resolved_app_token = resolved_token
            self.app_token = resolved_token
            return resolved_token

        self._resolved_app_token = self.raw_app_token
        self.app_token = self.raw_app_token
        return self._resolved_app_token

    def upload_image(self, filename: str, content: bytes) -> str:
        if not content:
            raise RuntimeError("上传飞书附件失败: 图片内容为空")

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
            raise RuntimeError(f"上传飞书附件失败: {response.code} - {response.msg}")
        return file_token

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

    def get_record(self, record_id: str, table_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """获取单条记录，返回 fields 字典"""
        request = (
            GetAppTableRecordRequest.builder()
            .app_token(self.resolve_app_token())
            .table_id(table_id or self.table_id)
            .record_id(record_id)
            .build()
        )
        response = self.client.bitable.v1.app_table_record.get(request, self._request_option())
        if not response.success():
            print(f"[飞书] 获取记录失败: {response.code} - {response.msg}")
            return None
        record = getattr(getattr(response, "data", None), "record", None)
        if record is None:
            return None
        return getattr(record, "fields", None)

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
