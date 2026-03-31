import json
import time
from io import BytesIO
from typing import Any, Dict, List, Optional

import lark_oapi as lark
from lark_oapi.api.auth.v3 import (
    InternalTenantAccessTokenRequest,
    InternalTenantAccessTokenRequestBody,
)
from lark_oapi.api.bitable.v1 import AppTableRecord, UpdateAppTableRecordRequest
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
        # 调试日志：核对发送给飞书的凭据
        print(f"[DEBUG] refresh_token: AppId={self.app_id}, Secret前4位={self.app_secret[:4]}...")
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
        print(f"[DEBUG] refresh_token response: success={response.success()}, code={response.code}, msg={response.msg}")
        if hasattr(response, 'raw') and response.raw:
            print(f"[DEBUG] refresh_token raw content: {response.raw.content[:200] if response.raw.content else 'None'}")
        if not response.success():
            raise RuntimeError(f"获取飞书 tenant_access_token 失败: {response.code} - {response.msg}")

        payload = json.loads(response.raw.content)
        self.tenant_token = payload["tenant_access_token"]
        expire_in = payload.get("expire", 7200)
        self.token_expire_time = int(time.time()) + expire_in - 300
        print(f"[DEBUG] refresh_token 成功, token前10位={self.tenant_token[:10]}...")
        return self.tenant_token

    def ensure_token(self) -> str:
        print(f"[DEBUG] ensure_token called. has_token={bool(self.tenant_token)}, expired={int(time.time()) >= self.token_expire_time}")
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
