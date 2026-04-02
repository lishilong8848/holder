import json
import logging
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
    ListAppTableRecordRequest,
    UpdateAppTableRecordRequest,
)
from lark_oapi.api.bitable.v1.model.attachment import Attachment as BitableAttachment
from lark_oapi.api.drive.v1 import UploadAllMediaRequest, UploadAllMediaRequestBody
from lark_oapi.api.wiki.v2 import GetNodeSpaceRequest


logger = logging.getLogger(__name__)

BITABLE_MEDIA_PARENT_TYPE = "bitable_image"
WIKI_NODE_OBJ_TYPE = "wiki"
BITABLE_OBJ_TYPE = "bitable"


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
        logger.info("飞书 tenant_access_token 刷新成功")
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
