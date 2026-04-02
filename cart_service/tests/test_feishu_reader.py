import unittest
from unittest.mock import MagicMock, Mock

from app.feishu_reader import BITABLE_MEDIA_PARENT_TYPE, FeishuTableReader


class FeishuReaderUnitTests(unittest.TestCase):
    def _build_reader_without_init(self):
        reader = FeishuTableReader.__new__(FeishuTableReader)
        reader.raw_app_token = "base_xxx"
        reader.app_token = "base_xxx"
        reader.table_id = "tbl_xxx"
        reader._resolved_app_token = None
        reader.tenant_token = "tenant-token"
        reader.client = MagicMock()
        reader._request_option = Mock(return_value="request-option")
        reader._request_option_from_current_token = Mock(return_value="request-option")
        return reader

    def test_upload_image_returns_file_token(self):
        reader = self._build_reader_without_init()

        fake_response = MagicMock()
        fake_response.success.return_value = True
        fake_response.data.file_token = "file_token_123"
        reader.client.drive.v1.media.upload_all.return_value = fake_response
        reader.resolve_app_token = Mock(return_value="base_xxx")

        token = reader.upload_image("test.jpg", b"image-bytes")

        self.assertEqual(token, "file_token_123")
        request = reader.client.drive.v1.media.upload_all.call_args[0][0]
        self.assertEqual(request.request_body.file_name, "test.jpg")
        self.assertEqual(request.request_body.parent_type, BITABLE_MEDIA_PARENT_TYPE)
        self.assertEqual(request.request_body.parent_node, "base_xxx")
        self.assertIsNone(request.request_body.checksum)
        self.assertEqual(request.request_body.file[0], "test.jpg")
        self.assertEqual(request.request_body.file[2], "image/jpeg")

    def test_upload_image_rejects_empty_content(self):
        reader = self._build_reader_without_init()
        with self.assertRaises(RuntimeError):
            reader.upload_image("empty.jpg", b"")

    def test_build_attachment_field_contains_file_token(self):
        attachments = FeishuTableReader.build_attachment_field(
            file_token="file_token_123",
            filename="test.jpg",
            size=12,
            mime_type="image/jpeg",
        )

        self.assertEqual(len(attachments), 1)
        self.assertEqual(attachments[0].file_token, "file_token_123")
        self.assertEqual(attachments[0].name, "test.jpg")

    def test_update_record_short_circuits_empty_fields(self):
        reader = self._build_reader_without_init()
        self.assertTrue(reader.update_record("rec_001", {}))
        reader.client.bitable.v1.app_table_record.update.assert_not_called()

    def test_list_records_uses_field_names_and_collects_all_pages(self):
        reader = self._build_reader_without_init()
        reader.resolve_app_token = Mock(return_value="base_xxx")

        first_response = MagicMock()
        first_response.success.return_value = True
        first_response.data.items = [MagicMock(record_id="rec_1", fields={"Name": "Alice"})]
        first_response.data.has_more = True
        first_response.data.page_token = "next-page"

        second_response = MagicMock()
        second_response.success.return_value = True
        second_response.data.items = [MagicMock(record_id="rec_2", fields={"Name": "Bob"})]
        second_response.data.has_more = False
        second_response.data.page_token = None

        reader.client.bitable.v1.app_table_record.list.side_effect = [first_response, second_response]

        records = reader.list_records(field_names=["Name", "ID Number"])

        self.assertEqual(len(records), 2)
        first_request = reader.client.bitable.v1.app_table_record.list.call_args_list[0][0][0]
        second_request = reader.client.bitable.v1.app_table_record.list.call_args_list[1][0][0]
        self.assertEqual(first_request.field_names, "[\"Name\", \"ID Number\"]")
        self.assertEqual(second_request.page_token, "next-page")

    def test_list_records_retries_without_field_names_when_filtered_request_fails(self):
        reader = self._build_reader_without_init()
        reader.resolve_app_token = Mock(return_value="base_xxx")

        failed_response = MagicMock()
        failed_response.success.return_value = False
        failed_response.code = 500
        failed_response.msg = "bad field_names"

        success_response = MagicMock()
        success_response.success.return_value = True
        success_response.data.items = []
        success_response.data.has_more = False
        success_response.data.page_token = None

        reader.client.bitable.v1.app_table_record.list.side_effect = [failed_response, success_response]

        records = reader.list_records(field_names=["Name"])

        self.assertEqual(records, [])
        first_request = reader.client.bitable.v1.app_table_record.list.call_args_list[0][0][0]
        second_request = reader.client.bitable.v1.app_table_record.list.call_args_list[1][0][0]
        self.assertEqual(first_request.field_names, "[\"Name\"]")
        self.assertIsNone(second_request.field_names)

    def test_resolve_app_token_converts_wiki_token_to_base_token(self):
        reader = self._build_reader_without_init()
        reader.raw_app_token = "wiki_xxx"
        reader.app_token = "wiki_xxx"

        fake_response = MagicMock()
        fake_response.success.return_value = True
        fake_response.data.node.obj_token = "base_resolved"
        fake_response.data.node.obj_type = "bitable"
        reader.client.wiki.v2.space.get_node.return_value = fake_response

        resolved = reader.resolve_app_token()

        self.assertEqual(resolved, "base_resolved")
        self.assertEqual(reader.app_token, "base_resolved")

    def test_resolve_app_token_keeps_base_token_when_wiki_lookup_fails(self):
        reader = self._build_reader_without_init()

        fake_response = MagicMock()
        fake_response.success.return_value = False
        fake_response.code = 999
        fake_response.msg = "not wiki"
        reader.client.wiki.v2.space.get_node.return_value = fake_response

        resolved = reader.resolve_app_token()

        self.assertEqual(resolved, "base_xxx")
        self.assertEqual(reader.app_token, "base_xxx")


if __name__ == "__main__":
    unittest.main()
