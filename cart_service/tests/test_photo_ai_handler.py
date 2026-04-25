import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.photo_ai_handler import (
    DEFAULT_PHOTO_AI_TABLE_ID,
    FINAL_FEEDBACK_FIELD,
    FINAL_PHOTO_FIELD,
    PROCESS_FEEDBACK_FIELD,
    PROCESS_PHOTO_FIELD,
    process_photo_ai_record,
)


class FakeFeishuClient:
    def __init__(self, fields):
        self.fields = fields
        self.download_calls = []
        self.update_calls = []

    def get_record(self, record_id, table_id=None):
        self.get_record_call = (record_id, table_id)
        return self.fields

    def download_media(self, **kwargs):
        self.download_calls.append(kwargs)
        return b"fake-image-bytes"

    def update_record(self, record_id, fields, table_id=None):
        self.update_calls.append((record_id, fields, table_id))
        return True


class PhotoAiHandlerTests(unittest.TestCase):
    def _run_with_temp_root(self, client):
        with tempfile.TemporaryDirectory() as temp_dir, patch(
            "app.photo_ai_handler.get_qwen_api_key", return_value="qwen-key"
        ), patch(
            "app.photo_ai_handler.call_qwen_vision", return_value="单图识别结果"
        ) as call_qwen_vision, patch(
            "app.photo_ai_handler.summarize_recognition_results", return_value="融合后的反馈"
        ) as summarize:
            process_photo_ai_record("rec_photo_001", client, project_root=Path(temp_dir))
        return call_qwen_vision, summarize

    def test_empty_process_feedback_uses_process_photos_and_writes_process_feedback(self):
        client = FakeFeishuClient(
            {
                PROCESS_FEEDBACK_FIELD: "",
                PROCESS_PHOTO_FIELD: [
                    {"file_token": "tok_process", "name": "过程.jpg", "type": "image/jpeg"}
                ],
            }
        )

        call_qwen_vision, summarize = self._run_with_temp_root(client)

        self.assertEqual(client.get_record_call, ("rec_photo_001", DEFAULT_PHOTO_AI_TABLE_ID))
        self.assertEqual(client.download_calls[0]["field_id_or_name"], PROCESS_PHOTO_FIELD)
        self.assertEqual(client.update_calls[0], ("rec_photo_001", {PROCESS_FEEDBACK_FIELD: "融合后的反馈"}, DEFAULT_PHOTO_AI_TABLE_ID))
        call_qwen_vision.assert_called_once()
        summarize.assert_called_once()

    def test_existing_process_feedback_uses_final_photos_and_writes_final_feedback(self):
        client = FakeFeishuClient(
            {
                PROCESS_FEEDBACK_FIELD: "已有过程反馈",
                FINAL_PHOTO_FIELD: [
                    {"file_token": "tok_final", "name": "收尾.jpg", "type": "image/jpeg"}
                ],
            }
        )

        self._run_with_temp_root(client)

        self.assertEqual(client.download_calls[0]["field_id_or_name"], FINAL_PHOTO_FIELD)
        self.assertEqual(client.update_calls[0], ("rec_photo_001", {FINAL_FEEDBACK_FIELD: "融合后的反馈"}, DEFAULT_PHOTO_AI_TABLE_ID))

    def test_no_images_writes_short_failure_text(self):
        client = FakeFeishuClient({PROCESS_FEEDBACK_FIELD: "", PROCESS_PHOTO_FIELD: []})

        with tempfile.TemporaryDirectory() as temp_dir:
            process_photo_ai_record("rec_photo_001", client, project_root=Path(temp_dir))

        self.assertEqual(len(client.download_calls), 0)
        record_id, fields, table_id = client.update_calls[0]
        self.assertEqual(record_id, "rec_photo_001")
        self.assertEqual(table_id, DEFAULT_PHOTO_AI_TABLE_ID)
        self.assertIn(PROCESS_FEEDBACK_FIELD, fields)
        self.assertIn("照片AI识别失败", fields[PROCESS_FEEDBACK_FIELD])

    def test_all_recognition_failures_write_short_failure_text(self):
        client = FakeFeishuClient(
            {
                PROCESS_FEEDBACK_FIELD: "",
                PROCESS_PHOTO_FIELD: [
                    {"file_token": "tok_process", "name": "过程.jpg", "type": "image/jpeg"}
                ],
            }
        )

        with tempfile.TemporaryDirectory() as temp_dir, patch(
            "app.photo_ai_handler.get_qwen_api_key", return_value="qwen-key"
        ), patch("app.photo_ai_handler.call_qwen_vision", side_effect=RuntimeError("boom")):
            process_photo_ai_record("rec_photo_001", client, project_root=Path(temp_dir))

        record_id, fields, table_id = client.update_calls[0]
        self.assertEqual(record_id, "rec_photo_001")
        self.assertEqual(table_id, DEFAULT_PHOTO_AI_TABLE_ID)
        self.assertIn("图片全部识别失败", fields[PROCESS_FEEDBACK_FIELD])


if __name__ == "__main__":
    unittest.main()
