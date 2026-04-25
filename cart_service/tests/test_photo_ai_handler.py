import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app.photo_ai_handler import (
    DEFAULT_PHOTO_AI_TABLE_ID,
    DEFAULT_PHOTO_AI_REQUIREMENT_TABLE_ID,
    FINAL_FEEDBACK_FIELD,
    FINAL_PHOTO_FIELD,
    PHOTO_AI_JOB_TYPE_FIELD,
    PROCESS_FEEDBACK_FIELD,
    PROCESS_PHOTO_FIELD,
    REQUIREMENT_FINAL_FIELD,
    REQUIREMENT_JOB_TYPE_FIELD,
    REQUIREMENT_PROCESS_FIELD,
    process_photo_ai_record,
)


class FakeFeishuClient:
    def __init__(self, fields, requirement_records=None):
        self.fields = fields
        self.requirement_records = requirement_records or []
        self.download_calls = []
        self.update_calls = []
        self.list_records_calls = []

    def get_record(self, record_id, table_id=None):
        self.get_record_call = (record_id, table_id)
        return self.fields

    def list_records(self, *, field_names=None, page_size=500, table_id=None):
        self.list_records_calls.append(
            {"field_names": field_names, "page_size": page_size, "table_id": table_id}
        )
        return self.requirement_records

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

    def test_record_job_type_loads_requirements_and_passes_them_to_qwen(self):
        client = FakeFeishuClient(
            {
                PROCESS_FEEDBACK_FIELD: "",
                PHOTO_AI_JOB_TYPE_FIELD: "高处作业",
                PROCESS_PHOTO_FIELD: [
                    {"file_token": "tok_process", "name": "过程.jpg", "type": "image/jpeg"}
                ],
            },
            requirement_records=[
                SimpleNamespace(
                    fields={
                        REQUIREMENT_JOB_TYPE_FIELD: "高处作业",
                        REQUIREMENT_PROCESS_FIELD: "过程必须佩戴安全帽并正确系挂安全带。",
                        REQUIREMENT_FINAL_FIELD: "收尾必须清理工具材料并恢复现场围挡。",
                    }
                )
            ],
        )

        with tempfile.TemporaryDirectory() as temp_dir, patch(
            "app.photo_ai_handler.get_qwen_api_key", return_value="qwen-key"
        ), patch(
            "app.photo_ai_handler.call_qwen_vision",
            return_value="带规范识别结果",
        ) as call_qwen_vision, patch(
            "app.photo_ai_handler.summarize_recognition_results", return_value="融合后的反馈"
        ) as summarize:
            process_photo_ai_record("rec_photo_001", client, project_root=Path(temp_dir))

        self.assertEqual(client.list_records_calls[0]["table_id"], DEFAULT_PHOTO_AI_REQUIREMENT_TABLE_ID)
        call_qwen_vision.assert_called_once()
        first_call = call_qwen_vision.call_args
        self.assertEqual(first_call.kwargs["job_type"], "高处作业")
        self.assertIn("安全带", first_call.kwargs["process_requirement"])
        self.assertIn("恢复现场围挡", first_call.kwargs["final_requirement"])
        self.assertEqual(summarize.call_args.kwargs["job_type"], "高处作业")
        self.assertIn("安全带", summarize.call_args.kwargs["process_requirement"])
        self.assertEqual(client.update_calls[0], ("rec_photo_001", {PROCESS_FEEDBACK_FIELD: "融合后的反馈"}, DEFAULT_PHOTO_AI_TABLE_ID))

    def test_multiple_record_job_types_combine_requirements(self):
        client = FakeFeishuClient(
            {
                PROCESS_FEEDBACK_FIELD: "",
                PHOTO_AI_JOB_TYPE_FIELD: [
                    {"text": "高处作业"},
                    {"text": "临时用电作业"},
                ],
                PROCESS_PHOTO_FIELD: [
                    {"file_token": "tok_process", "name": "过程.jpg", "type": "image/jpeg"}
                ],
            },
            requirement_records=[
                SimpleNamespace(
                    fields={
                        REQUIREMENT_JOB_TYPE_FIELD: "高处作业",
                        REQUIREMENT_PROCESS_FIELD: "高处过程要求。",
                        REQUIREMENT_FINAL_FIELD: "高处收尾要求。",
                    }
                ),
                SimpleNamespace(
                    fields={
                        REQUIREMENT_JOB_TYPE_FIELD: "临时用电作业",
                        REQUIREMENT_PROCESS_FIELD: "用电过程要求。",
                        REQUIREMENT_FINAL_FIELD: "用电收尾要求。",
                    }
                ),
            ],
        )

        with tempfile.TemporaryDirectory() as temp_dir, patch(
            "app.photo_ai_handler.get_qwen_api_key", return_value="qwen-key"
        ), patch(
            "app.photo_ai_handler.call_qwen_vision", return_value="带规范识别结果"
        ) as call_qwen_vision, patch(
            "app.photo_ai_handler.summarize_recognition_results", return_value="融合后的反馈"
        ) as summarize:
            process_photo_ai_record("rec_photo_001", client, project_root=Path(temp_dir))

        call_kwargs = call_qwen_vision.call_args.kwargs
        self.assertEqual(call_kwargs["job_type"], "高处作业、临时用电作业")
        self.assertIn("【高处作业】", call_kwargs["process_requirement"])
        self.assertIn("高处过程要求", call_kwargs["process_requirement"])
        self.assertIn("【临时用电作业】", call_kwargs["process_requirement"])
        self.assertIn("用电过程要求", call_kwargs["process_requirement"])
        self.assertIn("高处收尾要求", summarize.call_args.kwargs["final_requirement"])
        self.assertIn("用电收尾要求", summarize.call_args.kwargs["final_requirement"])


if __name__ == "__main__":
    unittest.main()
