import unittest
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from fastapi.testclient import TestClient

from app import server
from app.service import BatchPersonResult, BatchProcessResult, QueueFullError, QueueTimeoutError, QueuedRunResult


class BatchApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(server.app)

    def build_payload(self):
        return {
            "feishu": {
                "app_id": "cli_xxx",
                "app_secret": "secret_xxx",
                "app_token": "base_xxx",
                "table_id": "tbl_xxx",
            },
            "lookup": {
                "id_number_field": "ID Number",
                "name_field": "Name",
            },
            "field_mapping": {
                "high_voltage": {
                    "expire_field": "hv_expire",
                    "review_due_field": "hv_review_due",
                    "review_actual_field": "hv_review_actual",
                    "attachment_field": "hv_attachment",
                }
            },
            "people": [
                {
                    "name": "Li Shilong",
                    "id_number": "13012620001028361X",
                }
            ],
        }

    def test_healthz(self):
        response = self.client.get("/healthz")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})

    def test_ui_index_returns_html(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn("施工安全控制台", response.text)
        self.assertNotIn("Local Operations Console", response.text)
        self.assertNotIn("SAFETY AUTOMATION", response.text)
        self.assertNotIn("照片 AI", response.text)
        self.assertNotIn("任务ID", response.text)
        self.assertNotIn("记录ID", response.text)

    def test_uvicorn_access_log_is_disabled(self):
        self.assertTrue(server.logging.getLogger("uvicorn.access").disabled)
        self.assertGreaterEqual(server.logging.getLogger("uvicorn.access").level, server.logging.WARNING)

    def test_ui_status_returns_task_and_queue_info(self):
        response = self.client.get("/api/ui/status")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["service"]["status"], "ok")
        self.assertIn("queue", data)
        self.assertIn("tasks", data)

    def test_ui_config_masks_sensitive_values(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text("QWEN_API_KEY=sk-abcdef123456\nPORT=58000\n", encoding="utf-8")
            with patch.object(server, "loaded_env_path", env_path):
                response = self.client.get("/api/ui/config")

        self.assertEqual(response.status_code, 200)
        items = {item["key"]: item for item in response.json()["items"]}
        self.assertEqual(items["QWEN_API_KEY"]["value"], "sk-a••••3456")
        self.assertEqual(items["PORT"]["value"], "58000")

    def test_ui_config_update_writes_env_and_process_env(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text("PORT=58000\n", encoding="utf-8")
            with patch.object(server, "loaded_env_path", env_path):
                response = self.client.put(
                    "/api/ui/config",
                    json={"values": {"PORT": "58001", "QWEN_API_KEY": "sk-new"}},
                )

            content = env_path.read_text(encoding="utf-8")

        self.assertEqual(response.status_code, 200)
        self.assertIn("PORT=58001", content)
        self.assertIn("QWEN_API_KEY=sk-new", content)
        self.assertEqual(server.os.environ.get("PORT"), "58001")
        self.assertEqual(server.os.environ.get("QWEN_API_KEY"), "sk-new")

    def test_ui_certificate_trigger_creates_task(self):
        server._feishu_handler_client = Mock()

        class ImmediateThread:
            def __init__(self, *, target, name, daemon):
                self.target = target

            def start(self):
                self.target()

        with patch("app.server.threading.Thread", ImmediateThread), patch(
            "app.message_handler.claim_record_processing", return_value=True
        ), patch("app.message_handler.finish_record_processing"), patch(
            "app.message_handler.process_record_message"
        ) as process_record_message:
            response = self.client.post("/api/ui/certificate/trigger", json={"record_id": "rec_ui_cert"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["workflow"], "certificate")
        self.assertIn("task_id", response.json())
        process_record_message.assert_called_once()

    def test_ui_photo_ai_trigger_creates_task(self):
        server._feishu_handler_client = Mock()

        class ImmediateThread:
            def __init__(self, *, target, name, daemon):
                self.target = target

            def start(self):
                self.target()

        with patch("app.server.threading.Thread", ImmediateThread), patch(
            "app.message_handler.claim_record_processing", return_value=True
        ), patch("app.message_handler.finish_record_processing"), patch(
            "app.photo_ai_handler.process_photo_ai_record"
        ) as process_photo_ai_record:
            response = self.client.post("/api/ui/photo-ai/trigger", json={"record_id": "rec_ui_photo"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["workflow"], "photo_ai")
        self.assertIn("task_id", response.json())
        process_photo_ai_record.assert_called_once()

    def test_empty_people_returns_400(self):
        payload = self.build_payload()
        payload["people"] = []
        response = self.client.post("/api/v1/query/batch", json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"], "people 不能为空")

    def test_missing_lookup_returns_400_when_record_id_is_omitted(self):
        payload = self.build_payload()
        payload["lookup"] = None
        response = self.client.post("/api/v1/query/batch", json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"], "未传入 people[].record_id 时，必须提供 lookup 配置")

    def test_blank_lookup_id_number_field_returns_400(self):
        payload = self.build_payload()
        payload["lookup"]["id_number_field"] = "   "
        response = self.client.post("/api/v1/query/batch", json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"], "lookup.id_number_field 不能为空")

    def test_explicit_record_id_does_not_require_lookup(self):
        payload = self.build_payload()
        payload["lookup"] = None
        payload["people"][0]["record_id"] = "rec_001"

        queued_result = QueuedRunResult(
            result=BatchProcessResult(
                total=1,
                success=1,
                failed=0,
                results=[
                    BatchPersonResult(
                        name="Li Shilong",
                        id_number="13012620001028361X",
                        record_id="rec_001",
                        success=True,
                        query_status="查询成功",
                    )
                ],
                query_seconds=0.5,
                writeback_seconds=0.1,
            ),
            queued_seconds=0.0,
            execution_seconds=0.6,
        )
        with patch.object(server.REQUEST_COORDINATOR, "run", new=AsyncMock(return_value=queued_result)):
            response = self.client.post("/api/v1/query/batch", json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["results"][0]["record_id"], "rec_001")
        self.assertEqual(response.json()["results"][0]["query_status"], "查询成功")

    def test_blank_name_returns_400(self):
        payload = self.build_payload()
        payload["people"][0]["name"] = "   "
        response = self.client.post("/api/v1/query/batch", json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"], "people[1].name 不能为空")

    def test_batch_over_limit_returns_400(self):
        payload = self.build_payload()
        payload["people"] = [
            {
                "name": f"user_{index}",
                "id_number": "13012620001028361X",
            }
            for index in range(21)
        ]
        response = self.client.post("/api/v1/query/batch", json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"], "people 最多支持 20 条")

    def test_empty_field_mapping_returns_400(self):
        payload = self.build_payload()
        payload["field_mapping"] = {}
        response = self.client.post("/api/v1/query/batch", json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"], "field_mapping 至少需要配置一种证书类型")

    def test_queue_full_returns_429(self):
        payload = self.build_payload()
        with patch.object(
            server.REQUEST_COORDINATOR,
            "run",
            new=AsyncMock(side_effect=QueueFullError("queue is full")),
        ):
            response = self.client.post("/api/v1/query/batch", json=payload)
        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.json()["detail"], "queue is full")

    def test_queue_timeout_returns_503(self):
        payload = self.build_payload()
        with patch.object(
            server.REQUEST_COORDINATOR,
            "run",
            new=AsyncMock(side_effect=QueueTimeoutError("queue timed out")),
        ):
            response = self.client.post("/api/v1/query/batch", json=payload)
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["detail"], "queue timed out")

    def test_success_returns_minimal_response(self):
        payload = self.build_payload()
        queued_result = QueuedRunResult(
            result=BatchProcessResult(
                total=1,
                success=1,
                failed=0,
                results=[
                    BatchPersonResult(
                        name="Li Shilong",
                        id_number="13012620001028361X",
                        record_id="rec_001",
                        success=True,
                        query_status="查询成功",
                    )
                ],
                query_seconds=1.0,
                writeback_seconds=0.2,
            ),
            queued_seconds=0.0,
            execution_seconds=1.2,
        )
        with patch.object(server.REQUEST_COORDINATOR, "run", new=AsyncMock(return_value=queued_result)):
            response = self.client.post("/api/v1/query/batch", json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "total": 1,
                "success": 1,
                "failed": 0,
                "results": [
                    {
                        "name": "Li Shilong",
                        "id_number": "13012620001028361X",
                        "record_id": "rec_001",
                        "success": True,
                        "query_status": "查询成功",
                    }
                ],
            },
        )

    def test_response_always_keeps_query_and_writeback_errors(self):
        payload = self.build_payload()
        queued_result = QueuedRunResult(
            result=BatchProcessResult(
                total=2,
                success=1,
                failed=1,
                results=[
                    BatchPersonResult(
                        name="Li Shilong",
                        id_number="13012620001028361X",
                        record_id="rec_001",
                        success=True,
                        query_status="查询成功",
                    ),
                    BatchPersonResult(
                        name="Fan Shaohua",
                        id_number="320601199203020330",
                        success=False,
                        query_status="未查询到证件信息",
                        query_error="没有查询到相关证件信息",
                        writeback_error="查询未成功，跳过回填",
                    ),
                ],
                query_seconds=1.0,
                writeback_seconds=0.2,
            ),
            queued_seconds=0.4,
            execution_seconds=1.2,
        )
        with patch.object(server.REQUEST_COORDINATOR, "run", new=AsyncMock(return_value=queued_result)):
            response = self.client.post("/api/v1/query/batch", json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json()["results"],
            [
                {
                    "name": "Li Shilong",
                    "id_number": "13012620001028361X",
                    "record_id": "rec_001",
                    "success": True,
                    "query_status": "查询成功",
                },
                {
                    "name": "Fan Shaohua",
                    "id_number": "320601199203020330",
                    "success": False,
                    "query_status": "未查询到证件信息",
                    "query_error": "没有查询到相关证件信息",
                    "writeback_error": "查询未成功，跳过回填",
                },
            ],
        )


if __name__ == "__main__":
    unittest.main()
