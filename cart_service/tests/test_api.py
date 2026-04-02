import unittest
from unittest.mock import AsyncMock, patch

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

    def test_empty_people_returns_400(self):
        payload = self.build_payload()
        payload["people"] = []
        response = self.client.post("/api/v1/query/batch", json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"], "people cannot be empty")

    def test_missing_lookup_returns_400_when_record_id_is_omitted(self):
        payload = self.build_payload()
        payload["lookup"] = None
        response = self.client.post("/api/v1/query/batch", json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"], "lookup is required when people[].record_id is omitted")

    def test_blank_lookup_id_number_field_returns_400(self):
        payload = self.build_payload()
        payload["lookup"]["id_number_field"] = "   "
        response = self.client.post("/api/v1/query/batch", json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"], "lookup.id_number_field cannot be empty")

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

    def test_blank_name_returns_400(self):
        payload = self.build_payload()
        payload["people"][0]["name"] = "   "
        response = self.client.post("/api/v1/query/batch", json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"], "people[1].name cannot be empty")

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
        self.assertEqual(response.json()["detail"], "people supports at most 20 entries")

    def test_empty_field_mapping_returns_400(self):
        payload = self.build_payload()
        payload["field_mapping"] = {}
        response = self.client.post("/api/v1/query/batch", json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"], "field_mapping must contain at least one certificate type")

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
                    }
                ],
            },
        )

    def test_debug_mode_keeps_query_and_writeback_errors(self):
        payload = self.build_payload()
        payload["debug"] = True
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
                        query_status="success",
                    ),
                    BatchPersonResult(
                        name="Fan Shaohua",
                        id_number="320601199203020330",
                        success=False,
                        query_status="fail_no_data",
                        query_error="no data",
                        writeback_error="query skipped",
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
                    "query_status": "success",
                },
                {
                    "name": "Fan Shaohua",
                    "id_number": "320601199203020330",
                    "success": False,
                    "query_status": "fail_no_data",
                    "query_error": "no data",
                    "writeback_error": "query skipped",
                },
            ],
        )


if __name__ == "__main__":
    unittest.main()
