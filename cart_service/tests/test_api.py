import unittest
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from app.certificate_query import EFFECTIVE_END_FIELD, ExtractedCertificateCard, PersonQueryResult
from app.server import REVIEW_ACTUAL_FIELD, REVIEW_DUE_FIELD, app, date_to_timestamp


def build_success_result(record_id: str = "rec_001") -> PersonQueryResult:
    high_voltage_card = ExtractedCertificateCard(
        fields={
            EFFECTIVE_END_FIELD: "2030-11-06",
            REVIEW_DUE_FIELD: "2027-11-06",
            REVIEW_ACTUAL_FIELD: "2026-03-01",
        },
        screenshot_bytes=b"fake-jpeg-bytes",
    )
    return PersonQueryResult(
        record_id=record_id,
        name="Li Shilong",
        id_number="13012620001028361X",
        status="success",
        error=None,
        records=[high_voltage_card.fields],
        selected_certificates={"high_voltage": high_voltage_card},
        queried_at="2026-03-30 16:00:00",
    )


def build_failed_query_result(record_id: str = "rec_001") -> PersonQueryResult:
    return PersonQueryResult(
        record_id=record_id,
        name="Li Shilong",
        id_number="13012620001028361X",
        status="fail_no_data",
        error="no data",
        records=[],
        selected_certificates={},
        queried_at="2026-03-30 16:00:00",
    )


class BatchApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)

    def build_payload(self):
        return {
            "feishu": {
                "app_id": "cli_xxx",
                "app_secret": "secret_xxx",
                "app_token": "base_xxx",
                "table_id": "tbl_xxx",
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
                    "record_id": "rec_001",
                    "name": "Li Shilong",
                    "id_number": "13012620001028361X",
                }
            ],
        }

    def patch_query_and_feishu(self, fake_query, fake_feishu):
        query_patch = patch("app.server.CertificateQuery", return_value=fake_query)
        feishu_patch = patch("app.server.FeishuTableReader", return_value=fake_feishu)
        query_patch.start()
        feishu_patch.start()
        self.addCleanup(query_patch.stop)
        self.addCleanup(feishu_patch.stop)

    def test_healthz(self):
        response = self.client.get("/healthz")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})

    def test_empty_people_returns_400(self):
        payload = self.build_payload()
        payload["people"] = []
        response = self.client.post("/api/v1/query/batch", json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertIn("people", response.json()["detail"])

    def test_missing_record_id_returns_400(self):
        payload = self.build_payload()
        payload["people"] = [{"name": "Zhang San", "id_number": "13012620001028361X"}]
        response = self.client.post("/api/v1/query/batch", json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"], "请求参数不合法")

    def test_batch_over_limit_returns_400(self):
        payload = self.build_payload()
        payload["people"] = [
            {
                "record_id": f"rec_{index}",
                "name": f"user_{index}",
                "id_number": "13012620001028361X",
            }
            for index in range(21)
        ]
        response = self.client.post("/api/v1/query/batch", json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertIn("单次最多支持", response.json()["detail"])

    def test_empty_field_mapping_returns_400(self):
        payload = self.build_payload()
        payload["field_mapping"] = {}
        response = self.client.post("/api/v1/query/batch", json=payload)
        self.assertEqual(response.status_code, 400)
        self.assertIn("field_mapping", response.json()["detail"])

    def test_query_and_writeback_success_returns_minimal_response(self):
        fake_query = MagicMock()
        fake_query.__enter__.return_value = fake_query
        fake_query.__exit__.return_value = False
        fake_query.run_batch_query.return_value = [build_success_result()]

        fake_feishu = MagicMock()
        fake_feishu.ensure_token.return_value = "tenant-token"
        fake_feishu.upload_image.return_value = "file-token-1"
        fake_feishu.build_attachment_field.return_value = [{"file_token": "file-token-1"}]
        fake_feishu.update_record.return_value = True

        self.patch_query_and_feishu(fake_query, fake_feishu)
        payload = self.build_payload()
        response = self.client.post("/api/v1/query/batch", json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "total": 1,
                "success": 1,
                "failed": 0,
                "results": [{"record_id": "rec_001", "success": True}],
            },
        )
        fake_feishu.upload_image.assert_called_once()
        update_record_id, update_fields = fake_feishu.update_record.call_args[0]
        mapping = payload["field_mapping"]["high_voltage"]
        self.assertEqual(update_record_id, "rec_001")
        self.assertEqual(update_fields[mapping["expire_field"]], date_to_timestamp("2030-11-06"))
        self.assertEqual(update_fields[mapping["review_due_field"]], date_to_timestamp("2027-11-06"))
        self.assertEqual(update_fields[mapping["review_actual_field"]], date_to_timestamp("2026-03-01"))
        self.assertEqual(update_fields[mapping["attachment_field"]], [{"file_token": "file-token-1"}])

    def test_writeback_clears_other_mapped_fields_before_overwrite(self):
        fake_query = MagicMock()
        fake_query.__enter__.return_value = fake_query
        fake_query.__exit__.return_value = False
        fake_query.run_batch_query.return_value = [build_success_result()]

        fake_feishu = MagicMock()
        fake_feishu.ensure_token.return_value = "tenant-token"
        fake_feishu.upload_image.return_value = "file-token-1"
        fake_feishu.build_attachment_field.return_value = [{"file_token": "file-token-1"}]
        fake_feishu.update_record.return_value = True

        self.patch_query_and_feishu(fake_query, fake_feishu)
        payload = self.build_payload()
        payload["field_mapping"]["low_voltage"] = {
            "expire_field": "lv_expire",
            "review_due_field": "lv_review_due",
            "review_actual_field": "lv_review_actual",
            "attachment_field": "lv_attachment",
        }
        response = self.client.post("/api/v1/query/batch", json=payload)

        self.assertEqual(response.status_code, 200)
        update_record_id, update_fields = fake_feishu.update_record.call_args[0]
        high_mapping = payload["field_mapping"]["high_voltage"]
        low_mapping = payload["field_mapping"]["low_voltage"]
        self.assertEqual(update_record_id, "rec_001")
        self.assertIsNone(update_fields[low_mapping["expire_field"]])
        self.assertIsNone(update_fields[low_mapping["review_due_field"]])
        self.assertIsNone(update_fields[low_mapping["review_actual_field"]])
        self.assertEqual(update_fields[low_mapping["attachment_field"]], [])
        self.assertEqual(update_fields[high_mapping["expire_field"]], date_to_timestamp("2030-11-06"))

    def test_unmapped_certificates_are_ignored_but_mapped_fields_still_cleared(self):
        result = build_success_result()
        result.selected_certificates = {
            "low_voltage": ExtractedCertificateCard(
                fields={
                    EFFECTIVE_END_FIELD: "2030-11-06",
                },
                screenshot_bytes=b"ignored",
            )
        }

        fake_query = MagicMock()
        fake_query.__enter__.return_value = fake_query
        fake_query.__exit__.return_value = False
        fake_query.run_batch_query.return_value = [result]

        fake_feishu = MagicMock()
        fake_feishu.ensure_token.return_value = "tenant-token"
        fake_feishu.update_record.return_value = True

        self.patch_query_and_feishu(fake_query, fake_feishu)
        payload = self.build_payload()
        response = self.client.post("/api/v1/query/batch", json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["results"], [{"record_id": "rec_001", "success": True}])
        fake_feishu.upload_image.assert_not_called()
        update_record_id, update_fields = fake_feishu.update_record.call_args[0]
        mapping = payload["field_mapping"]["high_voltage"]
        self.assertEqual(update_record_id, "rec_001")
        self.assertIsNone(update_fields[mapping["expire_field"]])
        self.assertIsNone(update_fields[mapping["review_due_field"]])
        self.assertIsNone(update_fields[mapping["review_actual_field"]])
        self.assertEqual(update_fields[mapping["attachment_field"]], [])

    def test_writeback_failure_marks_person_unsuccessful(self):
        fake_query = MagicMock()
        fake_query.__enter__.return_value = fake_query
        fake_query.__exit__.return_value = False
        fake_query.run_batch_query.return_value = [build_success_result()]

        fake_feishu = MagicMock()
        fake_feishu.ensure_token.return_value = "tenant-token"
        fake_feishu.upload_image.side_effect = RuntimeError("upload failed")

        self.patch_query_and_feishu(fake_query, fake_feishu)
        response = self.client.post("/api/v1/query/batch", json=self.build_payload())

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["results"], [{"record_id": "rec_001", "success": False}])

    def test_debug_mode_returns_query_status_and_error(self):
        fake_query = MagicMock()
        fake_query.__enter__.return_value = fake_query
        fake_query.__exit__.return_value = False
        fake_query.run_batch_query.return_value = [build_failed_query_result()]

        fake_feishu = MagicMock()
        fake_feishu.ensure_token.return_value = "tenant-token"

        self.patch_query_and_feishu(fake_query, fake_feishu)
        payload = self.build_payload()
        payload["debug"] = True
        response = self.client.post("/api/v1/query/batch", json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json()["results"],
            [
                {
                    "record_id": "rec_001",
                    "success": False,
                    "query_status": "fail_no_data",
                    "query_error": "no data",
                }
            ],
        )

    def test_debug_mode_returns_writeback_error(self):
        fake_query = MagicMock()
        fake_query.__enter__.return_value = fake_query
        fake_query.__exit__.return_value = False
        fake_query.run_batch_query.return_value = [build_success_result()]

        fake_feishu = MagicMock()
        fake_feishu.ensure_token.return_value = "tenant-token"
        fake_feishu.upload_image.side_effect = RuntimeError("upload failed")

        self.patch_query_and_feishu(fake_query, fake_feishu)
        payload = self.build_payload()
        payload["debug"] = True
        response = self.client.post("/api/v1/query/batch", json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json()["results"],
            [
                {
                    "record_id": "rec_001",
                    "success": False,
                    "query_status": "success",
                    "writeback_error": "upload failed",
                }
            ],
        )


if __name__ == "__main__":
    unittest.main()
