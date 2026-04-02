import asyncio
import unittest
from unittest.mock import MagicMock, patch

from app.certificate_query import EFFECTIVE_END_FIELD, ExtractedCertificateCard, PersonQueryResult
from app.service import (
    LOOKUP_MULTIPLE_MESSAGE,
    LOOKUP_NOT_FOUND_MESSAGE,
    REVIEW_ACTUAL_FIELD,
    REVIEW_DUE_FIELD,
    SKIPPED_WRITEBACK_MESSAGE,
    BatchRequestCoordinator,
    CertificateService,
    ClientDisconnectedError,
    QueueFullError,
    QueueTimeoutError,
)


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


class BatchRequestCoordinatorTests(unittest.TestCase):
    async def _always_connected(self):
        return False

    def test_queue_serializes_requests(self):
        async def scenario():
            coordinator = BatchRequestCoordinator(max_queue_size=10, queue_timeout_seconds=5)
            order = []
            release_first = asyncio.Event()
            first_started = asyncio.Event()

            async def first_work():
                order.append("first-start")
                first_started.set()
                await release_first.wait()
                order.append("first-end")
                return "first"

            async def second_work():
                order.append("second-start")
                return "second"

            first_task = asyncio.create_task(
                coordinator.run(request_id="req-1", work=first_work, is_disconnected=self._always_connected)
            )
            await first_started.wait()

            second_task = asyncio.create_task(
                coordinator.run(request_id="req-2", work=second_work, is_disconnected=self._always_connected)
            )
            await asyncio.sleep(0.05)
            self.assertEqual(order, ["first-start"])

            release_first.set()
            await asyncio.gather(first_task, second_task)
            self.assertEqual(order, ["first-start", "first-end", "second-start"])

        asyncio.run(scenario())

    def test_queue_full_raises_error(self):
        async def scenario():
            coordinator = BatchRequestCoordinator(max_queue_size=1, queue_timeout_seconds=5)
            release_first = asyncio.Event()
            first_started = asyncio.Event()

            async def first_work():
                first_started.set()
                await release_first.wait()
                return "first"

            async def second_work():
                return "second"

            first_task = asyncio.create_task(
                coordinator.run(request_id="req-1", work=first_work, is_disconnected=self._always_connected)
            )
            await first_started.wait()

            second_task = asyncio.create_task(
                coordinator.run(request_id="req-2", work=second_work, is_disconnected=self._always_connected)
            )
            await asyncio.sleep(0.05)

            with self.assertRaises(QueueFullError):
                await coordinator.run(
                    request_id="req-3",
                    work=second_work,
                    is_disconnected=self._always_connected,
                )

            release_first.set()
            await asyncio.gather(first_task, second_task)

        asyncio.run(scenario())

    def test_queue_timeout_raises_error(self):
        async def scenario():
            coordinator = BatchRequestCoordinator(max_queue_size=10, queue_timeout_seconds=1)
            release_first = asyncio.Event()
            first_started = asyncio.Event()

            async def first_work():
                first_started.set()
                await release_first.wait()
                return "first"

            async def second_work():
                return "second"

            first_task = asyncio.create_task(
                coordinator.run(request_id="req-1", work=first_work, is_disconnected=self._always_connected)
            )
            await first_started.wait()

            with self.assertRaises(QueueTimeoutError):
                await coordinator.run(
                    request_id="req-2",
                    work=second_work,
                    is_disconnected=self._always_connected,
                )

            release_first.set()
            await first_task

        asyncio.run(scenario())

    def test_client_disconnect_while_waiting_does_not_execute_work(self):
        async def scenario():
            coordinator = BatchRequestCoordinator(max_queue_size=10, queue_timeout_seconds=5)
            release_first = asyncio.Event()
            first_started = asyncio.Event()
            disconnect_second = False
            second_executed = False

            async def first_work():
                first_started.set()
                await release_first.wait()
                return "first"

            async def second_work():
                nonlocal second_executed
                second_executed = True
                return "second"

            async def second_disconnected():
                return disconnect_second

            first_task = asyncio.create_task(
                coordinator.run(request_id="req-1", work=first_work, is_disconnected=self._always_connected)
            )
            await first_started.wait()

            second_task = asyncio.create_task(
                coordinator.run(request_id="req-2", work=second_work, is_disconnected=second_disconnected)
            )
            await asyncio.sleep(0.05)
            disconnect_second = True

            with self.assertRaises(ClientDisconnectedError):
                await second_task

            release_first.set()
            await first_task
            self.assertFalse(second_executed)

        asyncio.run(scenario())


class CertificateServiceTests(unittest.TestCase):
    def build_lookup(self):
        return {
            "id_number_field": "ID Number",
            "name_field": "Name",
        }

    def build_field_mapping(self):
        return {
            "high_voltage": {
                "expire_field": "hv_expire",
                "review_due_field": "hv_review_due",
                "review_actual_field": "hv_review_actual",
                "attachment_field": "hv_attachment",
            },
            "low_voltage": {
                "expire_field": "lv_expire",
                "review_due_field": "lv_review_due",
                "review_actual_field": "lv_review_actual",
                "attachment_field": "lv_attachment",
            },
        }

    def build_service(self):
        fake_feishu = MagicMock()
        with patch("app.service.FeishuTableReader", return_value=fake_feishu):
            service = CertificateService(
                feishu_config={
                    "app_id": "cli_xxx",
                    "app_secret": "secret_xxx",
                    "app_token": "base_xxx",
                    "table_id": "tbl_xxx",
                }
            )
        return service, fake_feishu

    def test_build_feishu_fields_clears_other_mapped_fields_before_overwrite(self):
        service, fake_feishu = self.build_service()
        fake_feishu.upload_image.return_value = "file-token-1"
        fake_feishu.build_attachment_field.return_value = [{"file_token": "file-token-1"}]

        fields = service.build_feishu_fields(
            result=build_success_result(),
            record_reference="rec_001",
            field_mapping=self.build_field_mapping(),
        )

        self.assertEqual(fields["hv_attachment"], [{"file_token": "file-token-1"}])
        self.assertIsNone(fields["lv_expire"])
        self.assertIsNone(fields["lv_review_due"])
        self.assertIsNone(fields["lv_review_actual"])
        self.assertEqual(fields["lv_attachment"], [])

    def test_build_feishu_fields_unmapped_certificates_only_clear_tracked_fields(self):
        service, fake_feishu = self.build_service()
        result = build_success_result()
        result.selected_certificates = {
            "working_at_height": ExtractedCertificateCard(
                fields={EFFECTIVE_END_FIELD: "2030-11-06"},
                screenshot_bytes=b"unused",
            )
        }

        fields = service.build_feishu_fields(
            result=result,
            record_reference="rec_001",
            field_mapping=self.build_field_mapping(),
        )

        self.assertIsNone(fields["hv_expire"])
        self.assertIsNone(fields["hv_review_due"])
        self.assertIsNone(fields["hv_review_actual"])
        self.assertEqual(fields["hv_attachment"], [])
        fake_feishu.upload_image.assert_not_called()

    def test_process_batch_request_success_returns_counts(self):
        service, fake_feishu = self.build_service()
        fake_feishu.ensure_token.return_value = "tenant-token"
        fake_feishu.upload_image.return_value = "file-token-1"
        fake_feishu.build_attachment_field.return_value = [{"file_token": "file-token-1"}]
        fake_feishu.update_record.return_value = True

        fake_query = MagicMock()
        fake_query.__enter__.return_value = fake_query
        fake_query.__exit__.return_value = False
        fake_query.run_batch_query.return_value = [build_success_result()]

        with patch("app.service.CertificateQuery", return_value=fake_query):
            result = service.process_batch_request(
                request_id="req-1",
                people=[{"record_id": "rec_001", "name": "Li Shilong", "id_number": "13012620001028361X"}],
                lookup=None,
                field_mapping=self.build_field_mapping(),
                debug=False,
            )

        self.assertEqual(result.total, 1)
        self.assertEqual(result.success, 1)
        self.assertEqual(result.failed, 0)
        self.assertEqual(result.results[0].record_id, "rec_001")
        self.assertEqual(result.results[0].name, "Li Shilong")
        self.assertEqual(result.results[0].id_number, "13012620001028361X")
        self.assertTrue(result.results[0].success)
        fake_feishu.ensure_token.assert_called_once()
        fake_feishu.update_record.assert_called_once()
        fake_feishu.list_records.assert_not_called()

    def test_process_batch_request_looks_up_record_id_when_missing(self):
        service, fake_feishu = self.build_service()
        fake_feishu.ensure_token.return_value = "tenant-token"
        fake_feishu.list_records.return_value = [
            {
                "record_id": "rec_lookup_001",
                "fields": {
                    "ID Number": "13012620001028361x",
                    "Name": "Li Shilong",
                },
            }
        ]
        fake_feishu.upload_image.return_value = "file-token-1"
        fake_feishu.build_attachment_field.return_value = [{"file_token": "file-token-1"}]
        fake_feishu.update_record.return_value = True

        fake_query = MagicMock()
        fake_query.__enter__.return_value = fake_query
        fake_query.__exit__.return_value = False
        fake_query.run_batch_query.return_value = [build_success_result(record_id="")]

        with patch("app.service.CertificateQuery", return_value=fake_query):
            result = service.process_batch_request(
                request_id="req-lookup",
                people=[{"record_id": "", "name": "Li Shilong", "id_number": "13012620001028361X"}],
                lookup=self.build_lookup(),
                field_mapping=self.build_field_mapping(),
                debug=False,
            )

        self.assertEqual(result.success, 1)
        self.assertEqual(result.results[0].record_id, "rec_lookup_001")
        fake_feishu.list_records.assert_called_once()
        fake_feishu.update_record.assert_called_once_with("rec_lookup_001", unittest.mock.ANY)

    def test_process_batch_request_skips_writeback_on_query_failure(self):
        service, fake_feishu = self.build_service()
        fake_feishu.ensure_token.return_value = "tenant-token"

        fake_query = MagicMock()
        fake_query.__enter__.return_value = fake_query
        fake_query.__exit__.return_value = False
        fake_query.run_batch_query.return_value = [build_failed_query_result()]

        with patch("app.service.CertificateQuery", return_value=fake_query):
            result = service.process_batch_request(
                request_id="req-1",
                people=[{"record_id": "rec_001", "name": "Li Shilong", "id_number": "13012620001028361X"}],
                lookup=None,
                field_mapping=self.build_field_mapping(),
                debug=True,
            )

        self.assertEqual(result.total, 1)
        self.assertEqual(result.success, 0)
        self.assertEqual(result.failed, 1)
        self.assertEqual(result.results[0].query_status, "未查询到证件信息")
        self.assertEqual(result.results[0].writeback_error, SKIPPED_WRITEBACK_MESSAGE)
        fake_feishu.update_record.assert_not_called()

    def test_process_batch_request_reports_writeback_error(self):
        service, fake_feishu = self.build_service()
        fake_feishu.ensure_token.return_value = "tenant-token"
        fake_feishu.upload_image.side_effect = RuntimeError("upload failed")

        fake_query = MagicMock()
        fake_query.__enter__.return_value = fake_query
        fake_query.__exit__.return_value = False
        fake_query.run_batch_query.return_value = [build_success_result()]

        with patch("app.service.CertificateQuery", return_value=fake_query):
            result = service.process_batch_request(
                request_id="req-1",
                people=[{"record_id": "rec_001", "name": "Li Shilong", "id_number": "13012620001028361X"}],
                lookup=None,
                field_mapping=self.build_field_mapping(),
                debug=True,
            )

        self.assertEqual(result.success, 0)
        self.assertEqual(result.failed, 1)
        self.assertEqual(result.results[0].query_status, "查询成功")
        self.assertEqual(result.results[0].writeback_error, "upload failed")

    def test_process_batch_request_fails_when_lookup_finds_no_record(self):
        service, fake_feishu = self.build_service()
        fake_feishu.ensure_token.return_value = "tenant-token"
        fake_feishu.list_records.return_value = []

        fake_query = MagicMock()
        fake_query.__enter__.return_value = fake_query
        fake_query.__exit__.return_value = False
        fake_query.run_batch_query.return_value = [build_success_result(record_id="")]

        with patch("app.service.CertificateQuery", return_value=fake_query):
            result = service.process_batch_request(
                request_id="req-lookup-miss",
                people=[{"record_id": "", "name": "Li Shilong", "id_number": "13012620001028361X"}],
                lookup=self.build_lookup(),
                field_mapping=self.build_field_mapping(),
                debug=True,
            )

        self.assertEqual(result.success, 0)
        self.assertEqual(result.results[0].writeback_error, LOOKUP_NOT_FOUND_MESSAGE)
        fake_feishu.update_record.assert_not_called()

    def test_process_batch_request_fails_when_lookup_matches_multiple_records(self):
        service, fake_feishu = self.build_service()
        fake_feishu.ensure_token.return_value = "tenant-token"
        fake_feishu.list_records.return_value = [
            {
                "record_id": "rec_a",
                "fields": {"ID Number": "13012620001028361X", "Name": "Li Shilong"},
            },
            {
                "record_id": "rec_b",
                "fields": {"ID Number": "13012620001028361X", "Name": "Li Shilong"},
            },
        ]

        fake_query = MagicMock()
        fake_query.__enter__.return_value = fake_query
        fake_query.__exit__.return_value = False
        fake_query.run_batch_query.return_value = [build_success_result(record_id="")]

        with patch("app.service.CertificateQuery", return_value=fake_query):
            result = service.process_batch_request(
                request_id="req-lookup-multi",
                people=[{"record_id": "", "name": "Li Shilong", "id_number": "13012620001028361X"}],
                lookup=self.build_lookup(),
                field_mapping=self.build_field_mapping(),
                debug=True,
            )

        self.assertEqual(result.success, 0)
        self.assertEqual(result.results[0].writeback_error, LOOKUP_MULTIPLE_MESSAGE)
        fake_feishu.update_record.assert_not_called()


if __name__ == "__main__":
    unittest.main()
