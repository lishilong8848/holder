import unittest
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import patch

from PIL import Image

from app.certificate_query import (
    EFFECTIVE_END_FIELD,
    NAME_FIELD,
    OPERATION_ITEM_FIELD,
    CertificateQuery,
    ExtractedCertificateCard,
)




class FakeElement:
    def __init__(self, text="", displayed=True):
        self.text = text
        self._displayed = displayed

    def is_displayed(self):
        return self._displayed


class FakeRow:
    def __init__(self, headers, values):
        self.headers = [FakeElement(text=value) for value in headers]
        self.values = [FakeElement(text=value) for value in values]

    def find_elements(self, by, value):
        if by == "tag name" and value == "th":
            return self.headers
        if by == "tag name" and value == "td":
            return self.values
        return []


class FakeTable:
    def __init__(self, rows, screenshot_bytes):
        self.rows = rows
        self._screenshot_bytes = screenshot_bytes

    def find_elements(self, by, _value):
        if by == "xpath":
            return self.rows
        return []

    @property
    def screenshot_as_png(self):
        return self._screenshot_bytes

    def is_displayed(self):
        return True


class FakeDriver:
    def __init__(self, *, tables=None, no_result_elements=None):
        self._tables = tables or []
        self._no_result_elements = no_result_elements or []

    def find_elements(self, by, value):
        if by == "xpath":
            return self._tables
        if by == "class name" and value == "nocert-content":
            return self._no_result_elements
        return []

    def execute_script(self, _script, _table, _history_text=None):
        return False


class CertificateQueryUnitTests(unittest.TestCase):
    def _build_query_without_init(self):
        query = CertificateQuery.__new__(CertificateQuery)
        query._closed = False
        query._service_process = None
        return query

    def _png_bytes(self, color="red"):
        image = Image.new("RGB", (16, 16), color=color)
        buffer = BytesIO()
        image.save(buffer, format="PNG")
        return buffer.getvalue()

    def test_normalize_id_number_supports_whitespace_and_lowercase_x(self):
        normalized, error = CertificateQuery.normalize_id_number(" 13012620001028361x ")
        self.assertEqual(normalized, "13012620001028361X")
        self.assertIsNone(error)

    def test_normalize_id_number_rejects_invalid_checksum(self):
        normalized, error = CertificateQuery.normalize_id_number("130126200010283611")
        self.assertEqual(normalized, "130126200010283611")
        self.assertEqual(error, "身份证号校验位错误")

    def test_has_no_result_banner_detects_visible_message(self):
        query = self._build_query_without_init()
        query.driver = FakeDriver(
            no_result_elements=[FakeElement(text="没有查询到相关证件信息", displayed=True)]
        )
        self.assertTrue(query._has_no_result_banner())

    def test_extract_certificate_cards_returns_unique_cards_with_element_screenshot(self):
        row = FakeRow(
            headers=[NAME_FIELD, OPERATION_ITEM_FIELD, EFFECTIVE_END_FIELD],
            values=["李世龙", "低压电工作业", "2030-11-06"],
        )

        query = self._build_query_without_init()
        query.driver = FakeDriver(
            tables=[
                FakeTable([row], self._png_bytes("blue")),
                FakeTable([row], self._png_bytes("green")),
            ]
        )
        query._is_history_table = lambda _table: False
        query.capture_element_screenshot = lambda _table: b"fake-jpeg"

        cards = query.extract_certificate_cards()
        self.assertEqual(len(cards), 1)
        self.assertEqual(cards[0].fields[NAME_FIELD], "李世龙")
        self.assertIsNotNone(cards[0].screenshot_bytes)
        self.assertGreater(len(cards[0].screenshot_bytes), 0)

    def test_select_primary_certificates_prefers_latest_expire_date(self):
        earlier = ExtractedCertificateCard(
            fields={OPERATION_ITEM_FIELD: "低压电工作业", EFFECTIVE_END_FIELD: "2028-01-01"},
            screenshot_bytes=b"earlier",
        )
        later = ExtractedCertificateCard(
            fields={OPERATION_ITEM_FIELD: "低压电工作业", EFFECTIVE_END_FIELD: "2030-11-06"},
            screenshot_bytes=b"later",
        )

        selected = CertificateQuery.select_primary_certificates([earlier, later])
        self.assertEqual(selected["low_voltage"].screenshot_bytes, b"later")

    def test_classify_certificate_type_returns_expected_mapping(self):
        self.assertEqual(CertificateQuery.classify_certificate_type("高压电工作业"), "high_voltage")
        self.assertIsNone(CertificateQuery.classify_certificate_type("未知工种"))

    def test_is_certificate_valid_handles_valid_and_invalid_dates(self):
        self.assertTrue(CertificateQuery.is_certificate_valid("2030-11-06"))
        self.assertFalse(CertificateQuery.is_certificate_valid("2020-01-01"))
        self.assertIsNone(CertificateQuery.is_certificate_valid("not-a-date"))

    def test_close_calls_driver_quit_once(self):
        query = self._build_query_without_init()
        query.driver = Mock()
        query.close()
        query.close()
        query.driver.quit.assert_called_once()

    def test_get_ocr_falls_back_when_show_ad_is_unsupported(self):
        calls = []

        class FakeDdddOcr:
            def __init__(self, *args, **kwargs):
                calls.append(kwargs.copy())
                if "show_ad" in kwargs:
                    raise TypeError("DdddOcr.__init__() got an unexpected keyword argument 'show_ad'")

        query = self._build_query_without_init()
        query._ocr = None

        with patch.dict("sys.modules", {"ddddocr": SimpleNamespace(DdddOcr=FakeDdddOcr)}):
            ocr = query._get_ocr()

        self.assertIsInstance(ocr, FakeDdddOcr)
        self.assertEqual(calls, [{"show_ad": False}, {}])


if __name__ == "__main__":
    unittest.main()
