import tempfile
import unittest
from pathlib import Path

import openpyxl

from app.message_handler import parse_excel_for_personnel, should_query_certificate


class MessageHandlerExcelTests(unittest.TestCase):
    def build_excel(self, headers, rows) -> str:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)

        file_path = Path(temp_dir.name) / "personnel.xlsx"
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(headers)
        for row in rows:
            ws.append(row)
        wb.save(file_path)
        wb.close()
        return str(file_path)

    def test_should_query_certificate_returns_false_for_general_work(self):
        self.assertFalse(should_query_certificate("一般作业"))

    def test_should_query_certificate_returns_true_for_special_work(self):
        self.assertTrue(should_query_certificate("高压电工作业"))

    def test_should_query_certificate_returns_true_for_mixed_job_type(self):
        self.assertTrue(should_query_certificate("一般作业/高处安装、维护、拆除作业"))

    def test_should_query_certificate_ignores_surrounding_whitespace(self):
        self.assertFalse(should_query_certificate("  一般作业  "))

    def test_parse_excel_treats_blank_job_type_as_no_query(self):
        excel_path = self.build_excel(
            headers=["姓名", "身份证号", "作业类型", "是否有特殊作业权限"],
            rows=[["张三", "320101199001010011", "", "是"]],
        )

        personnel = parse_excel_for_personnel(excel_path)

        self.assertEqual(len(personnel), 1)
        self.assertEqual(personnel[0]["job_type"], "")
        self.assertFalse(personnel[0]["has_permission"])

    def test_parse_excel_treats_missing_job_type_column_as_no_query(self):
        excel_path = self.build_excel(
            headers=["姓名", "身份证号", "是否有特殊作业权限"],
            rows=[["李四", "320101199001010022", "是"]],
        )

        personnel = parse_excel_for_personnel(excel_path)

        self.assertEqual(len(personnel), 1)
        self.assertEqual(personnel[0]["job_type"], "")
        self.assertFalse(personnel[0]["has_permission"])


if __name__ == "__main__":
    unittest.main()
