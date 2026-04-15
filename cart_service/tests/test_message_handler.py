import json
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import app.message_handler as message_handler
from app.message_handler import (
    EFFECTIVE_END_FIELD,
    REVIEW_ACTUAL_FIELD,
    RecordProcessingContext,
    build_processing_summary_text,
    extract_record_id,
    handle_message_async,
    send_processing_summary_to_chat,
    split_summary_message,
)


class ExtractRecordIdTests(unittest.TestCase):
    def test_extracts_record_id_from_visible_card_text(self):
        msg_data = {
            "chat_name": "南通运维全景-施工管理",
            "sender_type": "bot",
            "display_text": "\n".join(
                [
                    "南通运维全景-施工管理",
                    "机器人",
                    "来自飞书多维表格",
                    "特种作业查证",
                    "记录ID：recvgJe0RmBmIM",
                    "来自 南通运维全景平台-施工管理",
                ]
            ),
        }

        self.assertEqual(extract_record_id(msg_data), "recvgJe0RmBmIM")

    def test_extracts_record_id_from_nested_interactive_card_payload(self):
        msg_data = {
            "msg_type": "interactive",
            "content_raw": json.dumps(
                {
                    "type": "template",
                    "data": {
                        "template_id": "AAqwerty",
                        "template_variable": {
                            "title": "特种作业查证",
                            "record_info": {
                                "label": "记录ID",
                                "value": "recvgJe0RmBmIM",
                            },
                        },
                    },
                },
                ensure_ascii=False,
            ),
        }

        self.assertEqual(extract_record_id(msg_data), "recvgJe0RmBmIM")

    def test_extracts_record_id_from_stringified_dict_payload(self):
        msg_data = {
            "content": "{'text': '特种作业查证', 'record_id': 'recvgJe0RmBmIM'}",
        }

        self.assertEqual(extract_record_id(msg_data), "recvgJe0RmBmIM")

    def test_returns_none_for_degraded_interactive_card_without_record_id(self):
        msg_data = {
            "msg_type": "interactive",
            "content": {
                "title": "特种作业查证",
                "elements": [
                    [
                        {"tag": "img", "image_key": "img_v3_xxx"},
                        {"tag": "text", "text": "请升级至最新版本客户端，以查看内容"},
                    ]
                ],
            },
            "content_raw": json.dumps(
                {
                    "title": "特种作业查证",
                    "elements": [
                        [
                            {"tag": "img", "image_key": "img_v3_xxx"},
                            {"tag": "text", "text": "请升级至最新版本客户端，以查看内容"},
                        ]
                    ],
                },
                ensure_ascii=False,
            ),
        }

        self.assertIsNone(extract_record_id(msg_data))

    def test_extracts_record_id_from_degraded_card_title(self):
        msg_data = {
            "msg_type": "interactive",
            "content": {
                "title": "特种作业查证 记录ID：recvgJe0RmBmIM",
                "elements": [
                    [
                        {"tag": "img", "image_key": "img_v3_xxx"},
                        {"tag": "text", "text": "请升级至最新版本客户端，以查看内容"},
                    ]
                ],
            },
            "content_raw": json.dumps(
                {
                    "title": "特种作业查证 记录ID：recvgJe0RmBmIM",
                    "elements": [
                        [
                            {"tag": "img", "image_key": "img_v3_xxx"},
                            {"tag": "text", "text": "请升级至最新版本客户端，以查看内容"},
                        ]
                    ],
                },
                ensure_ascii=False,
            ),
        }

        self.assertEqual(extract_record_id(msg_data), "recvgJe0RmBmIM")


class ProcessingSummaryTests(unittest.TestCase):
    def build_context(self) -> RecordProcessingContext:
        return RecordProcessingContext(
            source_record_id="rec_summary_001",
            shigong_code="SG-001",
            target_table_id="tbl_target",
            download_dir=Path("output/records/rec_summary_001"),
            timestamp="20260415_101112",
            personnel_count=3,
            query_required_count=2,
            direct_write_count=1,
            expected_write_tasks=3,
            feishu_client=Mock(),
            service=Mock(),
            chat_id="oc_chat_001",
        )

    def test_build_processing_summary_text_covers_all_sections(self):
        context = self.build_context()
        success_result = SimpleNamespace(
            status="success",
            error=None,
            selected_certificates={
                "high_voltage": SimpleNamespace(
                    fields={
                        EFFECTIVE_END_FIELD: "2030-11-06",
                        REVIEW_ACTUAL_FIELD: "2026-03-01",
                    }
                ),
                "low_voltage": SimpleNamespace(
                    fields={
                        EFFECTIVE_END_FIELD: "2029-08-15",
                        REVIEW_ACTUAL_FIELD: "",
                    }
                ),
            },
        )
        failed_result = SimpleNamespace(
            status="fail_no_data",
            error="接口超时不要展示",
            selected_certificates={},
        )

        context.mark_write_completed(
            created=True,
            person={
                "name": "张三",
                "id_number": "320101199001010011",
                "phone": "13800000000",
                "job_type": "高压电工作业",
            },
            query_result=success_result,
        )
        context.mark_write_completed(
            created=False,
            person={
                "name": "李四",
                "id_number": "320101199001010022",
                "phone": "13900000000",
                "job_type": "低压电工作业",
            },
            query_result=failed_result,
        )
        context.mark_write_completed(
            created=False,
            person={
                "name": "王五",
                "id_number": "320101199001010033",
                "phone": "13700000000",
                "job_type": "",
            },
            query_result=None,
        )

        text = build_processing_summary_text(context)

        self.assertIn("特种作业查证结果汇总", text)
        self.assertIn("施工编码：SG-001", text)
        self.assertNotIn("记录ID", text)
        self.assertNotIn("rec_summary_001", text)
        self.assertIn("统计汇总", text)
        self.assertIn("总人数 ：3", text)
        self.assertIn("需查证件 ：2", text)
        self.assertIn("查询成功 ：1", text)
        self.assertIn("查询失败 ：1", text)
        self.assertIn("未查询直回填：1", text)
        self.assertIn("回填成功 ：1", text)
        self.assertIn("回填失败 ：2", text)
        self.assertNotIn("| 序号 | 姓名 | 作业类型 | 证件信息 |", text)
        self.assertNotIn("查询成功人员", text)
        self.assertNotIn("查询失败人员", text)
        self.assertNotIn("未查询证件", text)
        self.assertNotIn("张三", text)
        self.assertNotIn("李四", text)
        self.assertNotIn("王五", text)
        self.assertNotIn("高压证", text)
        self.assertNotIn("接口超时不要展示", text)
        self.assertNotIn("320101199001010011", text)
        self.assertNotIn("13800000000", text)
        self.assertNotIn("本机附件目录", text)

    def test_split_summary_message_adds_part_numbers(self):
        message = "特种作业查证结果汇总\n" + "\n".join(
            f"{index}. 张三｜作业类型：高压电工作业｜回填：成功"
            for index in range(1, 15)
        )

        chunks = split_summary_message(message, max_chars=120)

        self.assertGreater(len(chunks), 1)
        for index, chunk in enumerate(chunks, start=1):
            self.assertTrue(chunk.startswith(f"特种作业查证结果汇总（第 {index}/{len(chunks)} 段）"))

    def test_send_processing_summary_to_chat_uses_original_chat_id(self):
        context = self.build_context()
        context.expected_write_tasks = 1
        context.personnel_count = 1
        context.query_required_count = 0
        context.direct_write_count = 1
        context.mark_write_completed(
            created=True,
            person={"name": "王五", "job_type": "一般作业"},
            query_result=None,
        )
        context.feishu_client.send_text_message = Mock(return_value=True)

        send_processing_summary_to_chat(context)

        context.feishu_client.send_text_message.assert_called_once()
        args, kwargs = context.feishu_client.send_text_message.call_args
        self.assertEqual(args[0], "oc_chat_001")
        self.assertIn("特种作业查证结果汇总", args[1])
        self.assertEqual(kwargs["receive_id_type"], "chat_id")

    def test_handle_message_async_passes_chat_id_to_processor(self):
        message_handler._processing_ids.clear()
        message_handler._recent_processed_records.clear()

        class ImmediateThread:
            def __init__(self, *, target, name, daemon):
                self.target = target
                self.name = name
                self.daemon = daemon

            def start(self):
                self.target()

        msg_data = {
            "msg_type": "text",
            "display_text": "特种作业查证 记录ID：recvgJe0RmBmIM",
            "chat_id": "oc_chat_001",
        }

        with patch("app.message_handler.threading.Thread", ImmediateThread), patch(
            "app.message_handler.process_record_message"
        ) as process_record_message:
            handle_message_async(msg_data, Mock())

        process_record_message.assert_called_once()
        _args, kwargs = process_record_message.call_args
        self.assertEqual(kwargs["chat_id"], "oc_chat_001")
        message_handler._processing_ids.clear()
        message_handler._recent_processed_records.clear()

    def test_handle_message_async_skips_recent_duplicate_record_id(self):
        message_handler._processing_ids.clear()
        message_handler._recent_processed_records.clear()

        class ImmediateThread:
            def __init__(self, *, target, name, daemon):
                self.target = target
                self.name = name
                self.daemon = daemon

            def start(self):
                self.target()

        msg_data = {
            "msg_type": "text",
            "display_text": "特种作业查证 记录ID：recvgJe0RmBmIM",
            "chat_id": "oc_chat_001",
        }

        with patch("app.message_handler.threading.Thread", ImmediateThread), patch(
            "app.message_handler.process_record_message"
        ) as process_record_message:
            handle_message_async(msg_data, Mock())
            handle_message_async(msg_data, Mock())

        process_record_message.assert_called_once()
        self.assertIn("recvgJe0RmBmIM", message_handler._recent_processed_records)
        message_handler._processing_ids.clear()
        message_handler._recent_processed_records.clear()


if __name__ == "__main__":
    unittest.main()
