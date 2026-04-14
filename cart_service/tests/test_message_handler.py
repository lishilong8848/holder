import json
import unittest

from app.message_handler import extract_record_id


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


if __name__ == "__main__":
    unittest.main()
