import json
import ssl
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

from app.photo_ai_recognition import build_image_prompt, build_summary_prompt, _post_chat_completion


class FakeHttpResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def read(self):
        return json.dumps(self.payload, ensure_ascii=False).encode("utf-8")


class PhotoAiRecognitionPromptTests(unittest.TestCase):
    def test_image_prompt_only_uses_provided_requirement_text(self):
        prompt = build_image_prompt(
            Path("现场.jpg"),
            phase="过程",
            job_type="高处作业",
            process_requirement=(
                "1、随工人员需全程监督施工安全与质量。\n"
                "2、施工人员、随工人员不得擅自离开作业现场。\n"
                "3、如涉及特殊作业时需按特殊作业要求进行管控。"
            ),
        )

        self.assertIn("只根据下方规范文本", prompt)
        self.assertIn("不得补充、扩展或引用未出现在上述规范中的其他作业类型要求", prompt)
        self.assertIn("随工人员需全程监督施工安全与质量", prompt)
        self.assertNotIn("绝缘手套", prompt)
        self.assertNotIn("动火作业", prompt)
        self.assertNotIn("吊装作业", prompt)

    def test_summary_prompt_does_not_generalize_to_other_work_types(self):
        prompt = build_summary_prompt(
            [("现场.jpg", "按规范已确认：随工人员在现场。")],
            phase="过程",
            job_type="高处作业",
            process_requirement="随工人员需全程监督施工安全与质量。",
        )

        self.assertIn("只回答当前作业类型匹配到的规范内容", prompt)
        self.assertIn("不要补充其它特种作业类型的要求", prompt)
        self.assertNotIn("绝缘手套", prompt)
        self.assertNotIn("动火作业", prompt)

    def test_post_chat_completion_retries_ssl_eof_once_then_succeeds(self):
        response_payload = {"choices": [{"message": {"content": "成功"}}]}
        ssl_error = urllib.error.URLError(
            ssl.SSLError("[SSL: UNEXPECTED_EOF_WHILE_READING] EOF occurred in violation of protocol")
        )

        with patch.dict("os.environ", {"QWEN_API_RETRY_ATTEMPTS": "2", "QWEN_API_RETRY_BASE_DELAY_SECONDS": "0"}), patch(
            "app.photo_ai_recognition.urllib.request.urlopen",
            side_effect=[ssl_error, FakeHttpResponse(response_payload)],
        ) as urlopen, patch("app.photo_ai_recognition.time.sleep") as sleep:
            payload = _post_chat_completion(
                api_key="sk-test",
                base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
                payload={"model": "qwen-test", "messages": []},
                timeout=1,
            )

        self.assertEqual(payload, response_payload)
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
