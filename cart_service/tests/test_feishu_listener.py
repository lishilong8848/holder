import unittest

from app.feishu_listener import _build_message_list_request


class FeishuListenerRequestTests(unittest.TestCase):
    def test_build_message_list_request_defaults_to_latest_desc(self):
        req = _build_message_list_request("oc_test_chat", page_size=1)

        self.assertEqual(req.container_id_type, "chat")
        self.assertEqual(req.container_id, "oc_test_chat")
        self.assertEqual(req.page_size, 1)
        self.assertEqual(req.sort_type, "ByCreateTimeDesc")

    def test_build_message_list_request_keeps_optional_page_token(self):
        req = _build_message_list_request("oc_test_chat", page_size=50, page_token="token_123")

        self.assertEqual(req.page_token, "token_123")
        self.assertEqual(req.page_size, 50)
        self.assertEqual(req.sort_type, "ByCreateTimeDesc")


if __name__ == "__main__":
    unittest.main()
