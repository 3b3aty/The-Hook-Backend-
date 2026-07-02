import unittest

from main import _build_reply_headers


class ReplyHeaderTests(unittest.TestCase):
    def test_build_reply_headers_uses_original_message_id_and_existing_chain(self):
        headers = _build_reply_headers(
            original_message_id="<msg-1>",
            existing_references="<msg-0> <msg-1>",
        )
        self.assertEqual(headers["In-Reply-To"], "<msg-1>")
        self.assertEqual(headers["References"], "<msg-0> <msg-1>")

    def test_build_reply_headers_handles_missing_original_message_id(self):
        headers = _build_reply_headers(original_message_id=None, existing_references="<msg-0>")
        self.assertIsNone(headers["In-Reply-To"])
        self.assertEqual(headers["References"], "<msg-0>")


if __name__ == "__main__":
    unittest.main()
