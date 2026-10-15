import unittest

from relayledger.validation import RelayError, canonical, instant, label, timestamp


class ValidationTests(unittest.TestCase):
    def test_canonical_content(self):
        self.assertEqual(
            canonical({"b": 2, "a": "café"}), canonical({"a": "café", "b": 2})
        )
        self.assertEqual(timestamp(1_700_000_000_000), "2023-11-14T22:13:20.000Z")

    def test_ambiguous_and_invalid_payloads(self):
        for value in (
            {1: "integer key"},
            {"x": float("nan")},
            {"x": set()},
            {"x": "\ud800"},
            {"x": "a" * 1_048_577},
        ):
            with self.subTest(type=type(value)), self.assertRaises(RelayError):
                canonical(value)

    def test_depth_and_item_bounds(self):
        value = {}
        for _ in range(34):
            value = {"nested": value}
        for data in (value, list(range(10_001))):
            with self.assertRaises(RelayError):
                canonical(data)

    def test_identifier_and_clock_validation(self):
        for value in ("", "bad\r\nheader", "<script>", "x" * 129):
            with self.assertRaises(RelayError):
                label(value)
        for value in (True, -1, float("inf"), 4_102_444_800_001):
            with self.assertRaises(RelayError):
                instant(value)
