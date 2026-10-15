import unittest

from relayledger.demo import run_demo


class DemoTests(unittest.TestCase):
    def test_real_http_duplicate_is_committed_once(self):
        result = run_demo()
        self.assertTrue(result['verified'])
        self.assertEqual(result['http_deliveries'],3)
        self.assertEqual(result['business_effects'],2)
        self.assertEqual(result['duplicate_deliveries'],1)
