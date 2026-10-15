import unittest

from relayledger.retry import RetryPolicy, retry_after_ms
from relayledger.signing import sign, verify
from relayledger.validation import RelayError
from helpers import NOW

SECRET = b'local-test-key-32-bytes-or-longer!'


class DeliveryPolicyTests(unittest.TestCase):
    def test_backoff_growth_cap_and_jitter(self):
        policy = RetryPolicy(1000,4000)
        self.assertEqual([policy.delay_ms(i,random_value=1) for i in (1,2,3,1000)],[1000,2000,4000,4000])
        self.assertEqual(policy.delay_ms(1,random_value=0),500)
        for value in (-1,True,float('nan')):
            with self.assertRaises(RelayError): policy.delay_ms(1,random_value=value)

    def test_retry_after_seconds_date_and_past(self):
        self.assertEqual(retry_after_ms('30',now_ms=NOW),30000)
        self.assertEqual(retry_after_ms('Tue, 14 Nov 2023 22:13:30 GMT',now_ms=NOW),10000)
        self.assertEqual(retry_after_ms('Tue, 14 Nov 2023 22:13:00 GMT',now_ms=NOW),0)
        self.assertIsNone(retry_after_ms('invalid',now_ms=NOW))
        self.assertIsNone(retry_after_ms('-10',now_ms=NOW))
        with self.assertRaises(RelayError): retry_after_ms('86401',now_ms=NOW)

    def test_signature_exact_bytes_and_freshness(self):
        body = b'{"a":1}'
        header = sign(body,SECRET,now_ms=NOW)
        self.assertTrue(verify(body,header,SECRET,now_ms=NOW+300_000))
        self.assertFalse(verify(body,header,SECRET,now_ms=NOW+301_000))
        self.assertFalse(verify(body+b' ',header,SECRET,now_ms=NOW))
        self.assertFalse(verify(body,header+b'bad'.decode(),SECRET,now_ms=NOW))
        self.assertFalse(verify(body,header,SECRET+b'wrong',now_ms=NOW))

    def test_ambiguous_headers_and_weak_secrets(self):
        for header in ('t=1,v1=abc','t=1,t=2,v1='+'a'*64,'bad','t=NaN,v1='+'a'*64):
            self.assertFalse(verify(b'{}',header,SECRET,now_ms=NOW))
        with self.assertRaises(RelayError): sign(b'{}',b'weak',now_ms=NOW)
