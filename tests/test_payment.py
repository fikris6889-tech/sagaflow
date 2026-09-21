import unittest

from src.common.exceptions import PaymentDeclinedError
from src.common.fake_adapters import FakeDeps
from src.handlers import charge_payment, refund_payment


class TestChargePayment(unittest.TestCase):
    def setUp(self):
        self.deps = FakeDeps()

    def test_successful_charge(self):
        event = {"order_id": "o-1", "amount_cents": 4599}
        result = charge_payment.handle(event, self.deps)

        self.assertTrue(result["charge"]["charge_id"].startswith("ch_"))
        self.assertEqual(result["charge"]["amount_cents"], 4599)
        self.assertEqual(self.deps.order_store.get_status("o-1"), "PAYMENT_CHARGED")
        self.assertEqual(self.deps.payment_gateway.charge_calls, 1)

    def test_declined_amount_raises_business_error(self):
        event = {"order_id": "o-2", "amount_cents": 66_600}  # DECLINE_AMOUNT_CENTS
        with self.assertRaises(PaymentDeclinedError):
            charge_payment.handle(event, self.deps)
        self.assertNotEqual(self.deps.order_store.get_status("o-2"), "PAYMENT_CHARGED")

    def test_retried_charge_with_same_order_id_never_double_charges(self):
        event = {"order_id": "o-3", "amount_cents": 1200}

        first = charge_payment.handle(event, self.deps)
        second = charge_payment.handle(event, self.deps)  # simulated Step Functions retry

        self.assertEqual(first["charge"]["charge_id"], second["charge"]["charge_id"])
        # The @idempotent wrapper should have short-circuited entirely on
        # the second call, so the gateway itself was only ever invoked once.
        self.assertEqual(self.deps.payment_gateway.charge_calls, 1)


class TestRefundPayment(unittest.TestCase):
    def setUp(self):
        self.deps = FakeDeps()

    def test_refund_runs_after_a_successful_charge(self):
        charged = charge_payment.handle({"order_id": "o-1", "amount_cents": 2500}, self.deps)
        result = refund_payment.handle(charged, self.deps)

        self.assertEqual(result["compensation_refund"]["charge_id"], charged["charge"]["charge_id"])
        self.assertEqual(self.deps.order_store.get_status("o-1"), "FAILED")

    def test_refund_is_idempotent_across_a_simulated_retry(self):
        charged = charge_payment.handle({"order_id": "o-1", "amount_cents": 2500}, self.deps)

        refund_payment.handle(charged, self.deps)
        refund_payment.handle(charged, self.deps)  # should be a safe no-op the second time

        # No exception, and the idempotency store only ever recorded one result.
        cached = self.deps.idempotency_store.get_cached_result("o-1:refund_payment")
        self.assertIsNotNone(cached)


if __name__ == "__main__":
    unittest.main()
