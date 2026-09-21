"""
End-to-end tests that load the REAL statemachine/order_saga.asl.json and
execute it via tools/asl_interpreter.py, with the ASL's `Resource`
strings wired to the real src/handlers/*.py `handle()` functions backed
by fake, in-memory adapters (tests/helpers.py).

This is the test suite's highest-value layer: every test below is
proof that the specific JSON file that would be handed to
`aws stepfunctions create-state-machine` actually implements the saga
correctly -- not a hand-written Python re-implementation of "what the
saga should do" that could drift from the real definition over time.
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from src.common.fake_adapters import FakeDeps
from tests.helpers import build_resources
from tools.asl_interpreter import run_state_machine

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFINITION = json.loads((REPO_ROOT / "statemachine" / "order_saga.asl.json").read_text())


def _base_order(**overrides) -> dict:
    order = {
        "order_id": "order-1",
        "sku": "WIDGET-1",
        "qty": 2,
        "amount_cents": 4999,
        "address": "42 Wallaby Way, Sydney",
    }
    order.update(overrides)
    return order


class TestHappyPath(unittest.TestCase):
    def test_full_saga_succeeds_and_commits_every_step(self):
        deps = FakeDeps(initial_stock={"WIDGET-1": 10})
        result = run_state_machine(DEFINITION, _base_order(), build_resources(deps))

        self.assertEqual(result.status, "SUCCEEDED")
        self.assertEqual(result.output["status"], "COMPLETED")
        self.assertEqual(deps.inventory_store.quantity_of("WIDGET-1"), 8)
        self.assertEqual(deps.order_store.get_status("order-1"), "COMPLETED")
        self.assertEqual(
            result.trace,
            ["ReserveInventory", "ChargePayment", "CreateShipment", "FinalizeOrder"],
        )


class TestInsufficientInventoryPath(unittest.TestCase):
    def test_out_of_stock_fails_with_no_compensation_and_no_side_effects(self):
        deps = FakeDeps(initial_stock={"WIDGET-1": 1})  # order asks for 2
        result = run_state_machine(DEFINITION, _base_order(qty=2), build_resources(deps))

        self.assertEqual(result.status, "FAILED")
        self.assertEqual(result.fail_error, "OrderFailed")
        self.assertEqual(result.trace, ["ReserveInventory", "OrderFailedNoCompensation"])
        # Nothing should have been charged, and stock must be untouched.
        self.assertEqual(deps.inventory_store.quantity_of("WIDGET-1"), 1)
        self.assertEqual(deps.payment_gateway.charge_calls, 0)
        # No order record either -- see reserve_inventory.py's docstring.
        self.assertIsNone(deps.order_store.get_status("order-1"))


class TestPaymentDeclinedPath(unittest.TestCase):
    def test_declined_payment_compensates_by_releasing_inventory(self):
        deps = FakeDeps(initial_stock={"WIDGET-1": 10})
        # DECLINE_AMOUNT_CENTS = 66_600 triggers the fake gateway's decline rule.
        result = run_state_machine(
            DEFINITION, _base_order(amount_cents=66_600), build_resources(deps)
        )

        self.assertEqual(result.status, "FAILED")
        self.assertEqual(result.fail_error, "OrderFailedCompensated")
        self.assertEqual(
            result.trace,
            ["ReserveInventory", "ChargePayment", "CompensateReleaseInventoryOnly", "OrderFailedAfterCompensation"],
        )
        # Inventory must be back to its original level -- the reservation
        # from ReserveInventory was fully undone.
        self.assertEqual(deps.inventory_store.quantity_of("WIDGET-1"), 10)
        self.assertEqual(deps.order_store.get_status("order-1"), "FAILED")


class TestShipmentFailurePath(unittest.TestCase):
    def test_shipment_failure_compensates_both_payment_and_inventory(self):
        deps = FakeDeps(initial_stock={"WIDGET-1": 10})
        result = run_state_machine(
            DEFINITION, _base_order(address="UNSHIPPABLE OUTPOST"), build_resources(deps)
        )

        self.assertEqual(result.status, "FAILED")
        self.assertEqual(result.fail_error, "OrderFailedCompensated")
        self.assertIn("CompensateRefundAndRelease", result.trace)

        # Both compensations must have actually run: inventory restored...
        self.assertEqual(deps.inventory_store.quantity_of("WIDGET-1"), 10)
        # ...and the charge refunded (recorded in the idempotency store
        # under its refund namespace).
        refund_record = deps.idempotency_store.get_cached_result("order-1:refund_payment")
        self.assertIsNotNone(refund_record)
        self.assertEqual(deps.order_store.get_status("order-1"), "FAILED")

    def test_shipment_failure_never_leaves_inventory_permanently_reserved(self):
        # A regression guard for the single most damaging possible bug in
        # this whole project: money refunded but stock never restored (or
        # vice versa) because only one branch of the Parallel compensation
        # actually ran. Checked independently of the test above.
        deps = FakeDeps(initial_stock={"WIDGET-1": 3})
        run_state_machine(DEFINITION, _base_order(qty=3, address="UNSHIPPABLE"), build_resources(deps))
        self.assertEqual(deps.inventory_store.quantity_of("WIDGET-1"), 3)


class TestRetryBudget(unittest.TestCase):
    def test_transient_errors_are_retried_before_giving_up(self):
        """A resource that fails with a transient error the first two
        times and succeeds the third time should still complete the saga
        -- proving the Retry block (not just the Catch block) actually
        works when driven through the real interpreter."""
        from tools.asl_interpreter import StatesError

        deps = FakeDeps(initial_stock={"WIDGET-1": 10})
        resources = build_resources(deps)
        real_reserve = resources["reserve_inventory"]
        attempts = {"n": 0}

        def flaky_reserve(data):
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise StatesError("OptimisticLockConflict", "simulated contention")
            return real_reserve(data)

        resources["reserve_inventory"] = flaky_reserve

        result = run_state_machine(DEFINITION, _base_order(), resources)

        self.assertEqual(result.status, "SUCCEEDED")
        self.assertEqual(attempts["n"], 3)

    def test_transient_error_that_exhausts_its_retry_budget_still_fails_safely(self):
        """More attempts than MaxAttempts allows for ReserveInventory (3)
        must fall through to the States.ALL safety-net Catch instead of
        raising an unhandled exception out of the whole execution."""
        from tools.asl_interpreter import StatesError

        deps = FakeDeps(initial_stock={"WIDGET-1": 10})
        resources = build_resources(deps)

        def always_flaky(data):
            raise StatesError("OptimisticLockConflict", "permanently contended")

        resources["reserve_inventory"] = always_flaky

        result = run_state_machine(DEFINITION, _base_order(), resources)

        self.assertEqual(result.status, "FAILED")
        self.assertEqual(result.fail_error, "OrderFailed")
        self.assertEqual(result.trace, ["ReserveInventory", "OrderFailedNoCompensation"])


class TestIdempotentRetryOfAnAlreadySucceededStep(unittest.TestCase):
    def test_re_running_the_whole_saga_for_the_same_order_id_has_no_extra_side_effects(self):
        """Simulates Step Functions re-invoking the state machine's
        initial Task twice for the same order (e.g. a client-side retry
        after a network timeout on the StartExecution call, using the
        same order_id as the idempotency key) by simply running the same
        input through the interpreter twice with the SAME deps."""
        deps = FakeDeps(initial_stock={"WIDGET-1": 10})
        resources = build_resources(deps)

        first = run_state_machine(DEFINITION, _base_order(), resources)
        second = run_state_machine(DEFINITION, _base_order(), resources)

        self.assertEqual(first.status, "SUCCEEDED")
        self.assertEqual(second.status, "SUCCEEDED")
        # Only ONE reservation's worth of stock should be gone, not two.
        self.assertEqual(deps.inventory_store.quantity_of("WIDGET-1"), 8)
        self.assertEqual(deps.payment_gateway.charge_calls, 1)


if __name__ == "__main__":
    unittest.main()
