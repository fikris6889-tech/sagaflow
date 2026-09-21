"""
Compensating action for charge_payment.

Only reachable from the CompensateRefundAndRelease Parallel state, i.e.
only when shipment creation failed *after* a successful charge. Runs
alongside release_inventory in the other Parallel branch -- refunding
money and restocking inventory are independent operations, so there is
no reason to force one to wait on the other, and doing so would only
slow down compensation without buying any correctness.

Refunds are naturally idempotent on every real payment processor
(refunding an already-refunded charge is defined as a no-op, not an
error), so the idempotency key here mostly guards against redundant
network calls on a Step Functions retry rather than against a real
double-refund -- the @idempotent wrapper's cache still short-circuits
that case cleanly regardless.
"""

from __future__ import annotations

from src.common.idempotency import idempotent


@idempotent("refund_payment")
def handle(event: dict, deps) -> dict:
    order_id = event["order_id"]
    charge = event["charge"]

    deps.payment_gateway.refund(charge["charge_id"], idempotency_key=f"charge:{order_id}")
    deps.order_store.set_status(order_id, "FAILED")

    return {**event, "compensation_refund": {"charge_id": charge["charge_id"]}}


_deps = None


def lambda_handler(event, context):  # pragma: no cover - exercised only inside AWS Lambda
    global _deps
    if _deps is None:
        from src.common.dynamo_adapters import RealDeps

        _deps = RealDeps()
    return handle(event, _deps)
