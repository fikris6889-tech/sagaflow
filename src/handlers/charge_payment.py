"""
Saga step 2 (forward action): charge the customer for the order.

Only runs after ReserveInventory has already succeeded (see the ASL's
`Next` chain), so a PaymentDeclinedError here means inventory *has*
already been decremented and must be put back -- which is exactly why
this state's Catch routes to CompensateReleaseInventoryOnly rather than
straight to a Fail state, unlike ReserveInventory's own Catch.

The idempotency key passed to the payment gateway is derived from
`order_id` alone (not order_id + step name, unlike the IdempotencyStore
key below) because a real payment processor's idempotency key must
survive even if this Lambda is cold-started fresh with no memory of a
prior attempt -- it is the processor, not this code, that is the
ultimate source of truth for "has this order already been charged."
The @idempotent wrapper's own cache is a *second*, independent layer on
top of that (skips even calling the gateway on a pure retry), not a
replacement for it.
"""

from __future__ import annotations

from src.common.idempotency import idempotent


@idempotent("charge_payment")
def handle(event: dict, deps) -> dict:
    order_id = event["order_id"]
    amount_cents = event["amount_cents"]

    result = deps.payment_gateway.charge(
        order_id=order_id,
        amount_cents=amount_cents,
        idempotency_key=f"charge:{order_id}",
    )
    deps.order_store.set_status(order_id, "PAYMENT_CHARGED")

    return {
        **event,
        "charge": {"charge_id": result.charge_id, "amount_cents": result.amount_cents},
    }


_deps = None


def lambda_handler(event, context):  # pragma: no cover - exercised only inside AWS Lambda
    global _deps
    if _deps is None:
        from src.common.dynamo_adapters import RealDeps

        _deps = RealDeps()
    return handle(event, _deps)
