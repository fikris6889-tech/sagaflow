"""
Compensating action for reserve_inventory.

Invoked from two places in statemachine/order_saga.asl.json:
* CompensateReleaseInventoryOnly -- payment was declined, nothing else
  succeeded yet, so only inventory needs to be put back.
* The "ReleaseInventory2" branch of the CompensateRefundAndRelease
  Parallel state -- shipment creation failed *after* payment succeeded,
  so this runs alongside refund_payment rather than after it, since the
  two compensations are independent of each other and there is no
  correctness reason to serialize them.

Releasing stock has no business rule that can fail (you can always add
units back), so this step has no BusinessError / Catch branch in the ASL
at all -- only the standard TransientError Retry block shared by every
Task. Still wrapped with @idempotent: Step Functions can retry a Task
inside a Parallel branch independently of its sibling branch, so the
same replay-safety argument as every other step applies here too.
"""

from __future__ import annotations

from src.common.idempotency import idempotent


@idempotent("release_inventory")
def handle(event: dict, deps) -> dict:
    order_id = event["order_id"]
    reservation = event["reservation"]

    result = deps.inventory_store.release(reservation["sku"], reservation["qty_reserved"])
    deps.order_store.set_status(order_id, "FAILED")

    return {
        **event,
        "compensation_release": {
            "sku": result.sku,
            "remaining_qty": result.remaining_qty,
        },
    }


_deps = None


def lambda_handler(event, context):  # pragma: no cover - exercised only inside AWS Lambda
    global _deps
    if _deps is None:
        from src.common.dynamo_adapters import RealDeps

        _deps = RealDeps()
    return handle(event, _deps)
