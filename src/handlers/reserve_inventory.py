"""
Saga step 1 (forward action): reserve `qty` units of `sku` for an order.

Classification for statemachine/order_saga.asl.json:
* Raises OptimisticLockConflict / ThrottledError (both TransientError) ->
  caught by this state's Retry block, no compensation involved.
* Raises InsufficientInventoryError (BusinessError) -> caught by this
  state's Catch block, routed straight to OrderFailedNoCompensation.
  Nothing has succeeded yet at this point in the saga, so there is
  genuinely nothing to compensate -- which is also why this is the only
  forward step that writes the order's *first* OrderStore record only on
  success: a doomed order that never got past this check is never
  persisted at all.

Idempotency: wrapped with @idempotent so a Step Functions retry of this
exact Task (e.g. after a network timeout on the response, even though
the DynamoDB write itself committed) replays the cached result instead
of decrementing the same SKU a second time for the same order.
"""

from __future__ import annotations

from src.common.idempotency import idempotent


@idempotent("reserve_inventory")
def handle(event: dict, deps) -> dict:
    order_id = event["order_id"]
    sku = event["sku"]
    qty = event["qty"]

    result = deps.inventory_store.reserve(sku, qty)
    deps.order_store.set_status(order_id, "INVENTORY_RESERVED")

    return {
        **event,
        "reservation": {
            "sku": result.sku,
            "qty_reserved": result.qty_reserved,
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
