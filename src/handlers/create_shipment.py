"""
Saga step 3 (forward action): create the shipment with the carrier.

The last step that can trigger compensation. A ShipmentError here means
*both* inventory reservation and payment have already succeeded, so this
state's Catch routes to CompensateRefundAndRelease -- a Parallel state
that undoes both of them at once (see refund_payment.py and
release_inventory.py for why those two run concurrently rather than
sequentially).

FinalizeOrder, the step after this one, deliberately has no Catch branch
of its own in the ASL: by the time shipment creation has succeeded, the
saga has committed to fulfilling the order in the real world (the
carrier already has it), so a failure updating our own status record
afterwards is treated as an operational issue for a human to fix, not a
business failure to compensate -- unwinding a shipment that has already
physically left the warehouse is not something this system can do.
This tradeoff is called out explicitly rather than silently modeled away.
"""

from __future__ import annotations

from src.common.idempotency import idempotent


@idempotent("create_shipment")
def handle(event: dict, deps) -> dict:
    order_id = event["order_id"]
    address = event["address"]

    result = deps.shipment_provider.create_shipment(order_id=order_id, address=address)
    deps.order_store.set_status(order_id, "SHIPMENT_CREATED")

    return {**event, "shipment": {"shipment_id": result.shipment_id}}


_deps = None


def lambda_handler(event, context):  # pragma: no cover - exercised only inside AWS Lambda
    global _deps
    if _deps is None:
        from src.common.dynamo_adapters import RealDeps

        _deps = RealDeps()
    return handle(event, _deps)
