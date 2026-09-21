"""
Saga step 4 (forward action, terminal): mark the order COMPLETED.

The state machine's FinalizeOrder Task has `"End": true` and no Catch --
see create_shipment.py's docstring for why a failure this late is
treated as an operational concern rather than something to compensate.
"""

from __future__ import annotations

from src.common.idempotency import idempotent


@idempotent("finalize_order")
def handle(event: dict, deps) -> dict:
    order_id = event["order_id"]
    deps.order_store.set_status(order_id, "COMPLETED")
    return {**event, "status": "COMPLETED"}


_deps = None


def lambda_handler(event, context):  # pragma: no cover - exercised only inside AWS Lambda
    global _deps
    if _deps is None:
        from src.common.dynamo_adapters import RealDeps

        _deps = RealDeps()
    return handle(event, _deps)
