"""
Shared test helper: wires the real src/handlers/*.py `handle()` functions
into tools/asl_interpreter.py's `resources` registry, translating this
project's SagaError subclasses into asl_interpreter.StatesError so the
interpreter's generic Retry/Catch matching (which only knows about error
*names*, exactly like real Step Functions) can route on them.
"""

from __future__ import annotations

from src.common.exceptions import SagaError
from src.handlers import (
    charge_payment,
    create_shipment,
    finalize_order,
    refund_payment,
    release_inventory,
    reserve_inventory,
)
from tools.asl_interpreter import StatesError


def _wrap(handle_fn, deps):
    def wrapped(data: dict) -> dict:
        try:
            return handle_fn(data, deps)
        except SagaError as exc:
            # Mirrors how a real AWS Lambda integration reports an
            # unhandled exception to Step Functions: the exception
            # class's own name becomes the `errorType` that Retry/Catch
            # ErrorEquals match against.
            raise StatesError(type(exc).__name__, str(exc)) from exc

    return wrapped


def build_resources(deps) -> dict:
    return {
        "reserve_inventory": _wrap(reserve_inventory.handle, deps),
        "release_inventory": _wrap(release_inventory.handle, deps),
        "charge_payment": _wrap(charge_payment.handle, deps),
        "refund_payment": _wrap(refund_payment.handle, deps),
        "create_shipment": _wrap(create_shipment.handle, deps),
        "finalize_order": _wrap(finalize_order.handle, deps),
    }
