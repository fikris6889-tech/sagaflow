"""
A small decorator that makes any saga step idempotent given an
IdempotencyStore, using the same "check cache, do work, record cache"
pattern real payment APIs use for their idempotency-key headers.

Why every mutating step needs this: AWS Lambda + Step Functions gives
*at-least-once* execution. A Task can time out, or the Lambda service can
retry a network blip, *after* the handler's side effect has already
committed but before its response made it back to Step Functions. Step
Functions' `Retry` block will then invoke the same step again with the
same input. Without idempotency, "reserve 3 units" invoked twice means 6
units silently vanish from inventory for one order -- a correctness bug
that will not show up in a single happy-path smoke test, only under real
retries or real network flakiness, which is exactly why
tests/test_idempotency.py drives it explicitly rather than hoping it
never comes up.
"""

from __future__ import annotations

import functools
from typing import Callable

from src.common.ports import IdempotencyStore


def idempotent(step_name: str) -> Callable:
    """Decorate a `handle(event, deps) -> dict` step function.

    `event` must contain `order_id`. The cache key is
    f"{order_id}:{step_name}" -- deterministic and reproducible across
    retries of the *same* saga step for the *same* order, but distinct
    across different steps and different orders, so a cached
    ReserveInventory result can never be mistaken for a cached
    ChargePayment result.
    """

    def decorator(fn: Callable[[dict, object], dict]) -> Callable[[dict, object], dict]:
        @functools.wraps(fn)
        def wrapper(event: dict, deps) -> dict:
            store: IdempotencyStore = deps.idempotency_store
            key = f"{event['order_id']}:{step_name}"
            cached = store.get_cached_result(key)
            if cached is not None:
                return cached
            result = fn(event, deps)
            store.record(key, result)
            return result

        return wrapper

    return decorator
