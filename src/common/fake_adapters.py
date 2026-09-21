"""
Pure-stdlib, thread-safe in-memory adapters implementing the ports in
src/common/ports.py.

These are NOT toy stubs that always succeed -- they deliberately
reproduce the exact failure modes and race conditions the real DynamoDB
adapters exhibit, which is what lets tests/test_concurrency.py fire real
concurrent threads at InMemoryInventoryStore and get a real answer about
whether the optimistic-locking logic is correct, and what lets
tests/test_saga_end_to_end.py drive genuine business failures (declined
payments, out-of-stock SKUs) through the real
statemachine/order_saga.asl.json definition.
"""

from __future__ import annotations

import threading
import uuid
from typing import Optional

from src.common.exceptions import (
    InsufficientInventoryError,
    PaymentDeclinedError,
    ShipmentError,
)
from src.common.ports import ChargeResult, ReservationResult, ShipmentResult


class InMemoryInventoryStore:
    """Simulates a DynamoDB table keyed on `sku` with a `quantity`
    attribute, using one `threading.Lock` per SKU as the in-memory
    stand-in for DynamoDB's atomic, per-item ConditionExpression check.

    The lock is scoped per-SKU (not one global lock) so that concurrent
    reservations against *different* SKUs never contend with each other
    -- matching how DynamoDB's per-item conditional writes behave in
    production, and letting tests prove that property rather than just
    assert it in a docstring.
    """

    def __init__(self, initial_stock: Optional[dict[str, int]] = None) -> None:
        self._stock: dict[str, int] = dict(initial_stock or {})
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()
        self.reserve_calls = 0  # instrumentation for idempotency tests

    def _lock_for(self, sku: str) -> threading.Lock:
        with self._locks_guard:
            if sku not in self._locks:
                self._locks[sku] = threading.Lock()
            return self._locks[sku]

    def reserve(self, sku: str, qty: int) -> ReservationResult:
        if qty <= 0:
            raise ValueError("qty must be positive")
        lock = self._lock_for(sku)
        with lock:
            self.reserve_calls += 1
            available = self._stock.get(sku, 0)
            if available < qty:
                raise InsufficientInventoryError(
                    f"SKU {sku!r} has {available} unit(s), requested {qty}"
                )
            self._stock[sku] = available - qty
            return ReservationResult(sku=sku, qty_reserved=qty, remaining_qty=self._stock[sku])

    def release(self, sku: str, qty: int) -> ReservationResult:
        lock = self._lock_for(sku)
        with lock:
            self._stock[sku] = self._stock.get(sku, 0) + qty
            return ReservationResult(sku=sku, qty_reserved=-qty, remaining_qty=self._stock[sku])

    def quantity_of(self, sku: str) -> int:
        return self._stock.get(sku, 0)


class InMemoryPaymentGateway:
    """Simulates a real card processor's idempotency-key contract: the
    *first* call for a given idempotency_key actually runs the "charge"
    logic (including the decline rule); every subsequent call with the
    same key returns the exact same ChargeResult without re-running
    anything -- so a Step Functions retry after a timeout can never
    double-charge a customer even in a fake that has no database behind
    it at all.

    Decline rule (deliberately simple and documented, not hidden):
    amounts that are an exact multiple of 66600 cents ($666.00) are
    declined, standing in for "the processor's fraud model rejected
    this card." This gives tests/test_saga_end_to_end.py a
    deterministic way to drive the PaymentDeclinedError branch of the
    saga without needing a real fraud model.
    """

    DECLINE_AMOUNT_CENTS = 66_600

    def __init__(self) -> None:
        self._charges_by_key: dict[str, ChargeResult] = {}
        self.charge_calls = 0

    def charge(self, order_id: str, amount_cents: int, idempotency_key: str) -> ChargeResult:
        cached = self._charges_by_key.get(idempotency_key)
        if cached is not None:
            return cached
        self.charge_calls += 1
        if amount_cents % self.DECLINE_AMOUNT_CENTS == 0:
            raise PaymentDeclinedError(
                f"processor declined charge of {amount_cents} cents for order {order_id}"
            )
        result = ChargeResult(charge_id=f"ch_{uuid.uuid4().hex[:16]}", amount_cents=amount_cents)
        self._charges_by_key[idempotency_key] = result
        return result

    def refund(self, charge_id: str, idempotency_key: str) -> None:
        # Refunds are naturally idempotent (refunding an already-refunded
        # charge is a documented no-op on every real processor), so no
        # extra bookkeeping is needed here beyond recording the attempt.
        self._charges_by_key.setdefault(
            f"refund:{idempotency_key}", ChargeResult(charge_id=charge_id, amount_cents=0)
        )


class InMemoryShipmentProvider:
    """Deterministic decline rule: an address containing the substring
    'UNSHIPPABLE' fails, standing in for a real carrier rejecting an
    undeliverable address -- again so the ShipmentError compensation
    branch is reachable by a normal, deterministic unit test."""

    def __init__(self) -> None:
        self._shipments: dict[str, ShipmentResult] = {}

    def create_shipment(self, order_id: str, address: str) -> ShipmentResult:
        if "UNSHIPPABLE" in address:
            raise ShipmentError(f"carrier rejected address for order {order_id}: {address!r}")
        result = ShipmentResult(shipment_id=f"shp_{uuid.uuid4().hex[:16]}", order_id=order_id)
        self._shipments[order_id] = result
        return result


class InMemoryIdempotencyStore:
    """Simulates the conditional-put-based idempotency table described in
    IdempotencyStore's docstring. A `threading.Lock` protects the whole
    dict, which is fine here since these entries are tiny and the point
    under test is the *handler* logic's idempotency, not this store's own
    internal contention behaviour (unlike InventoryStore, where per-SKU
    contention is itself the thing being tested)."""

    def __init__(self) -> None:
        self._results: dict[str, dict] = {}
        self._guard = threading.Lock()

    def get_cached_result(self, key: str) -> Optional[dict]:
        with self._guard:
            return self._results.get(key)

    def record(self, key: str, result: dict) -> None:
        with self._guard:
            self._results.setdefault(key, result)


class InMemoryOrderStore:
    def __init__(self) -> None:
        self._status: dict[str, str] = {}

    def set_status(self, order_id: str, status: str) -> None:
        self._status[order_id] = status

    def get_status(self, order_id: str) -> Optional[str]:
        return self._status.get(order_id)


class FakeDeps:
    """Bundles every fake adapter behind the same attribute names the real
    Lambda handlers expect on their `deps` object, so a single object can
    be constructed once per test and passed to any handler's `handle()`
    function or wired into tools/asl_interpreter.py's resource registry."""

    def __init__(self, initial_stock: Optional[dict[str, int]] = None) -> None:
        self.inventory_store = InMemoryInventoryStore(initial_stock)
        self.payment_gateway = InMemoryPaymentGateway()
        self.shipment_provider = InMemoryShipmentProvider()
        self.idempotency_store = InMemoryIdempotencyStore()
        self.order_store = InMemoryOrderStore()
