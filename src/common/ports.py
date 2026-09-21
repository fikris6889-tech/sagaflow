"""
Port (interface) definitions for every external dependency a saga step
talks to.

This project follows a ports-and-adapters (hexagonal) split specifically
so the business logic in src/handlers/*.py can be tested with real
concurrency and real failure injection *without* a live AWS account, a
running DynamoDB, or even the `boto3` package installed:

* Production wiring (src/common/dynamo_adapters.py) implements these
  ports against real DynamoDB tables via boto3. It is only imported
  inside each Lambda's `lambda_handler` entry point, which is only ever
  invoked by the real AWS Lambda runtime -- where boto3 is preinstalled.

* Test wiring (src/common/fake_adapters.py) implements the exact same
  ports as thread-safe, pure-stdlib in-memory stores. The test suite
  injects these into the handlers' core `handle()` functions directly,
  so every test in tests/ exercises the real business logic, the real
  optimistic-concurrency-control rules, and the real
  statemachine/order_saga.asl.json definition (via tools/asl_interpreter.py)
  -- just without a network call in sight.

Every method below documents the exact DynamoDB operation the production
adapter uses to implement it, so the contract stays honest even though
the interface itself never imports boto3.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class ReservationResult:
    sku: str
    qty_reserved: int
    remaining_qty: int


@dataclass(frozen=True)
class ChargeResult:
    charge_id: str
    amount_cents: int


@dataclass(frozen=True)
class ShipmentResult:
    shipment_id: str
    order_id: str


class InventoryStore(Protocol):
    """Backed in production by a DynamoDB table keyed on `sku`, with a
    numeric `quantity` attribute. Reservation is implemented as:

        UpdateItem(
            Key={"sku": sku},
            UpdateExpression="SET quantity = quantity - :qty",
            ConditionExpression="quantity >= :qty",
            ExpressionAttributeValues={":qty": qty},
        )

    DynamoDB evaluates the ConditionExpression atomically against the
    item's *current* value at write time, so this is optimistic
    concurrency control with no separate read-then-write race window --
    a `ConditionalCheckFailedException` from that call is translated to
    OptimisticLockConflict (transient, Step Functions retries with a
    fresh read) rather than InsufficientInventoryError (permanent,
    checked explicitly beforehand so the two failure modes never get
    confused).
    """

    def reserve(self, sku: str, qty: int) -> ReservationResult: ...

    def release(self, sku: str, qty: int) -> ReservationResult:
        """Compensation for `reserve`. Backed by the inverse UpdateItem
        (`quantity = quantity + :qty`), with no ConditionExpression --
        releasing stock can never fail on a business rule."""
        ...


class PaymentGateway(Protocol):
    """Stands in for a real processor (Stripe, Braintree, ...). Every
    real payment API of this kind requires an idempotency key on writes
    specifically so retries after a timeout can never double-charge --
    this port mirrors that contract exactly."""

    def charge(self, order_id: str, amount_cents: int, idempotency_key: str) -> ChargeResult: ...

    def refund(self, charge_id: str, idempotency_key: str) -> None:
        """Compensation for `charge`."""
        ...


class ShipmentProvider(Protocol):
    def create_shipment(self, order_id: str, address: str) -> ShipmentResult: ...


class IdempotencyStore(Protocol):
    """Backed in production by a DynamoDB table keyed on `idempotency_key`
    with a TTL attribute, written via:

        PutItem(
            Item={"idempotency_key": key, "result": ..., "ttl": ...},
            ConditionExpression="attribute_not_exists(idempotency_key)",
        )

    The ConditionExpression is what makes `record` atomic: if two
    concurrent Lambda retries race to record the same key, exactly one
    PutItem wins and the loser's ConditionalCheckFailedException tells it
    to go read the winner's cached result instead of redoing the work.
    """

    def get_cached_result(self, key: str) -> dict | None: ...

    def record(self, key: str, result: dict) -> None: ...


class OrderStore(Protocol):
    """Backed in production by a DynamoDB table keyed on `order_id`."""

    def set_status(self, order_id: str, status: str) -> None: ...

    def get_status(self, order_id: str) -> str | None: ...
