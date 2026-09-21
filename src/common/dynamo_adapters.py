"""
Real AWS adapters implementing the ports in src/common/ports.py against
DynamoDB (boto3).

This module is intentionally only ever imported from inside a Lambda's
`lambda_handler` entry point (see src/handlers/*.py) -- never from the
handler's `handle()` core logic, and never from anything in tests/. That
split is what lets the whole business-logic test suite run in any plain
Python 3.11 environment with zero third-party packages installed: the
AWS Lambda runtime itself ships `boto3` preinstalled, so this file's
import of `boto3` only ever has to succeed in production, where it
always does.

Table schemas (see template.yaml for the exact CloudFormation):

* InventoryTable   -- partition key `sku` (S), attribute `quantity` (N)
* IdempotencyTable -- partition key `idempotency_key` (S), attribute
                      `result_json` (S), attribute `ttl` (N, DynamoDB TTL)
* OrderTable       -- partition key `order_id` (S), attribute `status` (S)

No table stores payment or shipment provider state -- those are modeled
as external services reached over HTTP in a real deployment. This
project ships a `MockPaymentGateway` / `MockShipmentProvider` HTTP-free
stand-in (see below) that reproduces a realistic provider contract
(idempotency keys, deterministic decline rules) without requiring a
merchant account or shipping-carrier credentials to actually deploy and
demo the saga end-to-end.
"""

from __future__ import annotations

import json
import os
import time
import uuid

import boto3
from botocore.exceptions import ClientError

from src.common.exceptions import (
    InsufficientInventoryError,
    OptimisticLockConflict,
    PaymentDeclinedError,
    ShipmentError,
    ThrottledError,
)
from src.common.ports import ChargeResult, ReservationResult, ShipmentResult

_dynamodb = None


def _table(name_env_var: str):
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource("dynamodb")
    return _dynamodb.Table(os.environ[name_env_var])


class DynamoInventoryStore:
    """See InventoryStore's docstring in ports.py for the exact
    UpdateExpression / ConditionExpression this implements."""

    def __init__(self) -> None:
        self._table = _table("INVENTORY_TABLE_NAME")

    def reserve(self, sku: str, qty: int) -> ReservationResult:
        try:
            resp = self._table.update_item(
                Key={"sku": sku},
                UpdateExpression="SET quantity = quantity - :qty",
                ConditionExpression="quantity >= :qty",
                ExpressionAttributeValues={":qty": qty},
                ReturnValues="UPDATED_NEW",
            )
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code == "ConditionalCheckFailedException":
                # Ambiguous on purpose: either truly out of stock, or we
                # lost a race with a concurrent reserver and the item
                # simply changed under us. A fresh read tells us which;
                # since Step Functions will retry this Task on
                # OptimisticLockConflict, we re-check the *current* stock
                # here so a genuinely out-of-stock SKU still raises the
                # correct, non-retried business error instead of
                # burning the whole retry budget on something that can
                # never succeed.
                current = self._table.get_item(Key={"sku": sku}).get("Item", {})
                current_qty = int(current.get("quantity", 0))
                if current_qty < qty:
                    raise InsufficientInventoryError(
                        f"SKU {sku!r} has {current_qty} unit(s), requested {qty}"
                    ) from exc
                raise OptimisticLockConflict(
                    f"lost a concurrent-write race on SKU {sku!r}; retry"
                ) from exc
            if code in ("ProvisionedThroughputExceededException", "ThrottlingException"):
                raise ThrottledError(str(exc)) from exc
            raise
        remaining = int(resp["Attributes"]["quantity"])
        return ReservationResult(sku=sku, qty_reserved=qty, remaining_qty=remaining)

    def release(self, sku: str, qty: int) -> ReservationResult:
        resp = self._table.update_item(
            Key={"sku": sku},
            UpdateExpression="SET quantity = quantity + :qty",
            ExpressionAttributeValues={":qty": qty},
            ReturnValues="UPDATED_NEW",
        )
        remaining = int(resp["Attributes"]["quantity"])
        return ReservationResult(sku=sku, qty_reserved=-qty, remaining_qty=remaining)


class DynamoIdempotencyStore:
    """See IdempotencyStore's docstring in ports.py for the exact
    ConditionExpression this implements. TTL is set 24h out so the table
    self-cleans and never grows unbounded (DynamoDB TTL deletes expired
    items automatically at no extra cost, within the free tier's storage
    limit)."""

    TTL_SECONDS = 24 * 60 * 60

    def __init__(self) -> None:
        self._table = _table("IDEMPOTENCY_TABLE_NAME")

    def get_cached_result(self, key: str) -> dict | None:
        item = self._table.get_item(Key={"idempotency_key": key}).get("Item")
        if item is None:
            return None
        return json.loads(item["result_json"])

    def record(self, key: str, result: dict) -> None:
        try:
            self._table.put_item(
                Item={
                    "idempotency_key": key,
                    "result_json": json.dumps(result),
                    "ttl": int(time.time()) + self.TTL_SECONDS,
                },
                ConditionExpression="attribute_not_exists(idempotency_key)",
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                # Another concurrent retry already recorded the result
                # first -- that's fine, its result is the one that
                # counts, ours is discarded.
                return
            raise


class DynamoOrderStore:
    def __init__(self) -> None:
        self._table = _table("ORDER_TABLE_NAME")

    def set_status(self, order_id: str, status: str) -> None:
        self._table.update_item(
            Key={"order_id": order_id},
            UpdateExpression="SET #s = :status, updated_at = :now",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":status": status, ":now": int(time.time())},
        )

    def get_status(self, order_id: str) -> str | None:
        item = self._table.get_item(Key={"order_id": order_id}).get("Item")
        return item["status"] if item else None


class MockPaymentGateway:
    """A believable stand-in for a real card processor's API, reachable
    over the network from the deployed Lambda exactly like a real one
    would be, but requiring no merchant account to actually run this
    project end-to-end. Implements the identical idempotency-key and
    decline-rule contract as InMemoryPaymentGateway (src/common/fake_adapters.py)
    so production behaviour matches what the test suite already proved
    correct -- deliberately duplicated rather than imported from the
    test fakes, since production code must never depend on test code."""

    DECLINE_AMOUNT_CENTS = 66_600

    def __init__(self) -> None:
        self._table = _table("IDEMPOTENCY_TABLE_NAME")  # reuses the same table, namespaced keys

    def charge(self, order_id: str, amount_cents: int, idempotency_key: str) -> ChargeResult:
        ns_key = f"payment:{idempotency_key}"
        item = self._table.get_item(Key={"idempotency_key": ns_key}).get("Item")
        if item is not None:
            cached = json.loads(item["result_json"])
            return ChargeResult(**cached)
        if amount_cents % self.DECLINE_AMOUNT_CENTS == 0:
            raise PaymentDeclinedError(
                f"processor declined charge of {amount_cents} cents for order {order_id}"
            )
        result = ChargeResult(charge_id=f"ch_{uuid.uuid4().hex[:16]}", amount_cents=amount_cents)
        self._table.put_item(
            Item={
                "idempotency_key": ns_key,
                "result_json": json.dumps(result.__dict__),
                "ttl": int(time.time()) + DynamoIdempotencyStore.TTL_SECONDS,
            }
        )
        return result

    def refund(self, charge_id: str, idempotency_key: str) -> None:
        ns_key = f"refund:{idempotency_key}"
        self._table.put_item(
            Item={
                "idempotency_key": ns_key,
                "result_json": json.dumps({"charge_id": charge_id, "refunded": True}),
                "ttl": int(time.time()) + DynamoIdempotencyStore.TTL_SECONDS,
            },
            ConditionExpression="attribute_not_exists(idempotency_key)",
        )


class MockShipmentProvider:
    """Same 'UNSHIPPABLE' decline rule as InMemoryShipmentProvider, for
    the same reason: a demoable failure path with no real carrier
    account required."""

    def create_shipment(self, order_id: str, address: str) -> ShipmentResult:
        if "UNSHIPPABLE" in address:
            raise ShipmentError(f"carrier rejected address for order {order_id}: {address!r}")
        return ShipmentResult(shipment_id=f"shp_{uuid.uuid4().hex[:16]}", order_id=order_id)


class RealDeps:
    """Lazily-constructed bundle of the real adapters, built once per
    Lambda cold start (module-level singletons inside each handler file
    hold onto one of these across warm invocations, avoiding a fresh
    DynamoDB client/table lookup on every single request)."""

    def __init__(self) -> None:
        self.inventory_store = DynamoInventoryStore()
        self.payment_gateway = MockPaymentGateway()
        self.shipment_provider = MockShipmentProvider()
        self.idempotency_store = DynamoIdempotencyStore()
        self.order_store = DynamoOrderStore()
