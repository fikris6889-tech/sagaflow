"""
HTTP entry point for starting a new order saga.

Deliberately NOT fronted by API Gateway: API Gateway is not part of
AWS's *Always Free* tier (only a 12-month allowance that a pre-existing
account may have already exhausted), whereas a Lambda Function URL adds
no charge beyond ordinary Lambda invocation pricing, which the account's
1M-requests/month Always Free allowance already covers for a demo
workload many times over. See template.yaml's StartOrderFunction and
README.md's "Cost / free-tier fit" section.

`order_id` doubles as the Step Functions execution name (see `handle`
below), which gives this endpoint a second, independent layer of
idempotency on top of every individual step's own @idempotent wrapper:
Step Functions Standard workflows reject a StartExecution call that
reuses the name of an execution that is still running or that succeeded
within roughly the last 90 days, so retrying "start order o-123" twice
in a row can never accidentally kick off two concurrent sagas for the
same order.
"""

from __future__ import annotations

import json
import re

REQUIRED_FIELDS = ("order_id", "sku", "qty", "amount_cents", "address")

# Step Functions execution names must match [a-zA-Z0-9-_]{1,80} -- order_id
# is validated against a safe subset of that up front so a malformed
# order_id fails fast with a clear 400 instead of an opaque
# InvalidName error from the StartExecution API call itself.
_SAFE_ORDER_ID = re.compile(r"^[A-Za-z0-9\-_]{1,64}$")


class ValidationError(Exception):
    pass


def parse_and_validate(raw_body: str) -> dict:
    try:
        body = json.loads(raw_body or "{}")
    except json.JSONDecodeError as exc:
        raise ValidationError(f"body is not valid JSON: {exc}") from exc

    missing = [f for f in REQUIRED_FIELDS if f not in body]
    if missing:
        raise ValidationError(f"missing required field(s): {missing}")

    if not _SAFE_ORDER_ID.match(str(body["order_id"])):
        raise ValidationError(
            "order_id must match ^[A-Za-z0-9-_]{1,64}$ (Step Functions execution-name safe)"
        )
    if not isinstance(body["qty"], int) or body["qty"] <= 0:
        raise ValidationError("qty must be a positive integer")
    if not isinstance(body["amount_cents"], int) or body["amount_cents"] <= 0:
        raise ValidationError("amount_cents must be a positive integer")
    if not isinstance(body["sku"], str) or not body["sku"]:
        raise ValidationError("sku must be a non-empty string")
    if not isinstance(body["address"], str) or not body["address"]:
        raise ValidationError("address must be a non-empty string")

    return {field: body[field] for field in REQUIRED_FIELDS}


def handle(raw_body: str, sfn_client, state_machine_arn: str) -> dict:
    """Core logic, deliberately free of any Lambda-Function-URL-specific
    request/response shape so it can be unit tested with a fake
    `sfn_client` (any object exposing a `start_execution(...)` method) --
    the same ports-and-adapters split used by every saga step."""

    order = parse_and_validate(raw_body)

    try:
        resp = sfn_client.start_execution(
            stateMachineArn=state_machine_arn,
            name=order["order_id"],
            input=json.dumps(order),
        )
    except sfn_client.exceptions.ExecutionAlreadyExists:
        # A retried StartOrder call for the same order_id -- treat as
        # success, not an error, per this module's idempotency design.
        return {"order_id": order["order_id"], "status": "ALREADY_STARTED"}

    return {"order_id": order["order_id"], "status": "STARTED", "execution_arn": resp["executionArn"]}


def lambda_handler(event, context):  # pragma: no cover - exercised only inside AWS Lambda
    import os

    import boto3

    sfn_client = boto3.client("stepfunctions")
    body = event.get("body", "{}")
    try:
        result = handle(body, sfn_client, os.environ["STATE_MACHINE_ARN"])
        status_code = 200
    except ValidationError as exc:
        result = {"error": str(exc)}
        status_code = 400

    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(result),
    }
