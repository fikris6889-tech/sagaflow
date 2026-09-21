import json
import unittest

from src.handlers.start_order import ValidationError, handle, parse_and_validate


class _Exceptions:
    class ExecutionAlreadyExists(Exception):
        pass


class FakeStepFunctionsClient:
    """Mimics just enough of boto3's stepfunctions client shape (a
    `.start_execution()` method and a `.exceptions.ExecutionAlreadyExists`
    class, exactly like the real generated client exposes) to unit test
    start_order.handle() without boto3 installed."""

    def __init__(self, already_started_names: set[str] | None = None):
        self.exceptions = _Exceptions()
        self.started: list[dict] = []
        self._already_started_names = already_started_names or set()

    def start_execution(self, stateMachineArn, name, input):
        if name in self._already_started_names:
            raise self.exceptions.ExecutionAlreadyExists(name)
        self.started.append({"stateMachineArn": stateMachineArn, "name": name, "input": input})
        return {"executionArn": f"arn:aws:states:::execution/{name}"}


VALID_ORDER = {
    "order_id": "order-42",
    "sku": "WIDGET-1",
    "qty": 2,
    "amount_cents": 1999,
    "address": "1 Test St",
}


class TestParseAndValidate(unittest.TestCase):
    def test_valid_body_round_trips(self):
        parsed = parse_and_validate(json.dumps(VALID_ORDER))
        self.assertEqual(parsed, VALID_ORDER)

    def test_missing_field_is_rejected(self):
        body = dict(VALID_ORDER)
        del body["sku"]
        with self.assertRaises(ValidationError):
            parse_and_validate(json.dumps(body))

    def test_invalid_json_is_rejected(self):
        with self.assertRaises(ValidationError):
            parse_and_validate("{not json")

    def test_unsafe_order_id_is_rejected(self):
        body = dict(VALID_ORDER, order_id="order with spaces / slashes")
        with self.assertRaises(ValidationError):
            parse_and_validate(json.dumps(body))

    def test_non_positive_qty_is_rejected(self):
        body = dict(VALID_ORDER, qty=0)
        with self.assertRaises(ValidationError):
            parse_and_validate(json.dumps(body))

    def test_non_positive_amount_is_rejected(self):
        body = dict(VALID_ORDER, amount_cents=-5)
        with self.assertRaises(ValidationError):
            parse_and_validate(json.dumps(body))


class TestHandle(unittest.TestCase):
    def test_starts_a_new_execution(self):
        client = FakeStepFunctionsClient()
        result = handle(json.dumps(VALID_ORDER), client, "arn:aws:states:::stateMachine:test")

        self.assertEqual(result["status"], "STARTED")
        self.assertEqual(len(client.started), 1)
        self.assertEqual(client.started[0]["name"], "order-42")

    def test_duplicate_order_id_is_idempotent_not_an_error(self):
        client = FakeStepFunctionsClient(already_started_names={"order-42"})
        result = handle(json.dumps(VALID_ORDER), client, "arn:aws:states:::stateMachine:test")

        self.assertEqual(result["status"], "ALREADY_STARTED")
        self.assertEqual(client.started, [])


if __name__ == "__main__":
    unittest.main()
