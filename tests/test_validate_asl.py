import json
import unittest
from pathlib import Path

from tools.validate_asl import validate

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_real_definition() -> dict:
    return json.loads((REPO_ROOT / "statemachine" / "order_saga.asl.json").read_text())


class TestValidateRealDefinition(unittest.TestCase):
    def test_the_actual_shipped_definition_has_no_problems(self):
        problems = validate(_load_real_definition())
        self.assertEqual(problems, [])


class TestValidateCatchesBrokenDefinitions(unittest.TestCase):
    """Each test below takes a copy of the real, valid definition and
    breaks exactly one thing about it, then asserts the validator
    actually notices. This is what makes tools/validate_asl.py itself a
    tested piece of the project rather than an unverified assumption."""

    def setUp(self):
        self.definition = _load_real_definition()

    def test_catches_a_next_pointing_at_a_typo_d_state_name(self):
        self.definition["States"]["ReserveInventory"]["Next"] = "ChragePayment"  # typo
        problems = validate(self.definition)
        self.assertTrue(any("unknown target" in p for p in problems), problems)

    def test_catches_an_unreachable_state(self):
        self.definition["States"]["Orphan"] = {"Type": "Succeed"}
        problems = validate(self.definition)
        self.assertTrue(any("unreachable" in p for p in problems), problems)

    def test_catches_a_zero_interval_seconds(self):
        self.definition["States"]["ReserveInventory"]["Retry"][0]["IntervalSeconds"] = 0
        problems = validate(self.definition)
        self.assertTrue(any("non-positive IntervalSeconds" in p for p in problems), problems)

    def test_catches_a_backoff_rate_below_one(self):
        self.definition["States"]["ReserveInventory"]["Retry"][0]["BackoffRate"] = 0.5
        problems = validate(self.definition)
        self.assertTrue(any("BackoffRate < 1.0" in p for p in problems), problems)

    def test_catches_a_negative_max_attempts(self):
        self.definition["States"]["ReserveInventory"]["Retry"][0]["MaxAttempts"] = -1
        problems = validate(self.definition)
        self.assertTrue(any("negative MaxAttempts" in p for p in problems), problems)

    def test_catches_a_catch_that_dead_ends_without_reaching_fail_or_succeed(self):
        # Point ChargePayment's business-error Catch at a Task that has
        # neither Next nor End -- a dead end no real deployment should
        # ever have, since the execution would just hang with nowhere
        # left to go.
        self.definition["States"]["Dangling"] = {"Type": "Task", "Resource": "release_inventory"}
        self.definition["States"]["ChargePayment"]["Catch"][0]["Next"] = "Dangling"
        problems = validate(self.definition)
        self.assertTrue(any("never reaches a Fail/Succeed/End" in p for p in problems), problems)

    def test_catches_a_missing_compensation_for_an_already_succeeded_step(self):
        # Simulate someone "simplifying" ChargePayment's Catch to skip
        # releasing inventory -- the exact class of bug this project's
        # own saga-completeness invariant exists to catch before it ships.
        self.definition["States"]["ChargePayment"]["Catch"] = [
            {"ErrorEquals": ["PaymentDeclinedError"], "Next": "OrderFailedNoCompensation"}
        ]
        problems = validate(self.definition)
        self.assertTrue(
            any("release_inventory" in p and "ChargePayment" in p for p in problems), problems
        )

    def test_catches_a_bad_start_at(self):
        self.definition["StartAt"] = "DoesNotExist"
        problems = validate(self.definition)
        self.assertTrue(any("StartAt" in p for p in problems), problems)


if __name__ == "__main__":
    unittest.main()
