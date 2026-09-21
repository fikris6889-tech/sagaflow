import unittest

from src.common.fake_adapters import FakeDeps
from src.common.idempotency import idempotent


class TestIdempotentDecorator(unittest.TestCase):
    def test_wrapped_function_runs_only_once_per_order_and_step(self):
        deps = FakeDeps()
        call_count = {"n": 0}

        @idempotent("some_step")
        def do_work(event, deps):
            call_count["n"] += 1
            return {**event, "did_work": True}

        result1 = do_work({"order_id": "o-1"}, deps)
        result2 = do_work({"order_id": "o-1"}, deps)

        self.assertEqual(call_count["n"], 1)
        self.assertEqual(result1, result2)

    def test_different_orders_are_independent(self):
        deps = FakeDeps()
        call_count = {"n": 0}

        @idempotent("some_step")
        def do_work(event, deps):
            call_count["n"] += 1
            return {**event, "call_number": call_count["n"]}

        result_a = do_work({"order_id": "o-A"}, deps)
        result_b = do_work({"order_id": "o-B"}, deps)

        self.assertEqual(call_count["n"], 2)
        self.assertNotEqual(result_a["call_number"], result_b["call_number"])

    def test_different_steps_for_the_same_order_are_independent(self):
        deps = FakeDeps()
        calls = {"step_a": 0, "step_b": 0}

        @idempotent("step_a")
        def do_a(event, deps):
            calls["step_a"] += 1
            return event

        @idempotent("step_b")
        def do_b(event, deps):
            calls["step_b"] += 1
            return event

        do_a({"order_id": "o-1"}, deps)
        do_b({"order_id": "o-1"}, deps)
        do_a({"order_id": "o-1"}, deps)  # retry of step_a only

        self.assertEqual(calls["step_a"], 1)
        self.assertEqual(calls["step_b"], 1)


if __name__ == "__main__":
    unittest.main()
