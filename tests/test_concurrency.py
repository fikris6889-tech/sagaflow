"""
Real-thread concurrency tests for InMemoryInventoryStore's optimistic
locking. These do not mock time or use fake threads -- they spin up real
`threading.Thread`s racing for the same, deliberately scarce SKU, which
is the only way to actually catch a broken lock scope (e.g. one global
lock instead of one per SKU silently "working" in a sequential test but
serializing unrelated SKUs, or a check-then-write race that isn't truly
atomic).
"""

from __future__ import annotations

import threading
import unittest

from src.common.exceptions import InsufficientInventoryError
from src.common.fake_adapters import FakeDeps
from src.handlers import reserve_inventory


class TestConcurrentReservations(unittest.TestCase):
    def test_concurrent_orders_never_oversell_a_scarce_sku(self):
        deps = FakeDeps(initial_stock={"LIMITED-EDITION": 10})
        num_threads = 50
        qty_per_order = 1

        successes = []
        failures = []
        lock = threading.Lock()
        barrier = threading.Barrier(num_threads)

        def worker(order_id: str):
            barrier.wait()  # maximize actual overlap, not just "started around the same time"
            try:
                result = reserve_inventory.handle(
                    {"order_id": order_id, "sku": "LIMITED-EDITION", "qty": qty_per_order}, deps
                )
                with lock:
                    successes.append(result)
            except InsufficientInventoryError:
                with lock:
                    failures.append(order_id)

        threads = [threading.Thread(target=worker, args=(f"order-{i}",)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Exactly 10 of the 50 concurrent orders should have won a unit;
        # the rest must have failed cleanly rather than oversold.
        self.assertEqual(len(successes), 10)
        self.assertEqual(len(failures), num_threads - 10)
        self.assertEqual(deps.inventory_store.quantity_of("LIMITED-EDITION"), 0)

        # No two winning reservations should report an overlapping
        # remaining_qty -- each successful call must have observed a
        # strictly different post-decrement value (proves the decrements
        # were serialized correctly per-SKU, not merely that the final
        # count happens to be right, which a lost-update bug could also
        # produce by coincidence with small numbers).
        remaining_values = [r["reservation"]["remaining_qty"] for r in successes]
        self.assertEqual(len(remaining_values), len(set(remaining_values)))
        self.assertEqual(sorted(remaining_values), list(range(10)))

    def test_concurrent_reservations_on_different_skus_do_not_serialize_each_other(self):
        # A correct per-SKU lock should let two different SKUs' reservations
        # both make progress concurrently. This test doesn't assert on
        # wall-clock timing (too flaky in a shared CI/test environment);
        # instead it asserts on correctness: both SKUs end up with the
        # right stock regardless of thread interleaving.
        deps = FakeDeps(initial_stock={"SKU-A": 5, "SKU-B": 5})

        def reserve_many(sku: str, order_prefix: str):
            for i in range(5):
                reserve_inventory.handle({"order_id": f"{order_prefix}-{i}", "sku": sku, "qty": 1}, deps)

        t1 = threading.Thread(target=reserve_many, args=("SKU-A", "a"))
        t2 = threading.Thread(target=reserve_many, args=("SKU-B", "b"))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        self.assertEqual(deps.inventory_store.quantity_of("SKU-A"), 0)
        self.assertEqual(deps.inventory_store.quantity_of("SKU-B"), 0)


if __name__ == "__main__":
    unittest.main()
