import unittest

from src.common.exceptions import InsufficientInventoryError
from src.common.fake_adapters import FakeDeps
from src.handlers import reserve_inventory, release_inventory


class TestReserveInventory(unittest.TestCase):
    def setUp(self):
        self.deps = FakeDeps(initial_stock={"WIDGET-1": 10})

    def test_successful_reservation_decrements_stock(self):
        event = {"order_id": "o-1", "sku": "WIDGET-1", "qty": 3}
        result = reserve_inventory.handle(event, self.deps)

        self.assertEqual(result["reservation"]["qty_reserved"], 3)
        self.assertEqual(result["reservation"]["remaining_qty"], 7)
        self.assertEqual(self.deps.inventory_store.quantity_of("WIDGET-1"), 7)
        self.assertEqual(self.deps.order_store.get_status("o-1"), "INVENTORY_RESERVED")

    def test_insufficient_stock_raises_business_error_and_leaves_stock_untouched(self):
        event = {"order_id": "o-2", "sku": "WIDGET-1", "qty": 999}

        with self.assertRaises(InsufficientInventoryError):
            reserve_inventory.handle(event, self.deps)

        # A failed reservation must not partially decrement stock.
        self.assertEqual(self.deps.inventory_store.quantity_of("WIDGET-1"), 10)
        # And it must not have written an OrderStore record either --
        # nothing succeeded, so nothing should be persisted (see
        # reserve_inventory.py's docstring).
        self.assertIsNone(self.deps.order_store.get_status("o-2"))

    def test_unknown_sku_has_zero_stock_and_fails_cleanly(self):
        event = {"order_id": "o-3", "sku": "DOES-NOT-EXIST", "qty": 1}
        with self.assertRaises(InsufficientInventoryError):
            reserve_inventory.handle(event, self.deps)


class TestReleaseInventory(unittest.TestCase):
    def setUp(self):
        self.deps = FakeDeps(initial_stock={"WIDGET-1": 10})

    def test_release_restores_exactly_the_reserved_quantity(self):
        reserved = reserve_inventory.handle({"order_id": "o-1", "sku": "WIDGET-1", "qty": 4}, self.deps)
        self.assertEqual(self.deps.inventory_store.quantity_of("WIDGET-1"), 6)

        released = release_inventory.handle(reserved, self.deps)

        self.assertEqual(self.deps.inventory_store.quantity_of("WIDGET-1"), 10)
        self.assertEqual(released["compensation_release"]["remaining_qty"], 10)
        self.assertEqual(self.deps.order_store.get_status("o-1"), "FAILED")

    def test_release_is_idempotent_across_a_simulated_retry(self):
        reserved = reserve_inventory.handle({"order_id": "o-1", "sku": "WIDGET-1", "qty": 4}, self.deps)

        release_inventory.handle(reserved, self.deps)
        # Simulate Step Functions retrying this exact Task (e.g. a
        # timeout on the response after the DynamoDB write committed).
        release_inventory.handle(reserved, self.deps)

        # Must NOT have released the quantity twice.
        self.assertEqual(self.deps.inventory_store.quantity_of("WIDGET-1"), 10)


if __name__ == "__main__":
    unittest.main()
