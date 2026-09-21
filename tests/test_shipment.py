import unittest

from src.common.exceptions import ShipmentError
from src.common.fake_adapters import FakeDeps
from src.handlers import create_shipment, finalize_order


class TestCreateShipment(unittest.TestCase):
    def setUp(self):
        self.deps = FakeDeps()

    def test_successful_shipment(self):
        event = {"order_id": "o-1", "address": "1 Main St, Springfield"}
        result = create_shipment.handle(event, self.deps)

        self.assertTrue(result["shipment"]["shipment_id"].startswith("shp_"))
        self.assertEqual(self.deps.order_store.get_status("o-1"), "SHIPMENT_CREATED")

    def test_unshippable_address_raises_business_error(self):
        event = {"order_id": "o-2", "address": "UNSHIPPABLE ADDRESS"}
        with self.assertRaises(ShipmentError):
            create_shipment.handle(event, self.deps)
        self.assertNotEqual(self.deps.order_store.get_status("o-2"), "SHIPMENT_CREATED")


class TestFinalizeOrder(unittest.TestCase):
    def setUp(self):
        self.deps = FakeDeps()

    def test_finalize_marks_order_completed(self):
        result = finalize_order.handle({"order_id": "o-1"}, self.deps)
        self.assertEqual(result["status"], "COMPLETED")
        self.assertEqual(self.deps.order_store.get_status("o-1"), "COMPLETED")


if __name__ == "__main__":
    unittest.main()
