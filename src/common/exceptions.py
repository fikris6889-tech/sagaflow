"""
Exception hierarchy shared by every saga step (Lambda handler).

Step Functions decides whether to retry a failed Task or route it to a
compensation branch by matching the *name* of the raised exception class
against the `ErrorEquals` list in the state machine definition
(statemachine/order_saga.asl.json). That means the exception class names
below are not just Python plumbing -- they are effectively part of the
public contract between this code and the state machine definition.
Renaming one without updating the ASL file (and tools/validate_asl.py's
fixtures) would silently break retry/compensation routing in production.

Two top-level categories:

* TransientError - something that *might* succeed if we just try again
  (a throttled DynamoDB write, a network blip talking to the mock payment
  processor). Step Functions' `Retry` block handles these with capped,
  exponential backoff. The step's own side effects must be safe to retry,
  which is why every mutating step is idempotent (see
  common/idempotency.py).

* BusinessError - a *permanent* failure for this order (out of stock, the
  card was declined). Retrying will not help. Step Functions' `Catch`
  block routes these straight into the saga's compensation branch instead
  of burning retry budget on something that can never succeed.
"""

from __future__ import annotations


class SagaError(Exception):
    """Base class for every exception this project raises on purpose."""


class TransientError(SagaError):
    """A retryable failure. Step Functions' Retry policy should catch this."""


class BusinessError(SagaError):
    """A permanent, business-level failure. Step Functions' Catch policy
    should route this into compensation, never retry it."""


class InsufficientInventoryError(BusinessError):
    """Not enough stock to reserve the requested quantity."""


class OptimisticLockConflict(TransientError):
    """A conditional write lost a race with a concurrent writer.

    In real DynamoDB this is exactly what a failed
    ConditionExpression on an UpdateItem call looks like. It is transient
    by nature: the caller should re-read the item and retry the update,
    which is precisely what Step Functions' Retry block does.
    """


class PaymentDeclinedError(BusinessError):
    """The payment processor declined the charge."""


class ShipmentError(BusinessError):
    """The shipping provider could not create a shipment for this order."""


class ThrottledError(TransientError):
    """A downstream dependency (DynamoDB, the payment gateway, ...) is
    rate-limiting us right now."""
