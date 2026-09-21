# SagaFlow — A Distributed Order Saga on AWS Step Functions, Lambda & DynamoDB

SagaFlow is a small, serverless order-processing pipeline that implements
the **Saga pattern** for distributed transactions: it reserves inventory,
charges a payment, and creates a shipment as three independent steps
running in three independent Lambda functions, orchestrated by an AWS
Step Functions state machine — with automatic **compensating
transactions** that undo exactly what already succeeded whenever a later
step fails permanently.

## Why this, and why now

The moment you split a single database transaction across several
independently-deployable services — the textbook microservices move —
you lose the one thing a relational transaction gave you for free:
all-or-nothing atomicity. If "charge the customer" succeeds but "create
the shipment" fails, something has to notice and undo the charge. The
Saga pattern is the standard answer to that problem, and AWS Step
Functions' native `Retry`/`Catch` state-machine primitives are a
genuinely good fit for implementing it — this project is a complete,
tested, from-scratch reference implementation of that combination, not a
toy "hello world" Lambda.

It is also a deliberately good showcase of the failure modes that make
distributed systems hard in practice, each with a real, tested fix:

* **At-least-once execution** (Lambda + Step Functions retries) means
  every mutating step must be *idempotent*, or a retried "reserve 3
  units" silently reserves 6.
* **Concurrent orders racing for the same inventory** need real
  optimistic concurrency control, not a good-faith read-then-write.
* **Partial failure** (2 of 3 steps succeeded, the 3rd didn't) needs
  compensations that are themselves safe to retry and safe to run
  concurrently with each other.

## Architecture

```
                       ┌────────────────────┐
   POST (SigV4) ─────► │ StartOrder (Lambda  │──StartExecution──►┌───────────────────────────┐
   Function URL        │ Function URL)       │                   │   OrderSaga State Machine   │
                       └────────────────────┘                   │        (Step Functions)     │
                                                                  └──────────────┬──────────────┘
                                                                                 │
        ┌────────────────────────────────────────────────────────────────────────┴───────────────┐
        │  ReserveInventory ──► ChargePayment ──► CreateShipment ──► FinalizeOrder ──► (Succeed)   │
        │        │ Catch             │ Catch            │ Catch                                    │
        │        ▼                   ▼                  ▼                                          │
        │   OrderFailed        ReleaseInventory   Parallel: RefundPayment + ReleaseInventory        │
        │  (no compensation)         │                          │                                   │
        │                            ▼                          ▼                                   │
        │                     OrderFailedCompensated    OrderFailedCompensated                       │
        └───────────────────────────────────────────────────────────────────────────────────────────┘
                │                       │                    │                    │
                ▼                       ▼                    ▼                    ▼
        DynamoDB: Inventory      DynamoDB: Idempotency   DynamoDB: Orders   (mock payment / shipment
        (optimistic locking)     (dedupe every step)     (status history)   providers, see below)
```

Every Task box above is its own Lambda function (see `template.yaml`),
each with an IAM policy scoped to only the DynamoDB table(s) it actually
touches. The full compensation-routing logic lives in
`statemachine/order_saga.asl.json` — the Amazon States Language (ASL)
JSON that *is* the orchestration; there is no separate Python
orchestrator to keep in sync with it.

### Ports and adapters (why this project has zero pip installs in its test suite)

Every Lambda handler's business logic (`src/handlers/*.py`, function
`handle(event, deps)`) is written against small interfaces
(`src/common/ports.py`), never directly against `boto3`. Two
implementations exist:

* `src/common/dynamo_adapters.py` — the real DynamoDB-backed adapters,
  imported only inside each handler's `lambda_handler(event, context)`
  entry point, which only the real AWS Lambda runtime ever calls (and
  which ships `boto3` preinstalled).
* `src/common/fake_adapters.py` — thread-safe, pure-Python in-memory
  adapters that reproduce the *same* failure modes and race conditions
  as the real ones (see `InMemoryInventoryStore`'s per-SKU locking).

This is what lets `tests/` run with a stock Python 3.11 interpreter and
**no package installation at all**, while still exercising real
concurrency (actual `threading.Thread`s, not mocked time) and the real,
checked-in `order_saga.asl.json` file (via `tools/asl_interpreter.py` —
see "Testing approach" below).

## Design decisions

* **Standard, not Express, Step Functions workflow.** Standard
  executions are individually queryable after the fact
  (`GetExecutionHistory`), which matters for a saga where "what actually
  happened to order #4471" is a real support question. Express workflows
  only keep history in CloudWatch Logs. Standard's Always Free allowance
  (4,000 state transitions/month) is what this project's cost estimate
  below is scoped against.
* **No API Gateway.** API Gateway is *not* part of AWS's Always Free
  tier — only a 12-month allowance a pre-existing account may have
  already used up. The public entry point (`StartOrder`) instead uses a
  **Lambda Function URL** with `AuthType: AWS_IAM`, which has no charge
  of its own beyond ordinary Lambda invocation pricing.
* **A `States.ALL` safety-net `Catch` on every saga-triggering Task**,
  layered *after* the specific business-error catch. Without it, a
  transient error that exhausts its retry budget (e.g. DynamoDB
  throttled 4 times in a row under real load) would fail the whole Step
  Functions execution uncleanly, with no compensation run at all. With
  it, "ran out of retries" is treated exactly like an explicit business
  failure: fail safe, always attempt compensation.
* **Compensations run in `Parallel` where they're independent.**
  Refunding a payment and releasing reserved inventory don't depend on
  each other, so `CompensateRefundAndRelease` runs both at once rather
  than serializing them for no reason.
* **`FinalizeOrder` has no `Catch`.** By the time shipment creation has
  succeeded, the order has already physically left the building (the
  carrier has it) — there is nothing left for this system to undo. A
  failure updating the final status record is modeled as an operational
  concern for a human, not a saga failure. This is a deliberate,
  documented scope boundary, not an oversight (see "Known limitations").
* **Payment and shipping are realistic mocks, not real integrations.**
  `MockPaymentGateway` / `MockShipmentProvider` (in
  `dynamo_adapters.py`) implement the same idempotency-key contract and
  deterministic decline rules as a real processor/carrier, so the saga's
  failure paths are fully demoable without a merchant account.

## Testing approach

43 tests total, `python3 -m unittest discover -s tests`, zero
third-party packages required. Three layers:

1. **Per-step unit tests** (`test_inventory.py`, `test_payment.py`,
   `test_shipment.py`, `test_start_order.py`) — success paths, business
   failures, and idempotent-retry behaviour for each handler in
   isolation.
2. **Real concurrency** (`test_concurrency.py`) — 50 real threads racing
   for 10 units of a scarce SKU, asserting the exact right number win,
   the rest fail cleanly, stock never goes negative, and every winning
   reservation observed a *distinct* post-decrement value (a check that
   catches a lost-update bug that a mere "final count is right" assertion
   could miss with unlucky timing).
3. **End-to-end saga tests, against the real deployment artifact**
   (`test_saga_end_to_end.py`) — a small, purpose-built ASL interpreter
   (`tools/asl_interpreter.py`) loads and executes the *actual*
   `statemachine/order_saga.asl.json`, wired to the real handler
   functions via fake adapters. This proves the JSON file that would be
   deployed to AWS is correct — happy path, insufficient inventory,
   declined payment, unshippable address, exhausted retry budgets, and a
   full-saga idempotent replay — not a hand-written Python
   reimplementation of "what it should do" that could quietly drift from
   the real file.

There's a fourth layer most projects skip entirely:
`tools/validate_asl.py` is a structural linter for the ASL file itself
(orphaned states, unreachable states, non-terminating catch chains, and
a project-specific "every already-succeeded mutating step must have a
reachable compensation on every later failure path" check), and
`test_validate_asl.py` proves the linter itself actually catches broken
definitions by deliberately breaking a copy of the real file eight
different ways and asserting each one is caught.

**A bug the tests actually caught before this shipped:** the first draft
of the concurrency test asserted the 10 winning reservations' remaining
quantities matched `list(range(9, -1, -1))` (descending) when
`sorted()` naturally produces them ascending — a trivial-looking
assertion-direction mistake, but exactly the kind of thing that would
have made a genuinely correct implementation look broken (or worse,
made a broken one look fine, if the two mistakes had happened to
cancel out). Caught by simply running the suite, fixed in the same
commit as everything else.

## Setup & run (local — no AWS account needed)

```bash
git clone <this repo>
cd 2026-09-21-hard-cloud-sagaflow-order-orchestrator

# Run the whole test suite (zero pip installs required):
python3 -m unittest discover -s tests -v

# Lint the state machine definition on its own:
python3 tools/validate_asl.py statemachine/order_saga.asl.json
```

Both commands are also exactly what a CI pipeline should run on every
pull request, before `sam deploy` ever touches a real AWS account.

## Deployment (AWS, Always Free tier)

Prerequisites: an AWS account, the [AWS SAM
CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html)
installed, and `aws configure` already run with a profile that has
permission to create Lambda functions, DynamoDB tables, Step Functions
state machines, and IAM roles.

```bash
# 1. Build (packages src/ and template.yaml; no third-party deps to
#    vendor, since boto3 ships with the Lambda runtime — see requirements.txt).
sam build

# 2. Deploy, guided (creates an S3 bucket for deployment artifacts under
#    the hood, then walks you through confirming the stack name/region).
sam deploy --guided
#   Stack Name: sagaflow
#   AWS Region: pick any (e.g. us-east-1)
#   Confirm changes before deploy: Y
#   Allow SAM CLI IAM role creation: Y
#   Save arguments to configuration file: Y   (writes samconfig.toml, gitignored)

# 3. Grab the Function URL from the stack outputs:
aws cloudformation describe-stacks --stack-name sagaflow \
  --query "Stacks[0].Outputs[?OutputKey=='StartOrderFunctionUrl'].OutputValue" --output text

# 4. Seed some inventory (the InventoryTable starts empty on purpose --
#    a real system would seed it from a catalog service, not this saga):
aws dynamodb put-item --table-name SagaFlow-Inventory \
  --item '{"sku": {"S": "WIDGET-1"}, "quantity": {"N": "50"}}'

# 5. Start an order. The Function URL requires SigV4 signing
#    (AuthType: AWS_IAM in template.yaml) -- the AWS CLI can sign this
#    for you via `--aws-sigv4` support in curl, or more simply, drive it
#    the same way this project's own tests do: directly via boto3
#    (`boto3.client("stepfunctions").start_execution(...)`), which is
#    exactly what StartOrderFunction itself does on your behalf.
aws stepfunctions start-execution \
  --state-machine-arn "$(aws cloudformation describe-stacks --stack-name sagaflow \
      --query "Stacks[0].Outputs[?OutputKey=='StateMachineArn'].OutputValue" --output text)" \
  --name "demo-order-1" \
  --input '{"order_id":"demo-order-1","sku":"WIDGET-1","qty":2,"amount_cents":2499,"address":"1 Demo St"}'

# 6. Watch it run in the AWS Console under Step Functions -> State
#    machines -> SagaFlow-OrderSaga -> demo-order-1, which renders the
#    exact ASL graph in "## Architecture" above with live status per state.

# Tear down everything (stops any further charges):
sam delete --stack-name sagaflow
```

### Cost / free-tier fit

Every resource in `template.yaml` was chosen against AWS's **Always
Free** tier (not the 12-month new-account allowance, which may already
be spent on a pre-existing account), as of this project's build date:

| Service | Always-Free allowance | This project's footprint |
|---|---|---|
| Lambda | 1M requests/mo, 400K GB-seconds compute | 7 tiny (128MB) functions; a full happy-path saga is ~5 invocations |
| DynamoDB | 25 GB storage + 25 RCU/25 WCU (account-wide, provisioned) | 3 tables × 1 RCU/1 WCU = 3 RCU + 3 WCU total |
| Step Functions (Standard) | 4,000 state transitions/mo | A full happy-path saga is 4 state transitions; even the 4-branch compensation path is under 10 |

At those numbers, this project can run several thousand demo orders a
month without leaving the Always Free tier at all.

## Known limitations (documented, not hidden)

* **`FinalizeOrder` failures are not compensated** (see "Design
  decisions" above) — by design, since the shipment has already
  physically shipped by that point. A production system would instead
  alert an operator and retry the status write out-of-band.
* **The payment and shipping "providers" are realistic mocks**, not
  real Stripe/carrier integrations — see "Design decisions." Swapping in
  a real provider means changing exactly `MockPaymentGateway` /
  `MockShipmentProvider` in `dynamo_adapters.py`; nothing else in the
  saga or its tests needs to change, which is the whole point of the
  ports-and-adapters split.
* **No dead-letter queue on the Lambda functions themselves.** Step
  Functions' own `Retry`/`Catch` is this project's failure-handling
  layer; a genuinely production-grade deployment would still add
  Lambda-level DLQs or Step Functions' `execution failed` CloudWatch
  alarms for the "every Catch was itself exhausted" edge case, which is
  out of scope for a single-day build.
* **`tools/asl_interpreter.py` is intentionally minimal** — it supports
  exactly the ASL feature set this one state machine uses (Task,
  Parallel, Fail, Succeed, Pass, Retry, Catch), not the full ASL
  specification (no Choice, Map, or Wait). See its module docstring.

## Repository layout

```
template.yaml                       AWS SAM IaC — every resource, every IAM policy
statemachine/order_saga.asl.json    The saga itself, in Amazon States Language
src/common/ports.py                 Interfaces every handler codes against
src/common/fake_adapters.py         In-memory adapters used by every test
src/common/dynamo_adapters.py       Real DynamoDB/mock-provider adapters (Lambda-only)
src/common/idempotency.py           @idempotent decorator shared by every step
src/common/exceptions.py            The exception hierarchy Retry/Catch match on
src/handlers/*.py                   One file per Lambda function
tools/asl_interpreter.py            Minimal ASL executor used by the E2E tests
tools/validate_asl.py               Structural linter for the ASL file
tests/                              43 tests, zero third-party dependencies
```
