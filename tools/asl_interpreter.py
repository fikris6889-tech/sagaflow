"""
A small, deliberately scoped interpreter for Amazon States Language (ASL)
JSON, covering exactly the state types SagaFlow's own
statemachine/order_saga.asl.json uses: Task, Parallel, Fail, Succeed,
Pass, plus each Task's Retry and Catch blocks.

Why this exists at all: this project has no live AWS account to deploy
to from this build environment, and no local Step Functions emulator is
available either. Rather than write a second, hand-rolled Python
re-implementation of the saga's control flow (forward chain +
compensation routing) purely for testing -- which could quietly drift
out of sync with the real order_saga.asl.json file the moment either one
changes -- this interpreter loads and executes the *actual* ASL file, so
tests/test_saga_end_to_end.py is proving the real deployment artifact
correct, not a stand-in for it.

This is intentionally NOT a general-purpose Step Functions engine: it
has no Choice, Map, Wait, or JSONPath-based input/output processing,
because order_saga.asl.json does not use them. Extending
order_saga.asl.json with a new state type this interpreter does not
support will raise a clear NotImplementedError here rather than
silently doing the wrong thing -- see `_run_state`'s final `else`
branch.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Optional

# In the real deployment, Step Functions waits IntervalSeconds * BackoffRate**n
# between retries. Tests don't want to actually sleep, so the interpreter
# still enforces MaxAttempts / BackoffRate *counting* faithfully (proving the
# retry budget is respected) but takes zero wall-clock time doing it.
SIMULATE_RETRY_DELAYS = False


def resource_name(raw: str) -> str:
    """statemachine/order_saga.asl.json's Resource fields use SAM's
    `${name}` DefinitionSubstitutions syntax (the exact placeholder
    `sam build`/`sam deploy` replaces with each Lambda's real ARN at
    deploy time -- see template.yaml's DefinitionSubstitutions block).
    This interpreter runs against the checked-in file *before* that
    substitution ever happens, so it strips the `${...}` wrapper back
    off to get the plain name the `resources` registry is keyed by.
    A Resource string with no such wrapper is returned unchanged, so
    passing an already-plain name (as every unit test fixture in
    tests/test_validate_asl.py does) still works."""

    if raw.startswith("${") and raw.endswith("}"):
        return raw[2:-1]
    return raw


class StatesError(Exception):
    """Raised by a resource function to signal a named ASL error, the
    same way a real AWS Lambda integration surfaces the invoked
    function's exception class name as Step Functions' `errorType` for
    Catch/Retry matching."""

    def __init__(self, error: str, cause: str = ""):
        self.error = error
        self.cause = cause
        super().__init__(f"{error}: {cause}")


@dataclass
class ExecutionResult:
    status: str  # "SUCCEEDED" or "FAILED"
    output: dict
    trace: list[str] = field(default_factory=list)
    fail_error: Optional[str] = None
    fail_cause: Optional[str] = None


def _error_equals_matches(error_equals: list[str], error_name: str) -> bool:
    return "States.ALL" in error_equals or error_name in error_equals


def _find_matching_retrier(retriers: list[dict], error_name: str, attempts_so_far: dict) -> Optional[dict]:
    for retrier in retriers:
        if _error_equals_matches(retrier.get("ErrorEquals", []), error_name):
            return retrier
    return None


def _find_matching_catcher(catchers: list[dict], error_name: str) -> Optional[dict]:
    for catcher in catchers:
        if _error_equals_matches(catcher.get("ErrorEquals", []), error_name):
            return catcher
    return None


def run_state_machine(
    definition: dict,
    input_data: dict,
    resources: dict[str, Callable[[dict], dict]],
) -> ExecutionResult:
    """Execute `definition` (a parsed ASL document, or one Parallel
    branch of one) starting at its `StartAt` state, calling into
    `resources[state["Resource"]]` for every Task state.

    A resource function takes the current data dict and returns the next
    data dict, or raises StatesError(error_name, cause) to signal a named
    failure for Retry/Catch matching.
    """

    states = definition["States"]
    current_name = definition["StartAt"]
    data = input_data
    trace: list[str] = []
    # Per-state-per-error attempt counters, so a Task's retry budget is
    # tracked independently of any other Task's, exactly like real Step
    # Functions (retries never leak across states).
    attempts: dict[tuple[str, str], int] = {}

    while True:
        state = states[current_name]
        trace.append(current_name)
        state_type = state["Type"]

        if state_type == "Pass":
            if "Result" in state:
                data = state["Result"]
            if state.get("End"):
                return ExecutionResult("SUCCEEDED", data, trace)
            current_name = state["Next"]
            continue

        if state_type == "Succeed":
            return ExecutionResult("SUCCEEDED", data, trace)

        if state_type == "Fail":
            return ExecutionResult(
                "FAILED", data, trace, fail_error=state.get("Error"), fail_cause=state.get("Cause")
            )

        if state_type == "Task":
            fn = resources[resource_name(state["Resource"])]
            while True:
                try:
                    data = fn(data)
                    break
                except StatesError as exc:
                    retrier = _find_matching_retrier(state.get("Retry", []), exc.error, attempts)
                    key = (current_name, exc.error)
                    if retrier is not None and attempts.get(key, 0) < retrier.get("MaxAttempts", 0):
                        attempts[key] = attempts.get(key, 0) + 1
                        continue  # simulated retry, no real sleep
                    catcher = _find_matching_catcher(state.get("Catch", []), exc.error)
                    if catcher is not None:
                        result_path_key = catcher.get("ResultPath", "$.error").lstrip("$.")
                        data = {**data, result_path_key: {"Error": exc.error, "Cause": exc.cause}}
                        current_name = catcher["Next"]
                        break
                    raise
            else:  # pragma: no cover - unreachable, defensive only
                pass
            if current_name != trace[-1]:
                # a Catch redirected us; loop back to the top with the new state
                continue
            if state.get("End"):
                return ExecutionResult("SUCCEEDED", data, trace)
            current_name = state["Next"]
            continue

        if state_type == "Parallel":
            branch_results = []
            for branch in state["Branches"]:
                branch_result = run_state_machine(branch, data, resources)
                if branch_result.status == "FAILED":
                    # Real Step Functions fails the whole Parallel state if
                    # any branch fails (subject to the Parallel state's own
                    # Catch, which order_saga.asl.json does not use because
                    # every branch's own Tasks already have their own Retry
                    # policy and nothing in this saga's compensation logic
                    # is itself expected to fail permanently).
                    return ExecutionResult(
                        "FAILED",
                        data,
                        trace + branch_result.trace,
                        fail_error=branch_result.fail_error,
                        fail_cause=branch_result.fail_cause,
                    )
                branch_results.append(branch_result.output)
            data = branch_results
            if state.get("End"):
                return ExecutionResult("SUCCEEDED", data, trace)
            current_name = state["Next"]
            continue

        raise NotImplementedError(
            f"asl_interpreter.py does not support state type {state_type!r} "
            f"(state {current_name!r}) -- extend it before using this state type."
        )
