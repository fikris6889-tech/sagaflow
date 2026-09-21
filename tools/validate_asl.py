"""
A structural linter for this project's own Amazon States Language (ASL)
definitions -- the kind of check a CI pipeline would run against
statemachine/*.asl.json on every pull request, before ever attempting a
real `aws stepfunctions create-state-machine` call.

boto3's own `create_state_machine` / `update_state_machine` API does
validate ASL syntax against the full spec, but only against a live AWS
account, and only for generic ASL correctness -- it has no idea that,
for *this* saga specifically, every Task that mutates external state
after the first one must have a reachable compensation path on failure.
That project-specific invariant is exactly what this validator checks,
and it is intentionally opinionated about SagaFlow rather than being a
general ASL linter.

Checks performed:
1. The definition has StartAt/States, and every state referenced by a
   Next, a Catch's Next, or a Parallel branch's own StartAt actually
   exists (catches typos in state names, which a JSON file offers no
   other protection against).
2. Every state is reachable from StartAt (no orphaned/dead states left
   behind after a refactor).
3. Every Task's Retry entries have sane values (MaxAttempts >= 0,
   IntervalSeconds > 0, BackoffRate >= 1.0) -- a BackoffRate below 1.0 or
   a zero IntervalSeconds would make retries either immediately hammer a
   struggling dependency or never actually back off.
4. Every path from a "mutating" Task's Catch eventually reaches a
   Fail or Succeed state (a saga's compensation chain must terminate,
   never loop or dead-end on a Task with no further Next/End).
5. SagaFlow's specific saga invariant: once a mutating Task has
   succeeded, every failure catch for every *later* Task must be able to
   reach the compensation Resource for everything that could have
   already succeeded by that point. This is expressed as an explicit,
   hand-maintained map (`MUTATING_STEP_COMPENSATION`) rather than
   inferred automatically -- inferring "which resource undoes which"
   from names alone would be fragile, and a wrong inference would be
   worse than no check at all.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools.asl_interpreter import resource_name  # noqa: E402

# Maps each forward, state-mutating step to the *set* of compensating
# resources that must all appear somewhere on every failure path of every
# step that runs after it. Kept in lockstep with statemachine/order_saga.asl.json
# and src/handlers/*.py by hand -- see this module's docstring for why that's
# a deliberate choice, not an oversight.
MUTATING_STEP_COMPENSATION: dict[str, set[str]] = {
    "reserve_inventory": {"release_inventory"},
    "charge_payment": {"refund_payment"},
    # create_shipment has no compensation by design (see create_shipment.py's
    # docstring) -- intentionally absent from this map.
}

FORWARD_ORDER = ["reserve_inventory", "charge_payment", "create_shipment", "finalize_order"]


class ValidationError(Exception):
    pass


def _iter_all_states(definition: dict, prefix: str = "") -> dict[str, dict]:
    """Flattens a definition's States plus every Parallel branch's States
    into one dict, with branch states qualified as
    'ParentStateName/BranchIndex/StateName' so names never collide
    across branches or with the top level."""

    flat: dict[str, dict] = {}
    for name, state in definition.get("States", {}).items():
        qualified = f"{prefix}{name}"
        flat[qualified] = state
        if state.get("Type") == "Parallel":
            for i, branch in enumerate(state["Branches"]):
                flat.update(_iter_all_states(branch, prefix=f"{qualified}/branch{i}/"))
    return flat


def _local_next_targets(state: dict) -> list[str]:
    """Next-like targets that live in the *same* States dict as `state`
    (i.e. excludes a Parallel state's own branch StartAt, which lives in
    a nested, separate States dict and is validated recursively instead)."""

    targets = []
    if "Next" in state:
        targets.append(state["Next"])
    for catcher in state.get("Catch", []):
        if "Next" in catcher:
            targets.append(catcher["Next"])
    return targets


def validate(definition: dict, _top_level: bool = True) -> list[str]:
    """Returns a list of human-readable problems found. An empty list
    means the definition passed every check.

    `_top_level` is internal: check 5 (the SagaFlow-specific saga
    invariant) only makes sense against the *whole* state machine, since
    it reasons about the full forward chain across all four steps -- it
    is skipped when this function recurses into a single Parallel
    branch, which by design only ever contains one or two compensation
    Tasks, not the full saga.
    """

    problems: list[str] = []

    if "StartAt" not in definition or "States" not in definition:
        return ["Definition is missing required top-level 'StartAt' or 'States' key."]

    states = definition["States"]
    if definition["StartAt"] not in states:
        problems.append(f"StartAt {definition['StartAt']!r} is not a state in States.")

    # 1 & 2: every referenced Next/Catch target exists, and reachability.
    reachable: set[str] = set()
    stack = [definition["StartAt"]] if definition["StartAt"] in states else []
    while stack:
        name = stack.pop()
        if name in reachable:
            continue
        reachable.add(name)
        state = states.get(name)
        if state is None:
            continue
        for target in _local_next_targets(state):
            if target not in states:
                problems.append(f"State {name!r} references unknown target {target!r}.")
            else:
                stack.append(target)
        if state.get("Type") == "Parallel":
            for i, branch in enumerate(state.get("Branches", [])):
                sub_problems = validate(branch, _top_level=False)
                problems.extend(f"{name}/branch{i}: {p}" for p in sub_problems)
            for target in [state["Next"]] if "Next" in state else []:
                if target not in states:
                    problems.append(f"State {name!r} references unknown target {target!r}.")
                else:
                    stack.append(target)

    for name in states:
        if name not in reachable:
            problems.append(f"State {name!r} is unreachable from StartAt.")

    # 3: sane Retry values.
    for name, state in states.items():
        if state.get("Type") != "Task":
            continue
        for retrier in state.get("Retry", []):
            max_attempts = retrier.get("MaxAttempts", 3)
            interval = retrier.get("IntervalSeconds", 1)
            backoff = retrier.get("BackoffRate", 2.0)
            if max_attempts < 0:
                problems.append(f"Task {name!r} has a negative MaxAttempts ({max_attempts}).")
            if interval <= 0:
                problems.append(f"Task {name!r} has a non-positive IntervalSeconds ({interval}).")
            if backoff < 1.0:
                problems.append(f"Task {name!r} has a BackoffRate < 1.0 ({backoff}), retries would never slow down.")

    # 4: every Catch chain terminates at Fail or Succeed (walk forward from
    # each Catch's Next until End/Fail/Succeed, bounded to avoid infinite
    # loops on a malformed cycle).
    for name, state in states.items():
        if state.get("Type") != "Task":
            continue
        for catcher in state.get("Catch", []):
            target = catcher.get("Next")
            visited = set()
            cursor = target
            terminated = False
            while cursor and cursor not in visited:
                visited.add(cursor)
                cursor_state = states.get(cursor)
                if cursor_state is None:
                    break
                ctype = cursor_state.get("Type")
                if ctype in ("Fail", "Succeed"):
                    terminated = True
                    break
                if ctype == "Parallel":
                    cursor = cursor_state.get("Next")
                    if cursor is None:
                        terminated = cursor_state.get("End", False)
                    continue
                if cursor_state.get("End"):
                    terminated = True
                    break
                cursor = cursor_state.get("Next")
            if not terminated:
                problems.append(
                    f"Catch on {name!r} (target {target!r}) never reaches a Fail/Succeed/End state."
                )

    # 5: SagaFlow's own saga-completeness invariant (whole-machine only).
    if not _top_level:
        return problems
    for i, step in enumerate(FORWARD_ORDER):
        already_succeeded_compensations: set[str] = set()
        for earlier in FORWARD_ORDER[:i]:
            already_succeeded_compensations |= MUTATING_STEP_COMPENSATION.get(earlier, set())
        if not already_succeeded_compensations:
            continue
        # Find the Task state whose Resource is `step`.
        step_state_name = next(
            (n for n, s in states.items() if resource_name(s.get("Resource", "")) == step), None
        )
        if step_state_name is None:
            problems.append(f"No state in the definition has Resource {step!r}.")
            continue
        step_state = states[step_state_name]
        for catcher in step_state.get("Catch", []):
            reachable_resources = _resources_reachable_from(catcher["Next"], states)
            missing = already_succeeded_compensations - reachable_resources
            if missing:
                problems.append(
                    f"{step_state_name!r}'s Catch (-> {catcher['Next']!r}) never reaches "
                    f"compensation resource(s) {sorted(missing)} for already-succeeded step(s) "
                    f"before it."
                )

    return problems


def _resources_reachable_from(start: str, states: dict, _visited: set[str] | None = None) -> set[str]:
    if _visited is None:
        _visited = set()
    if start in _visited or start not in states:
        return set()
    _visited.add(start)
    state = states[start]
    found: set[str] = set()
    if state.get("Type") == "Task" and "Resource" in state:
        found.add(resource_name(state["Resource"]))
    if state.get("Type") == "Parallel":
        for branch in state.get("Branches", []):
            found |= _resources_reachable_from(branch["StartAt"], branch["States"], _visited=set())
    for target in _local_next_targets(state):
        found |= _resources_reachable_from(target, states, _visited)
    return found


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: validate_asl.py <path-to-asl.json>", file=sys.stderr)
        return 2
    definition = json.loads(Path(argv[1]).read_text())
    problems = validate(definition)
    if problems:
        print(f"{len(problems)} problem(s) found in {argv[1]}:")
        for p in problems:
            print(f"  - {p}")
        return 1
    print(f"{argv[1]}: OK ({len(_iter_all_states(definition))} states checked)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
