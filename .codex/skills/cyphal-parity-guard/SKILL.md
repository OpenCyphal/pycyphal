---
name: cyphal-parity-guard
description: Keep the Python Cyphal rewrite in wire-visible behavioral parity with the C reference at `reference/cy`. Use when auditing/reviewing parity drift, identifying wire/state-machine discrepancies, updating `src/pycyphal2/` to match reference behavior, replacing conflicting Python tests with C-parity expectations, and adding regression tests for every discovered divergence. API-level discrepancies are by design and are to be ignored; this skill focuses on wire-visible and state-machine behavior only.
---

# Cyphal Parity Guard

## Overview

Run a deterministic parity workflow for `pycyphal2` against `reference/cy` in two modes:
- `sync` mode: identify divergences, patch Python implementation, and add/adjust regression tests.
- `review` mode: report parity findings only, no edits.

Apply the following defaults unless the user overrides them:
- Target wire+state parity with `cy.c`.
- Treat `cy.c` behavior as source of truth when Python tests conflict.
- Add Python regression coverage for each confirmed divergence.
- Ignore API-level discrepancies that do not affect wire/state behavior (e.g., differences in API design, error handling style, etc).

## Mode Selection

Select mode from user intent:
- Use `review` mode when asked to "review", "audit", or "find discrepancies".
- Use `sync` mode when asked to "fix", "update", "bring in sync", or "correct divergences".
- If intent is ambiguous, start in `review` mode and then switch to `sync` when requested.

## Source-of-Truth Order

Use this precedence:
1. `reference/cy/cy/cy.h` for constants/API semantics.
2. `reference/cy/cy/cy.c` for wire-visible and state-machine behavior.
3. `reference/cy/model/` when C code intent is ambiguous.
4. `src/pycyphal2/` and existing tests as implementation artifacts, not normative authority.

## Workflow

1. Prepare context.
- Confirm repository root.
- Inspect touched files and current test baseline.
- Load `references/parity-checklist.md` and use it as the audit checklist.

2. Build a discrepancy matrix.
- Compare `reference/cy` behavior with `src/pycyphal2/_node.py`, `_wire.py`, and related modules.
- Ignore differences that are not visible on the wire or in state machines (e.g., differences in API design, error handling style, etc).
- Keep in mind that error handling differs significantly between C and Python; therefore, certain error-path-related
  discrepancies may be expected and should be noted as such in the matrix (e.g., where C would clamp invalid
  arguments, Python should raise ValueError, etc).
  Error handling must be Pythonic first of all.
- For each discrepancy, record:
  - C anchor (`file:line` + behavior statement).
  - Python anchor (`file:line` + divergent behavior).
  - Impact and severity.
  - Needed test coverage.

3. Execute mode-specific actions.
- In `review` mode:
  - Produce findings ordered by severity.
  - Include exact file/line anchors and missing regression tests.
  - Do not edit code.
- In `sync` mode:
  - Implement fixes in `src/pycyphal2/`.
  - Update/remove conflicting test expectations when they contradict `cy.c`.
  - Add at least one regression test per divergence under `tests/`.

4. Validate.
- Run targeted tests first for changed behavior.
- Run full quality gates when feasible:
  - `nox -s test-3.12`
  - `nox -s mypy`
  - `nox -s format`
- If full matrix is requested or practical, also run `test-3.11` and `test-3.13`.

5. Report.
- Always return the discrepancy matrix (resolved or unresolved).
- For `sync` mode, map every fixed divergence to specific tests.
- Call out residual risks if any discrepancy remains untested.

## Repository Constraints

Enforce project constraints while implementing parity fixes:
- Preserve behavior across GNU/Linux, Windows, and macOS.
- Keep support for all declared Python versions in `pyproject.toml` (currently `>=3.11`).
- Keep async I/O in `async`/`await` style and maintain strict typing.
- Keep formatting Black-compatible with line length 120.
- Keep logging rich and appropriately leveled for unusual/error paths.

## Output Contract

For parity reviews, return:
- Findings first, ordered high to low severity.
- File/line references for C and Python anchors.
- Explicit statement when no discrepancies are found.
- Testing gaps and confidence level.

For parity sync work, return:
- What changed in implementation.
- What changed in tests and which divergences they cover.
- Commands executed and notable pass/fail outcomes.

## Reference Map

- `references/parity-checklist.md`: hotspot checklist, anchor patterns, and discrepancy matrix template.
