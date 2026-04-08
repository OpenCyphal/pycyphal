# Parity Checklist

Use this file to drive fast, repeatable parity analysis between `reference/cy` and `src/pycyphal2/`.

## High-Risk Areas

1. CRDT allocation and collision arbitration.
2. Gossip propagation, validation, scope handling, and unknown-topic behavior.
3. Implicit topic lifecycle and retirement timing.
4. Reliable publish ACK/NACK acceptance and association slack updates.
5. Deduplication and reordering interaction with reliability.
6. Response ACK/NACK and future retention semantics.
7. Header packing/unpacking and wire constants.
8. Consult with the reference implementation and formal models to identify additional high-risk areas.

## Discrepancy Matrix Template

Use one row per confirmed divergence.

| ID | Area | C Anchor | Python Anchor | Divergence | Severity | Fix Plan / Action | Regression Test |
|---|---|---|---|---|---|---|---|
| P-001 | ACK acceptance | `reference/cy/cy/cy.c:4448` | `src/pycyphal2/_node.py:...` | Describe exact behavioral mismatch | High | Adjust ACK acceptance rules and slack handling | `tests/test_pubsub.py::...` |

## Review Quality Bar

Before declaring parity, ensure:
1. Every listed high-risk area was inspected or explicitly marked not applicable.
2. Every confirmed divergence has at least one mapped regression test (existing or new).
3. Any changed expectation that conflicts with previous Python tests is resolved in favor of `cy.c`.
