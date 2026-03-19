# Instructions for AI agents

This is a Python implementation of the Cyphal decentralized real-time publish-subscribe protocol. The key design goals are **simplicity** and **robustness**.

All features of the library MUST work on GNU/Linux, Windows, and macOS; the CI system must ensure that. Supported Python versions are starting from the oldest version specified in `pyproject.toml` up to the current latest stable Python.

To get a better feel of the problem domain, peruse `reference/cy`, especially the formal models and the reference implementation in C.

## Code Layout

Source is in `src/pycyphal/`, tests in `tests/`. The package is extremely compact by design and has very few modules:

- `_common.py` — Exception hierarchy, utilities, etc.
- `_transport.py` — Abstract `Transport` interface defining subject broadcast and unicast operations. `SubjectWriter` interface. `TransportArrival` dataclass.
- `_node.py` — Core `Node` implementation. Manages CRDT, gossip protocol, message routing, etc — all main functions of the protocol.
- `_wire.py` — Wire protocol: message headers, subject ID computation, CRDT timestamp reconciliation.
- `__init__.py` — Public API re-exports from the above modules.
- Concrete transports:
    - `udp.py` — Cyphal/UDP transport implementation.

Internal implementation modules use leading underscores. Keep public symbols explicit through `__init__.py`; keep private helpers in underscore-prefixed modules.

`reference/` contains git submodules with the reference implementations in C of the session layer (`cy/`) and Cyphal/UDP transport layer (`libudpard/`). These serve as the ultimate source of truth shall any wire-visible discrepancies be found.

## Architecture

The stack is layered: **Transport** (abstract I/O) → **Wire** (framing/headers) → **Node** (session logic). `Node` is the main user-facing class that ties everything together.

Key mechanisms in `Node`:
- **CRDT**: Management of the distributed topic allocation table. Topics map to subject IDs via rapidhash; collisions are resolved by deterministic eviction.
- **Gossip**: Periodic and urgent data exchange to keep CRDT consistent.
- **Deduplication, Reordering, Reliable delivery** of user messages.

## Conventions

- **Formatting**: PEP8, Black, line-length=120.
- **Async**: All I/O is async/await (pytest-asyncio with `asyncio_mode="auto"`).
- **Types**: Fully type-annotated; frozen dataclasses for data; `__slots__` for performance.
- **Dependencies**: Intentionally kept to the bare minimum.
- **Testing**: Mock transport/network in `tests/conftest.py`; tests are ~10x the size of source code.
- **Logging**: Rich and extensive logging is required throughout the codebase:
  - DEBUG for super detailed traces;
  - INFO for anything not on the hot data path;
  - WARNING for anything unusual;
  - ERROR for errors or anything unexpected;
  - CRITICAL for fatal or high-severity errors.
- For agent-authored commits, set `GIT_AUTHOR_NAME="Agent"` and `GIT_COMMITTER_NAME="Agent"`.
