# Instructions for AI agents

This is a Python implementation of the Cyphal decentralized real-time publish-subscribe protocol.
The key design goals are **simplicity** and **robustness**.
Avoid overengineering and complexity; prefer straightforward solutions and explicit code.

All features of the library MUST work on GNU/Linux, Windows, and macOS; the CI system must ensure that.
Supported Python versions are starting from the oldest version specified in `pyproject.toml` up to the current
latest stable Python.

Rely on the Python type system as much as possible and avoid dynamic typing mechanisms;
for example, always use type annotations, prefer dataclasses over dicts, etc.

To get a better feel of the problem domain, peruse `reference/cy`,
especially the formal models and the reference implementation in C.

## Architecture and code layout

Source is in `src/pycyphal2/`, tests in `tests/`. The package is extremely compact by design and has very few modules.

Concrete transports are in top-level submodules:
- `pycyphal2.udp` — Cyphal/UDP transport implementation.
- `pycyphal2.can` (coming soon, not yet in the codebase) — Cyphal/CAN transport implementation.

The core must be dependency-free.
Transports may introduce (optional) dependencies that MUST be kept to the bare minimum.

Data inputs from the wire are not guaranted to be well-formed and are not trusted;
as such, incorrect wire inputs must never trigger exceptions.
The correct handling of malformed inputs is to silently drop and debug-log.

Internal implementation modules use leading underscores.
Keep public symbols explicit through `__init__.py`; keep private helpers in underscore-prefixed modules.
The application is expected to `import pycyphal2` only, without reaching out for any submodules directly;
one exception applies to the transport modules mentioned above because the application will only import the transports
that it needs.

Since the entirety of the library API is explicitly exposed through `pycyphal2/__init__.py`,
internally the library is free to use public visibility for all symbols/members that may require shared access
between modules, even if they are not intended for external use.

DO NOT access protected members externally. If you need access, make the required members public.
Remember this does not contaminate the API in this design.

All I/O is async/await (pytest-asyncio with `asyncio_mode="auto"`).
The code is fully type-annotated; frozen dataclasses for data.

Formatting follows PEP8, enforced using Black, line-length=120.

Read `noxfile.py` to understand the project infrastructure.

## Reference design

`reference/` contains git submodules with the reference implementations in C of the session layer (`cy/`)
and transport layers (like `libudpard/` etc).
These serve as the ultimate source of truth shall any wire-visible discrepancies be found.
If there is a divergence between the references and this Python library, assume this Python library to be wrong.
Non-wire-visible differences in API design, error handling style, and similar are intentional and are due to the
differences between C and Python.

For parity audits or sync work against the reference, use the repo-local skill `$cyphal-parity-guard`.
Expected usage patterns:
- Review-only audit: Use `$cyphal-parity-guard` to review parity vs reference and report discrepancies.
- Sync/fix pass: Use `$cyphal-parity-guard` to bring implementation in sync with the reference and add regression tests for every divergence fixed.

### Intentional deviations from the reference that must be ignored

- Topic name strings are whitespace-stripped, while the reference implementation does not do that at the time of
  writing. This behavior may be introduced in the reference as well at a later stage.
- Additional minor intentional deviations may be documented directly in the codebase.
  Such intentional deviations should be marked with `REFERENCE PARITY` comments in the code.

## Documentation

The documentation must be concise and to the point, with a strong focus on "how to use" rather than "how it works".
Assume the reader to be short on time, impatient, and looking for quick answers.
Prefer examples over long prose.

When changing code, ALWAYS ensure that the documentation is updated accordingly.

## Testing

Mock transport/network in `tests/conftest.py`.
Tests are x10+ the size of source code and must provide full coverage of the core.
Transport test coverage is more opportunistic.

The library must ONLY be tested with Python versions starting from the minimum specified in `pyproject.toml`
up to the current latest stable Python.
TESTING ON UNSUPPORTED VERSIONS IS NOT ALLOWED.

ACCEPTANCE CRITERIA: Work will not be accepted unless `nox` (without arguments) runs successfully.

When starting work on a new feature, it is best to clean up temporary files using `nox -s clean`.

## Logging

Logging is required throughout the codebase; prefer many short messages. Avoid adding logging statements on code
paths that immediately raise/enqueue/schedule an error as they are often redundant.
Follow `getLogger(__name__)` convention.
Logging policy:

- DEBUG for super detailed traces. Each DEBUG logging statement must occupy at most one line of code.
  Use abbreviations and formatting helpers.
- INFO for anything not on the hot data path. Each INFO logging statement should take at most 2 lines of code.
- WARNING for anything unusual. No LoC restriction.
- ERROR for errors or anything unexpected. No LoC restriction.
- CRITICAL for fatal or high-severity errors. No LoC restriction.
