Full-featured UAVCAN stack in Python
====================================

[![Travis CI](https://travis-ci.org/UAVCAN/pyuavcan.svg?branch=master)](https://travis-ci.org/UAVCAN/pyuavcan)
[![Coverage Status](https://coveralls.io/repos/github/UAVCAN/pyuavcan/badge.svg)](https://coveralls.io/github/UAVCAN/pyuavcan)
[![Forum](https://img.shields.io/discourse/https/forum.uavcan.org/users.svg)](https://forum.uavcan.org)

PyUAVCAN is a full-featured implementation of the [UAVCAN protocol stack](https://uavcan.org) in Python.
PyUAVCAN aims to support all features and transport layers of UAVCAN,
be portable across all major platforms supporting Python, and
be extensible to permit low-effort experimentation and testing of new protocol capabilities.

UAVCAN is an open lightweight data bus standard designed for reliable intravehicular communication
in aerospace and robotic applications via CAN bus, Ethernet, and other robust transports.

If you have questions, please bring them to the [UAVCAN support forum](https://forum.uavcan.org/).

## Installation

Install from PIP: `pip install pyuavcan`.

Note that a similar library titled `uavcan` is also available from PIP,
which implements an early experimental version of the protocol known as UAVCAN v0
that is no longer recommended for new designs.
It should not be confused with this library (titled `pyuavcan`) which implements the
long-term stable version of the protocol known as UAVCAN v1.0.

## Usage

The library is currently under heavy development, and as such, the usage documentation is not yet available.
Please come back later.
If you are willing to help, please join the
[UAVCAN Development & Maintenance forum](https://forum.uavcan.org/c/dev) for coordination.

## Development

### Semantic naming conventions

API functions and methods that contain the following parameters should adhere to the semantic naming conventions:

 Type                                   | Name                  | Purpose
----------------------------------------|-----------------------|----------------------------------------------------------
`pydsdl.*Type`                          | `model`               | PyDSDL type model (descriptor).
`pyuavcan.dsdl.*Object`                 | `obj`                 | Instance of a generated class implementing a DSDL type.
`typing.Type[pyuavcan.dsdl.*Object]`    | `cls`                 | Generated class implementing a DSDL type.
`float`                                 | `monotonic_deadline`  | The operation shall be aborted if not completed by this time.
`int`                                   | `node_id`             | A node identifier.

### Writing tests

Aim to cover 100% of the code in the branch coverage mode, excepting the DSDL generated packages.

Write unit tests as functions without arguments prefixed with `_unittest_`;
optionally, for slow test functions use the prefix `_unittest_slow_` (more on this below).
Generally, simple test functions should be located as close as possible to the tested code,
preferably at the end of the same Python module.
Complex functions that require sophisticated setup and teardown process shall be moved into the
separate test package (aptly named `tests`).
The reason for the separation is that test functions that are located inside the library are shipped
together with the library, which makes having complex testing logic inside the main codebase undesirable.

Tests that are implemented inside the main codebase shall not use any external dependencies that are not
listed among the runtime library dependencies; for example, the library `pytest` cannot be imported
because it will break the library outside of test-enabled environments.
You can do that only in the separate test package since it's never shipped and hence does not need to work
outside of test-enabled environments.

By default, all test functions will be executed during a testing session.
Since some of them may take a considerable time to run,
the developer may want to temporarily disable slow tests by setting the environment variable
`PYUAVCAN_TEST_SKIP_SLOW=1`.
This will trigger the test executor to skip test functions whose names match the pattern `_unittest_slow_*`.

For more information refer to the PyTest documentation.

## License

The library is available under the terms of the MIT License.
