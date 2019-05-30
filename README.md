UAVCAN stack in Python
======================

[![Travis CI](https://travis-ci.org/UAVCAN/pyuavcan.svg?branch=master)](https://travis-ci.org/UAVCAN/pyuavcan)
[![Coverage Status](https://coveralls.io/repos/github/UAVCAN/pyuavcan/badge.svg)](https://coveralls.io/github/UAVCAN/pyuavcan)
[![Forum](https://img.shields.io/discourse/https/forum.uavcan.org/users.svg)](https://forum.uavcan.org)

Python implementation of the [UAVCAN protocol stack](https://uavcan.org).

UAVCAN is an open lightweight protocol designed for reliable intravehicular communication in
aerospace and robotic applications over robust networks such as CAN bus or Ethernet.

TODO: the documentation is missing, please come back later.

## Development

### Semantic naming conventions

API functions and methods that contain parameters of the following types should adhere to
the semantic naming conventions:

 Type                                       | Name          | Notes
--------------------------------------------|---------------|----------------------------------------------------------
`pydsdl.*Type`                              | `model`       | PyDSDL type model (descriptor)
`pyuavcan.dsdl.*Object`                     | `obj`         | Instance of a generated class implementing a DSDL type
`typing.Type[pyuavcan.dsdl.*Object]`        | `cls`         | Generated class implementing a DSDL type

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
