Full-featured UAVCAN stack in Python
====================================

[![Travis CI](https://travis-ci.org/UAVCAN/pyuavcan.svg?branch=uavcan-v1.0)](https://travis-ci.org/UAVCAN/pyuavcan)
[![Coverage Status](https://coveralls.io/repos/github/UAVCAN/pyuavcan/badge.svg?branch=uavcan-v1.0)](https://coveralls.io/github/UAVCAN/pyuavcan)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=UAVCAN_pyuavcan&metric=alert_status)](https://sonarcloud.io/dashboard?id=UAVCAN_pyuavcan)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=UAVCAN_pyuavcan&metric=reliability_rating)](https://sonarcloud.io/dashboard?id=UAVCAN_pyuavcan)
[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=UAVCAN_pyuavcan&metric=ncloc)](https://sonarcloud.io/dashboard?id=UAVCAN_pyuavcan)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pyuavcan.svg)](https://pypi.org/project/pyuavcan/)
[![Forum](https://img.shields.io/discourse/https/forum.uavcan.org/users.svg)](https://forum.uavcan.org)

This mono-repository contains PyUAVCAN --
a full-featured implementation of the [UAVCAN protocol stack](https://uavcan.org) in Python --
and related entities.

UAVCAN is an open lightweight data bus standard designed for reliable intravehicular communication
in aerospace and robotic applications via CAN bus, Ethernet, and other robust transports.
The acronym stands for *Uncomplicated Application-level Vehicular Communication And Networking*.

If you have questions, please bring them to the [**UAVCAN support forum**](https://forum.uavcan.org/).

## Project structure

This is a mono-repository with the sources of multiple Python packages inside.
Please find package-specified details inside their respective subdirectories.

There are common top-level maintenance scripts that apply bulk actions to all packages;
please read their sources for more information.

## FAQ

**Q:** PyUAVCAN seems complex. Does that mean that UAVCAN is a complex protocol?
**A:** UAVCAN is a very simple protocol. This particular implementation may appear convoluted because it is very
generic and provides a very high-level API. For comparison, there is a full-featured UAVCAN-over-CAN
implementation in C99 only ~1k SLoC large.

**Q:** The library or the command-line tools complain about missing packages (usually `uavcan`).
Do I need to install additional packages to get the library working?
**A:** No. The missing packages are supposed to be auto-generated from DSDL definitions.
We no longer ship the public regulated DSDL definitions together with UAVCAN implementations
in order to simplify maintenance and integration; also, this underlines our commitment to make
vendor-specific (or application-specific) data types first-class citizens in UAVCAN v1.
Please read the user documentation to learn how to generate Python packages from DSDL namespaces.

## Development

### General conventions

Avoid raising exceptions from properties whenever possible.
Generally, a property should always return its value. If the availability of the value is conditional,
consider using a getter method instead.

### Visibility

Name all entities with a leading underscore, including modules and packages,
excepting those that are part of the API.

When re-exporting entities from a package-level `__init__.py`,
always use the form `import ... as ...` even if the name is not changed,
to signal static analysis tools that the name is intended to be reexported
(unless the aliased name starts with an underscore).

### Semantic naming conventions

API functions and methods that contain the following parameters should adhere to the semantic naming conventions:

 Type                                   | Name                  | Purpose
----------------------------------------|-----------------------|----------------------------------------------------------
`pydsdl.*Type`                          | `model`               | PyDSDL type model (descriptor).
`pyuavcan.dsdl.*Object`                 | `obj`                 | Instance of a generated class implementing a DSDL type.
`typing.Type[pyuavcan.dsdl.*Object]`    | `dtype`               | Generated class implementing a DSDL type.
`float`                                 | `monotonic_deadline`  | Abort operation if not completed by this time.
`int`                                   | `node_id`             | A node identifier.

### Writing tests

Aim to cover 100% of the code in the branch coverage mode, excepting the DSDL generated packages.

Write unit tests as functions without arguments prefixed with `_unittest_`;
optionally, for slow test functions use the prefix `_unittest_slow_` (more on this below).
Generally, simple test functions should be located as close as possible to the tested code,
preferably at the end of the same Python module; exception applies to the sub-package `pyuavcan.application`,
which is unconditionally excluded from unit test discovery because it relies on DSDL autogenerated code,
meaning that if you write your unit test function in there it will never be invoked.
Complex functions that require sophisticated setup and teardown process or that can't be located near the
tested code for other reasons shall be moved into the separate test package (aptly named `tests`).
Test functions that are located inside the library are shipped together with the library,
which makes having complex testing logic inside the main codebase undesirable.

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

### Running tests and static analysis

The script `test_all.sh` can be used to run the unit tests and static code analysis tools locally for all packages.
The coverage statistics will be collected from each package,
combined into one cumulative data file spanning the entire codebase,
and stored in the project root directory.

After the tests are executed, it is possible to run the [SonarQube](https://sonarqube.org) scanner as follows:
`sonar-scanner -Dsonar.login=<project-key>` (the project key is a 40-digit long hexadecimal number).
The scanner should not be run before the general test suite since it relies on its coverage data.

### Releasing via PyPI

The script `release_all.sh` packages and pushes all packages whose version numbers have been changed to PyPI.

## License

The contained packages are available under the terms of the MIT License.
