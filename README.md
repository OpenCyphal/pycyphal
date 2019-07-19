Full-featured UAVCAN stack in Python
====================================

[![Travis CI](https://travis-ci.org/UAVCAN/pyuavcan.svg?branch=uavcan-v1.0)](https://travis-ci.org/UAVCAN/pyuavcan)
[![AppVeyor CI](https://ci.appveyor.com/api/projects/status/2vv83afj3dxqibi5?svg=true)](https://ci.appveyor.com/project/Zubax/pyuavcan)
[![RTFD](https://readthedocs.org/projects/pyuavcan/badge/)](https://pyuavcan.readthedocs.io/)
[![Coverage Status](https://coveralls.io/repos/github/UAVCAN/pyuavcan/badge.svg?branch=uavcan-v1.0)](https://coveralls.io/github/UAVCAN/pyuavcan)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=UAVCAN_pyuavcan&metric=alert_status)](https://sonarcloud.io/dashboard?id=UAVCAN_pyuavcan)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=UAVCAN_pyuavcan&metric=reliability_rating)](https://sonarcloud.io/dashboard?id=UAVCAN_pyuavcan)
[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=UAVCAN_pyuavcan&metric=ncloc)](https://sonarcloud.io/dashboard?id=UAVCAN_pyuavcan)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pyuavcan.svg)](https://pypi.org/project/pyuavcan/)
[![Forum](https://img.shields.io/discourse/https/forum.uavcan.org/users.svg)](https://forum.uavcan.org)

PyUAVCAN is a full-featured implementation of the UAVCAN protocol stack in Python.

UAVCAN is an open lightweight data bus standard designed for reliable intravehicular communication
in aerospace and robotic applications via CAN bus, Ethernet, and other robust transports.
The acronym stands for *Uncomplicated Application-level Vehicular Communication And Networking*.

- **PYUAVCAN DOCS: [pyuavcan.readthedocs.io](https://pyuavcan.readthedocs.io/)**
- **SUPPORT FORUM: [forum.uavcan.org](https://forum.uavcan.org/)**
- **UAVCAN WEBSITE: [uavcan.org](https://uavcan.org)**

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

TODO: coverage measurement with sitecustomize.py -- quite a big gun

### Repository layout

The shippable entities are located exclusively inside the directory `pyuavcan`.
The entirety of the directory, excluding hidden files, if any, is shipped.
Everything outside of that is auxiliary and is never shipped.

The submodule `tests/public_regulated_data_types` is needed only for testing and documentation building.
It should be kept reasonably up-to-date, but remember that it does not affect the final product in any way.
We no longer ship DSDL namespaces with code for reasons explained in the documentation.

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

### Automation

The script `test.sh` can be used to run the unit tests and static code analysis tools locally or on a CI server.

After the tests are executed, it is possible to run the [SonarQube](https://sonarqube.org) scanner as follows:
`sonar-scanner -Dsonar.login=<project-key>` (the project key is a 40-digit long hexadecimal number).
The scanner should not be run before the general test suite since it relies on its coverage data.

The script `release.sh` runs the test and then, if successful, uploads the package onto PyPI.
