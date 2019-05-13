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
