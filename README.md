UAVCAN stack in Python
======================

[![Travis CI](https://travis-ci.org/UAVCAN/pyuavcan.svg?branch=master)](https://travis-ci.org/UAVCAN/pyuavcan)
[![PyPi](https://img.shields.io/pypi/dm/uavcan.svg)](https://pypi.python.org/pypi/uavcan)
[![Gitter](https://img.shields.io/badge/gitter-join%20chat-green.svg)](https://gitter.im/UAVCAN/general)

Python implementation of the [UAVCAN protocol stack](http://uavcan.org).

UAVCAN is a lightweight protocol designed for reliable communication in aerospace and robotic applications via CAN bus.

## Documentation

* [UAVCAN website](http://uavcan.org)
* [UAVCAN discussion group](https://groups.google.com/forum/#!forum/uavcan)
* [Pyuavcan overview](http://uavcan.org/Implementations/Pyuavcan/)
* [Pyuavcan tutorials](http://uavcan.org/Implementations/Pyuavcan/Tutorials/)

## Installation

Compatible Python versions are 2.7 and 3.3 and newer.
If the library is used with Python 3, which is recommended, it does not require any additional dependencies.
If Python 2.7 is used, additional dependencies are needed - refer to `setup.py` for more info.

```bash
pip install uavcan
```

## Development

### Automatic deployment to PyPI

In order to deploy to PyPI via CI, do this:

1. Update the version number in `version.py`, e.g. `1.0.0`, and commit before proceeding.
2. Create a new tag with the same version number, e.g. `git tag -a 1.0.0 -m "My release 1.0.0"`
3. Push to master.

### Code style

Please follow the [Zubax Python Coding Conventions](https://kb.zubax.com/x/_oAh).
