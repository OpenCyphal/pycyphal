Legacy UAVCAN/CAN v0 in Python
==============================

***LEGACY -- DO NOT USE IN NEW DESIGNS***

This is a **legacy** Python implementation of UAVCAN/CAN v0.
Read the docs at [legacy.uavcan.org/Implementations/Pyuavcan](http://legacy.uavcan.org/Implementations/Pyuavcan).

New applications should adopt PyUAVCAN v1 instead; please consult with [uavcan.org](https://uavcan.org) for details.

## Installation

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
