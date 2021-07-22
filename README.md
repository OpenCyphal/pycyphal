Legacy UAVCAN/CAN v0 in Python
==============================

This is the (legacy) implementation of UAVCAN/CAN v0 in Python. It is maintained under the `pyuavcan_v0` package name
to prevent conflict with the stable version of the protocol, UAVCAN v1.

This package is currently maintained for migration and backwards compatibility purposes. v0 is not recommended for
new designs; new users should adopt pyuavcan v1. Please consult with [uavcan.org](https://uavcan.org) for details.

## Installation

```bash
pip install pyuavcan_v0
```

## Development

### Automatic deployment to PyPI

In order to deploy to PyPI via CI, do this:

1. Update the version number in `version.py`, e.g. `1.0.0`, and commit before proceeding.
2. Create a new tag with the same version number, e.g. `git tag -a 1.0.0 -m "My release 1.0.0"`
3. Push to master.

### Code style

Please follow the [Zubax Python Coding Conventions](https://kb.zubax.com/x/_oAh).
