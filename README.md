<div align="center">

<img src="https://opencyphal.org/favicon-192.png" width="60px">

<h1>Cyphal in Python</h1>

_pub/sub without steroids_

[![Website](https://img.shields.io/badge/website-opencyphal.org-black?color=1700b3)](https://opencyphal.org/)
[![Forum](https://img.shields.io/discourse/https/forum.opencyphal.org/users.svg?logo=discourse&color=1700b3)](https://forum.opencyphal.org)
[![PyPI](https://img.shields.io/pypi/v/pycyphal2.svg)](https://pypi.org/project/pycyphal2/)
[![Docs](https://img.shields.io/badge/Docs-rtfm-black?color=ff00aa&logo=readthedocs)](https://opencyphal.github.io/pycyphal)

</div>

-----

Python implementation of the [Cyphal](https://opencyphal.org) stack that runs on GNU/Linux, Windows, and macOS.

PyCyphal v2 is published on PyPI as `pycyphal2` to enable coexistence with v1 `pycyphal` in the same Python environment.
The two packages have radically different APIs but are wire-compatible on Cyphal/CAN.
The maintenance of the original `pycyphal` package will eventually cease;
existing applications leveraging `pycyphal` should upgrade to the new API of `pycyphal2`.

📚 **Read the docs** at <https://opencyphal.github.io/pycyphal>.

💡 **Runnable examples** at `examples/`.
