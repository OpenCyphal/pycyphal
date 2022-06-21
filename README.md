Full-featured Cyphal stack in Python
====================================

[![Build status](https://ci.appveyor.com/api/projects/status/2vv83afj3dxqibi5/branch/master?svg=true)](https://ci.appveyor.com/project/Zubax/pycyphal/branch/master) [![RTFD](https://readthedocs.org/projects/pycyphal/badge/)](https://pycyphal.readthedocs.io/) [![Coverage Status](https://coveralls.io/repos/github/OpenCyphal/pycyphal/badge.svg)](https://coveralls.io/github/OpenCyphal/pycyphal) [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=PyCyphal&metric=alert_status)](https://sonarcloud.io/dashboard?id=PyCyphal) [![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=PyCyphal&metric=reliability_rating)](https://sonarcloud.io/dashboard?id=PyCyphal) [![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=PyCyphal&metric=ncloc)](https://sonarcloud.io/dashboard?id=PyCyphal) [![PyPI - Version](https://img.shields.io/pypi/v/pycyphal.svg)](https://pypi.org/project/pycyphal/) [![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black) [![Forum](https://img.shields.io/discourse/https/forum.opencyphal.org/users.svg)](https://forum.opencyphal.org)

PyCyphal is a full-featured implementation of the Cyphal protocol stack intended for non-embedded, user-facing applications such as GUI software, diagnostic tools, automation scripts, prototypes, and various R&D cases.

PyCyphal aims to support all features and transport layers of Cyphal, be portable across all major platforms supporting Python, and be extensible to permit low-effort experimentation and testing of new protocol capabilities.

It is designed to support **GNU/Linux**, **MS Windows**, and **macOS** as first-class target platforms. However, the library does not rely on any platform-specific capabilities, so it should be usable with other systems as well.

[Cyphal](https://opencyphal.org) is an open technology for real-time intravehicular distributed computing and communication based on modern networking standards (Ethernet, CAN FD, etc.).

<p align="center">
  <a href="https://pycyphal.readthedocs.io/"><img src="docs/figures/arch-non-redundant.svg" width="400px"></a>
</p>

**READ THE DOCS: [pycyphal.readthedocs.io](https://pycyphal.readthedocs.io/)**

**Ask questions: [forum.opencyphal.org](https://forum.opencyphal.org/)**

*See also: [**Yakut**](https://github.com/OpenCyphal/yakut) -- a CLI tool for diagnostics and management of Cyphal networks built on top of PyCyphal.*
