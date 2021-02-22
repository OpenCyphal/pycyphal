Full-featured UAVCAN stack in Python
====================================

[![Build status](https://ci.appveyor.com/api/projects/status/2vv83afj3dxqibi5/branch/master?svg=true)](https://ci.appveyor.com/project/Zubax/pyuavcan/branch/master)
[![RTFD](https://readthedocs.org/projects/pyuavcan/badge/)](https://pyuavcan.readthedocs.io/)
[![Coverage Status](https://coveralls.io/repos/github/UAVCAN/pyuavcan/badge.svg)](https://coveralls.io/github/UAVCAN/pyuavcan)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=UAVCAN_pyuavcan&metric=alert_status)](https://sonarcloud.io/dashboard?id=UAVCAN_pyuavcan)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=UAVCAN_pyuavcan&metric=reliability_rating)](https://sonarcloud.io/dashboard?id=UAVCAN_pyuavcan)
[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=UAVCAN_pyuavcan&metric=ncloc)](https://sonarcloud.io/dashboard?id=UAVCAN_pyuavcan)
[![PyPI - Version](https://img.shields.io/pypi/v/pyuavcan.svg)](https://pypi.org/project/pyuavcan/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Forum](https://img.shields.io/discourse/https/forum.uavcan.org/users.svg)](https://forum.uavcan.org)

PyUAVCAN is a full-featured implementation of the UAVCAN protocol stack intended for non-embedded,
user-facing applications such as GUI software, diagnostic tools, automation scripts, prototypes, and various R&D cases.

PyUAVCAN aims to support all features and transport layers of UAVCAN,
be portable across all major platforms supporting Python,
and be extensible to permit low-effort experimentation and testing of new protocol capabilities.

It is designed to support **GNU/Linux**, **MS Windows**, and **macOS** as first-class target platforms.
However, the library does not rely on any platform-specific capabilities,
so it should be usable with other systems as well.

[UAVCAN](https://uavcan.org) is an open technology for real-time intravehicular distributed computing
and communication based on modern networking standards (Ethernet, CAN FD, etc.).
The acronym *UAVCAN* stands for ***Uncomplicated Application-level Vehicular Computing And Networking***.

<p align="center">
  <a href="https://pyuavcan.readthedocs.io/"><img src="/docs/static/arch-non-redundant.svg" width="400px"></a>
</p>

**READ THE DOCS: [pyuavcan.readthedocs.io](https://pyuavcan.readthedocs.io/)**

**Ask questions: [forum.uavcan.org](https://forum.uavcan.org/)**

*See also: [**Yakut**](https://github.com/UAVCAN/yakut) -- a CLI tool for diagnostics and management of
UAVCAN networks built on top of PyUAVCAN.*
