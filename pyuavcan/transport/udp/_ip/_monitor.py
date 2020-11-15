#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import abc
import dataclasses
import pyuavcan


@dataclasses.dataclass(frozen=True)
class Endpoint:  # The IPv6 implementation may subclass this to add flow info and scope ID.
    address: str
    """
    Same format as reported by the ``socket`` module from the Python standard library; e.g., ``::1``, ``127.0.0.1``.
    """

    port: int
    """
    The UDP port number in [0, 0xFFFF].
    """


@dataclasses.dataclass(frozen=True)
class Packet:
    timestamp:   pyuavcan.transport.Timestamp
    source:      Endpoint
    destination: Endpoint
    payload:     memoryview


class Monitor(abc.ABC):
    """
    Network monitor is responsible for managing the raw socket and parsing and filtering the raw IP packets.
    """
    @abc.abstractmethod
    def close(self) -> None:
        raise NotImplementedError
