# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import abc
import typing


class CRCAlgorithm(abc.ABC):
    """
    Implementations are default-constructible.
    """

    @abc.abstractmethod
    def add(self, data: typing.Union[bytes, bytearray, memoryview]) -> None:
        """
        Updates the value with the specified block of data.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def check_residue(self) -> bool:
        """
        Checks if the current state matches the algorithm-specific residue.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def value(self) -> int:
        """
        The current CRC value, with output XOR applied, if applicable.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def value_as_bytes(self) -> bytes:
        """
        The current CRC value serialized in the algorithm-specific byte order.
        """
        raise NotImplementedError

    @classmethod
    def new(cls, *fragments: typing.Union[bytes, bytearray, memoryview]) -> CRCAlgorithm:
        """
        A factory that creates the new instance with the value computed over the fragments.
        """
        self = cls()
        for frag in fragments:
            self.add(frag)
        return self
