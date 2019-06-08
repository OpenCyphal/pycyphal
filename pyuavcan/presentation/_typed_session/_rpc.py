#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import abc
import typing
import pyuavcan.dsdl
import pyuavcan.transport
from ._base import TypedSession


ServiceTypeClass = typing.TypeVar('ServiceTypeClass', bound=pyuavcan.dsdl.ServiceObject)


class ServiceTypedSession(TypedSession[ServiceTypeClass]):
    @property
    @abc.abstractmethod
    def input_transport_session(self) -> pyuavcan.transport.InputSession:
        """
        The underlying transport session instance used for the input transfers (requests for servers, responses
        for clients).
        """
        raise NotImplementedError

    @property
    def port_id(self) -> int:
        ds = self.input_transport_session.specifier.data_specifier
        assert isinstance(ds, pyuavcan.transport.ServiceDataSpecifier)
        return ds.service_id

    def __repr__(self) -> str:
        return f'{type(self).__name__}(' \
            f'dtype={pyuavcan.dsdl.get_model(self.dtype)}, ' \
            f'input_transport_session={self.input_transport_session})'
