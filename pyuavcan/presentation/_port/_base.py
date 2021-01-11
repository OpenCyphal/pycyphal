# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import abc
import typing
import pyuavcan.util
import pyuavcan.dsdl
import pyuavcan.transport


DEFAULT_PRIORITY = pyuavcan.transport.Priority.NOMINAL
"""
This value is not mandated by Specification, it is an implementation detail.
"""

DEFAULT_SERVICE_REQUEST_TIMEOUT = 1.0
"""
This value is recommended by Specification.
"""

PortFinalizer = typing.Callable[[typing.Sequence[pyuavcan.transport.Session]], None]


TypeClass = typing.TypeVar("TypeClass", bound=pyuavcan.dsdl.CompositeObject)
MessageClass = typing.TypeVar("MessageClass", bound=pyuavcan.dsdl.CompositeObject)
ServiceClass = typing.TypeVar("ServiceClass", bound=pyuavcan.dsdl.ServiceObject)


class OutgoingTransferIDCounter:
    """
    A member of the output transfer-ID map. Essentially this is just a boxed integer.
    The value is monotonically increasing starting from zero;
    transport-specific modulus is computed by the underlying transport(s).
    """

    def __init__(self) -> None:
        """
        Initializes the counter to zero.
        """
        self._value: int = 0

    def get_then_increment(self) -> int:
        """
        Samples the counter with post-increment; i.e., like ``i++``.
        """
        out = self._value
        self._value += 1
        return out

    def override(self, value: int) -> None:
        """
        Assigns a new value. Raises a :class:`ValueError` if the value is not a non-negative integer.
        """
        value = int(value)
        if value >= 0:
            self._value = value
        else:
            raise ValueError(f"Not a valid transfer-ID value: {value}")

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, self._value)


class Closable(abc.ABC):
    """
    Base class for closable session resources.
    """

    @abc.abstractmethod
    def close(self) -> None:
        """
        Invalidates the object and closes the underlying resources if necessary.

        If the closed object had a blocked task waiting for data, the task will raise a
        :class:`pyuavcan.presentation.PortClosedError` shortly after close;
        or, if the task was started by the closed instance itself, it will be silently cancelled.
        At the moment the library provides no guarantees regarding how quickly the exception will be raised
        or the task cancelled; it is only guaranteed that it will happen automatically eventually, the
        application need not be involved in that.
        """
        raise NotImplementedError


class Port(Closable, typing.Generic[TypeClass]):
    """
    The base class for any presentation layer session such as publisher, subscriber, client, or server.
    The term "port" came to be from <https://forum.uavcan.org/t/a-generic-term-for-either-subject-or-service/182>.
    """

    @property
    @abc.abstractmethod
    def dtype(self) -> typing.Type[TypeClass]:
        """
        The generated Python class modeling the corresponding DSDL data type.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def port_id(self) -> int:
        """
        The immutable subject-/service-ID of the underlying transport session instance.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def __repr__(self) -> str:
        raise NotImplementedError


# noinspection DuplicatedCode
class MessagePort(Port[MessageClass]):
    """
    The base class for publishers and subscribers.
    """

    @property
    @abc.abstractmethod
    def transport_session(self) -> pyuavcan.transport.Session:
        """
        The underlying transport session instance. Input for subscribers, output for publishers.
        One instance per session specifier.
        """
        raise NotImplementedError

    @property
    def port_id(self) -> int:
        ds = self.transport_session.specifier.data_specifier
        assert isinstance(ds, pyuavcan.transport.MessageDataSpecifier)
        return ds.subject_id

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(
            self, dtype=str(pyuavcan.dsdl.get_model(self.dtype)), transport_session=self.transport_session
        )


# noinspection DuplicatedCode
class ServicePort(Port[ServiceClass]):
    @property
    @abc.abstractmethod
    def input_transport_session(self) -> pyuavcan.transport.InputSession:
        """
        The underlying transport session instance used for the input transfers
        (requests for servers, responses for clients). One instance per session specifier.
        """
        raise NotImplementedError

    @property
    def port_id(self) -> int:
        ds = self.input_transport_session.specifier.data_specifier
        assert isinstance(ds, pyuavcan.transport.ServiceDataSpecifier)
        return ds.service_id

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(
            self, dtype=str(pyuavcan.dsdl.get_model(self.dtype)), input_transport_session=self.input_transport_session
        )
