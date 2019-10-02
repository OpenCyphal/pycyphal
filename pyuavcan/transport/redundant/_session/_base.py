#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import abc
import typing
import logging
import dataclasses
import pyuavcan.transport


_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class RedundantSessionStatistics(pyuavcan.transport.SessionStatistics):
    """
    Aggregate statistics for all inferior sessions in a redundant group.
    """
    #: The ordering is guaranteed to match that of the inferiors.
    inferiors: typing.List[pyuavcan.transport.SessionStatistics] = dataclasses.field(default_factory=list)


class RedundantSession(abc.ABC):
    @property
    @abc.abstractmethod
    def specifier(self) -> pyuavcan.transport.OutputSessionSpecifier:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def payload_metadata(self) -> pyuavcan.transport.PayloadMetadata:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def inferiors(self) -> typing.Sequence[pyuavcan.transport.Session]:
        raise NotImplementedError

    @abc.abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def _add_inferior(self, session: pyuavcan.transport.Session) -> None:
        """
        If the new session is already an inferior, this method does nothing.
        If anything goes wrong during the initial setup, the inferior will not be added and
        an appropriate exception will be raised.

        This method is intended to be invoked by the transport class.
        The Python's type system does not allow us to concisely define module-internal APIs.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _close_inferior(self, session: pyuavcan.transport.Session) -> None:
        """
        If the session is not a registered inferior, this method does nothing.
        Removal always succeeds regardless of any exceptions raised.

        Like its counterpart, this method is supposed to be invoked by the transport class.
        """
        raise NotImplementedError
