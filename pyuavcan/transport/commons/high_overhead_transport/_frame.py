#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import dataclasses
import pyuavcan


@dataclasses.dataclass(frozen=True)
class Frame:
    """
    The base class of a high-overhead-transport frame.
    It is used with the common transport algorithms defined in this module.
    Concrete transport implementations should make their transport-specific frame dataclasses inherit from this class.
    """
    timestamp: pyuavcan.transport.Timestamp
    """
    For outgoing frames, this is the timestamp of the transfer instance.
    For incoming frames, this is the reception timestamp from the media implementation (hardware or software).
    """

    priority: pyuavcan.transport.Priority
    """
    Transfer priority should be the same for all frames within the transfer.
    """

    transfer_id: int
    """
    Transfer-ID is incremented whenever a transfer under a specific session-specifier is emitted.
    Always non-negative.
    """

    index: int
    """
    Index of the frame within its transfer, starting from zero. Always non-negative.
    """

    end_of_transfer: bool
    """
    True for the last frame within the transfer.
    """

    payload: memoryview
    """
    The data carried by the frame. Multi-frame transfer payload is suffixed with its CRC32C. May be empty.
    """

    def __post_init__(self) -> None:
        if not isinstance(self.timestamp, pyuavcan.transport.Timestamp):
            raise TypeError(f'Invalid timestamp: {self.timestamp}')

        if not isinstance(self.priority, pyuavcan.transport.Priority):
            raise TypeError(f'Invalid priority: {self.priority}')

        if self.transfer_id < 0:
            raise ValueError(f'Invalid transfer-ID: {self.transfer_id}')

        if self.index < 0:
            raise ValueError(f'Invalid frame index: {self.index}')

        if not isinstance(self.end_of_transfer, bool):
            raise TypeError(f'Bad end of transfer flag: {type(self.end_of_transfer).__name__}')

        if not isinstance(self.payload, memoryview):
            raise TypeError(f'Bad payload type: {type(self.payload).__name__}')

    @property
    def single_frame_transfer(self) -> bool:
        return self.index == 0 and self.end_of_transfer


# noinspection PyTypeChecker
def _unittest_frame_base_ctor() -> None:
    from pytest import raises
    from pyuavcan.transport import Priority, Timestamp

    Frame(timestamp=Timestamp.now(),
          priority=Priority.LOW,
          transfer_id=1234,
          index=321,
          end_of_transfer=True,
          payload=memoryview(b''))

    with raises(TypeError):
        Frame(timestamp=123456,  # type: ignore
              priority=Priority.LOW,
              transfer_id=1234,
              index=321,
              end_of_transfer=True,
              payload=memoryview(b''))

    with raises(TypeError):
        Frame(timestamp=Timestamp.now(),
              priority=2,  # type: ignore
              transfer_id=1234,
              index=321,
              end_of_transfer=True,
              payload=memoryview(b''))

    with raises(TypeError):
        Frame(timestamp=Timestamp.now(),
              priority=Priority.LOW,
              transfer_id=1234,
              index=321,
              end_of_transfer=1,  # type: ignore
              payload=memoryview(b''))

    with raises(TypeError):
        Frame(timestamp=Timestamp.now(),
              priority=Priority.LOW,
              transfer_id=1234,
              index=321,
              end_of_transfer=False,
              payload=b'')  # type: ignore

    with raises(ValueError):
        Frame(timestamp=Timestamp.now(),
              priority=Priority.LOW,
              transfer_id=-1,
              index=321,
              end_of_transfer=True,
              payload=memoryview(b''))

    with raises(ValueError):
        Frame(timestamp=Timestamp.now(),
              priority=Priority.LOW,
              transfer_id=0,
              index=-1,
              end_of_transfer=True,
              payload=memoryview(b''))
