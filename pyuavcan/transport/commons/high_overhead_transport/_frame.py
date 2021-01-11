# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import dataclasses
import pyuavcan


@dataclasses.dataclass(frozen=True)
class Frame:
    """
    The base class of a high-overhead-transport frame.
    It is used with the common transport algorithms defined in this module.
    Concrete transport implementations should make their transport-specific frame dataclasses inherit from this class.
    Derived types are recommended to not override ``__repr__()``.
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
        if not isinstance(self.priority, pyuavcan.transport.Priority):
            raise TypeError(f"Invalid priority: {self.priority}")

        if self.transfer_id < 0:
            raise ValueError(f"Invalid transfer-ID: {self.transfer_id}")

        if self.index < 0:
            raise ValueError(f"Invalid frame index: {self.index}")

        if not isinstance(self.end_of_transfer, bool):
            raise TypeError(f"Bad end of transfer flag: {type(self.end_of_transfer).__name__}")

        if not isinstance(self.payload, memoryview):
            raise TypeError(f"Bad payload type: {type(self.payload).__name__}")

    @property
    def single_frame_transfer(self) -> bool:
        return self.index == 0 and self.end_of_transfer

    def __repr__(self) -> str:
        """
        If the payload is unreasonably long for a sensible string representation,
        it is truncated and suffixed with an ellipsis.
        """
        payload_length_limit = 100
        if len(self.payload) > payload_length_limit:
            payload = bytes(self.payload[:payload_length_limit]).hex() + "..."
        else:
            payload = bytes(self.payload).hex()
        kwargs = {f.name: getattr(self, f.name) for f in dataclasses.fields(self)}
        kwargs["priority"] = self.priority.name
        kwargs["payload"] = payload
        return pyuavcan.util.repr_attributes(self, **kwargs)


# noinspection PyTypeChecker
def _unittest_frame_base_ctor() -> None:
    from pytest import raises
    from pyuavcan.transport import Priority

    Frame(priority=Priority.LOW, transfer_id=1234, index=321, end_of_transfer=True, payload=memoryview(b""))

    with raises(TypeError):
        Frame(priority=2, transfer_id=1234, index=321, end_of_transfer=True, payload=memoryview(b""))  # type: ignore

    with raises(TypeError):
        Frame(
            priority=Priority.LOW,
            transfer_id=1234,
            index=321,
            end_of_transfer=1,  # type: ignore
            payload=memoryview(b""),
        )

    with raises(TypeError):
        Frame(priority=Priority.LOW, transfer_id=1234, index=321, end_of_transfer=False, payload=b"")  # type: ignore

    with raises(ValueError):
        Frame(priority=Priority.LOW, transfer_id=-1, index=321, end_of_transfer=True, payload=memoryview(b""))

    with raises(ValueError):
        Frame(priority=Priority.LOW, transfer_id=0, index=-1, end_of_transfer=True, payload=memoryview(b""))
