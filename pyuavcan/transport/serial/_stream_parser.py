#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import pyuavcan
from ._frame import TimestampedFrame


class StreamParser:
    def __init__(self,
                 callback:               typing.Callable[[typing.Union[TimestampedFrame, memoryview]], None],
                 max_payload_size_bytes: int):
        """
        :param callback: Invoked when a new frame is parsed or when a block of data could not be recognized as a frame.
            In the case of success, an instance of the frame class is passed; otherwise, raw memoryview is passed.
            In either case, the passed memoryview instance is guaranteed to point to an immutable memory.
        :param max_payload_size_bytes: Frames containing more that this many bytes of payload (after escaping and
            not including the header and CRC) will be considered invalid.
        """
        max_payload_size_bytes = int(max_payload_size_bytes)
        if not (callable(callback) and max_payload_size_bytes > 0):
            raise ValueError('Invalid parameters')

        # Constant configuration
        self._callback = callback
        self._max_frame_size_bytes = \
            int(max_payload_size_bytes) + TimestampedFrame.NUM_OVERHEAD_BYTES_EXCEPT_DELIMITERS_AND_ESCAPING

        # Parser state
        self._frame_buffer = bytearray()  # Entire frame except delimiters.
        self._unescape_next = False
        self._current_frame_timestamp: typing.Optional[pyuavcan.transport.Timestamp] = None

    def process_next_chunk(self,
                           chunk:     typing.Union[bytes, bytearray, memoryview],
                           timestamp: pyuavcan.transport.Timestamp) -> None:
        for b in chunk:
            self._process_byte(b, timestamp)

        has_data = len(self._frame_buffer) > 0
        shall_abort = (not self._is_inside_frame()) or (len(self._frame_buffer) > self._max_frame_size_bytes)
        if has_data and shall_abort:
            self._finalize(known_invalid=True)

    def _process_byte(self, b: int, timestamp: pyuavcan.transport.Timestamp) -> None:
        # Reception of a frame delimiter terminates the current frame unconditionally.
        if b == TimestampedFrame.FRAME_DELIMITER_BYTE:
            self._finalize(known_invalid=not self._is_inside_frame())
            self._current_frame_timestamp = timestamp
            return

        # Unescaping is done only if we're inside a frame currently.
        if self._is_inside_frame():
            if b == TimestampedFrame.ESCAPE_PREFIX_BYTE:
                self._unescape_next = True
                return
            if self._unescape_next:
                self._unescape_next = False
                b ^= 0xFF

        # Appending to the buffer always, regardless of whether we're in a frame or not.
        # We may find out that the data does not belong to the protocol only much later; can't look ahead.
        self._frame_buffer.append(b)

    def _is_inside_frame(self) -> bool:
        return self._current_frame_timestamp is not None

    def _finalize(self, known_invalid: bool) -> None:
        try:
            mv = memoryview(self._frame_buffer)
            parsed: typing.Optional[TimestampedFrame] = None
            if (not known_invalid) and len(mv) <= self._max_frame_size_bytes:
                assert self._current_frame_timestamp is not None
                parsed = TimestampedFrame.parse_from_unescaped_image(mv, self._current_frame_timestamp)
            self._callback(parsed if parsed is not None else mv)
        finally:
            self._unescape_next = False
            self._current_frame_timestamp = None
            self._frame_buffer = bytearray()    # There are memoryview instances pointing to the old buffer!
