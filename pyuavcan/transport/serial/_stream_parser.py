#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import pyuavcan
from ._frame import SerialFrame


class StreamParser:
    """
    A stream parser is fed with bytes received from the channel.
    The parser maintains internal parsing state machine; whenever the machine detects that a valid frame is received,
    the callback is invoked.

    When the state machine identifies that a received block of data cannot possibly
    contain or be part of a valid frame, the raw bytes are delivered into the callback as-is for optional later
    processing; such data is called "out-of-band" (OOB) data.
    The OOB bytes are always unescaped and never contain the frame delimiter bytes; that is, the parser does not
    guarantee that OOB data (not belonging to the protocol set) is retained in its original form.
    An empty sequence of OOB bytes is never reported.
    The OOB data reporting can be useful if the same serial port is used both for UAVCAN and as a text console.
    """
    def __init__(self,
                 callback: typing.Callable[[typing.Union[SerialFrame, memoryview]], None],
                 max_payload_size_bytes: int):
        """
        :param callback: Invoked when a new frame is parsed or when a block of data could not be recognized as a frame.
            In the case of success, an instance of the frame class is passed; otherwise, raw memoryview is passed.
            In either case, the referenced memory is guaranteed to be immutable.
        :param max_payload_size_bytes: Frames containing more that this many bytes of payload (after escaping and
            not including the header and CRC) will be considered invalid.
        """
        max_payload_size_bytes = SerialFrame.calc_cobs_size(max_payload_size_bytes)
        if not (callable(callback) and max_payload_size_bytes > 0):
            raise ValueError('Invalid parameters')

        # Constant configuration
        self._callback = callback
        self._max_frame_size_bytes = \
            int(max_payload_size_bytes) + SerialFrame.NUM_OVERHEAD_BYTES_EXCEPT_DELIMITERS_AND_ESCAPING

        # Parser state
        self._frame_buffer = bytearray()  # Entire frame except delimiters.
        self._unescape_next = False
        self._current_frame_timestamp: typing.Optional[pyuavcan.transport.Timestamp] = None

    def process_next_chunk(self,
                           chunk:     typing.Union[bytes, bytearray, memoryview],
                           timestamp: pyuavcan.transport.Timestamp) -> None:
        for b in chunk:
            self._process_byte(b, timestamp)

        if (not self._is_inside_frame()) or (len(self._frame_buffer) > self._max_frame_size_bytes):
            self._finalize(known_invalid=True)

    def _process_byte(self, b: int, timestamp: pyuavcan.transport.Timestamp) -> None:
        # Reception of a frame delimiter terminates the current frame unconditionally.
        if b == SerialFrame.FRAME_DELIMITER_BYTE:
            self._finalize(known_invalid=not self._is_inside_frame())
            self._current_frame_timestamp = timestamp
            return

        # Appending to the buffer always, regardless of whether we're in a frame or not.
        # We may find out that the data does not belong to the protocol only much later; can't look ahead.
        self._frame_buffer.append(b)

    def _is_inside_frame(self) -> bool:
        return self._current_frame_timestamp is not None

    def _finalize(self, known_invalid: bool) -> None:
        try:
            mv = memoryview(self._frame_buffer)
            parsed: typing.Optional[SerialFrame] = None
            if (not known_invalid) and len(mv) <= self._max_frame_size_bytes:
                assert self._current_frame_timestamp is not None
                parsed = SerialFrame.parse_from_cobs_image(mv, self._current_frame_timestamp)
            if parsed:
                self._callback(parsed)
            elif mv:
                self._callback(mv)
            else:
                pass    # Empty - nothing to report.
        finally:
            self._unescape_next = False
            self._current_frame_timestamp = None
            self._frame_buffer = bytearray()    # There are memoryview instances pointing to the old buffer!


def _unittest_stream_parser() -> None:
    from pytest import raises
    from pyuavcan.transport import Priority, MessageDataSpecifier
    from ._frame import SerialFrame

    ts = pyuavcan.transport.Timestamp.now()

    outputs: typing.List[typing.Union[SerialFrame, memoryview]] = []

    with raises(ValueError):
        sp = StreamParser(outputs.append, 0)

    sp = StreamParser(outputs.append, 4)

    def proc(b: typing.Union[bytes, memoryview]) -> typing.Sequence[typing.Union[SerialFrame, memoryview]]:
        sp.process_next_chunk(b, ts)
        out = outputs[:]
        outputs.clear()
        return out

    assert not outputs
    assert [memoryview(b'abcdef')] == proc(b'abcdef')
    assert [] == proc(b'')

    # Valid frame.
    f1 = SerialFrame(timestamp=ts,
                     priority=Priority.HIGH,
                     source_node_id=SerialFrame.FRAME_DELIMITER_BYTE,
                     destination_node_id=SerialFrame.FRAME_DELIMITER_BYTE,
                     data_specifier=MessageDataSpecifier(2345),
                     transfer_id=1234567890123456789,
                     index=1234567,
                     end_of_transfer=True,
                     payload=memoryview(b'ab\x9E\x8E'))  # 4 bytes of payload.
    result = proc(f1.compile_into(bytearray(100)))
    assert len(result) == 1
    assert isinstance(result[0], SerialFrame)
    assert SerialFrame.__eq__(f1, result)

    # Second valid frame is too long.
    f2 = SerialFrame(timestamp=ts,
                     priority=Priority.HIGH,
                     source_node_id=SerialFrame.FRAME_DELIMITER_BYTE,
                     destination_node_id=SerialFrame.FRAME_DELIMITER_BYTE,
                     data_specifier=MessageDataSpecifier(2345),
                     transfer_id=1234567890123456789,
                     index=1234567,
                     end_of_transfer=True,
                     payload=f1.compile_into(bytearray(1000)))
    assert len(f2.payload) == 43  # Cobs escaping
    result = proc(f2.compile_into(bytearray(1000)))
    assert len(result) == 1
    assert isinstance(result[0], memoryview)

    # Create new instance with much larger frame size limit; feed both frames but let the first one be incomplete.
    sp = StreamParser(outputs.append, 10**6)
    assert [] == proc(f1.compile_into(bytearray(100))[:-2])     # First one is ended abruptly.
    result = proc(f2.compile_into(bytearray(100)))              # Then the second frame begins.
    assert len(result) == 2                                     # Make sure the second one is retrieved correctly.
    assert isinstance(result[0], memoryview)
    assert isinstance(result[1], SerialFrame)
    assert SerialFrame.__eq__(f2, result)
