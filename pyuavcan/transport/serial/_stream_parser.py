# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing
from pyuavcan.transport import Timestamp
from ._frame import SerialFrame


class StreamParser:
    """
    A stream parser is fed with bytes received from the channel.
    The parser maintains internal parsing state machine; whenever the machine detects that a valid frame is received,
    the callback is invoked.

    When the state machine identifies that a received block of data cannot possibly
    contain or be part of a valid frame, the raw bytes are delivered into the callback as-is for optional later
    processing; such data is called "out-of-band" (OOB) data. An empty sequence of OOB bytes is never reported.
    The OOB data reporting can be useful if the same serial port is used both for UAVCAN and as a text console.
    The OOB bytes may or may not be altered by the COBS decoding logic.
    """

    def __init__(
        self,
        callback: typing.Callable[[Timestamp, memoryview, typing.Optional[SerialFrame]], None],
        max_payload_size_bytes: int,
    ):
        """
        :param callback: Invoked when a new frame is parsed or when a block of data could not be recognized as a frame.
            In the case of success, an instance of the frame class is passed in the last argument, otherwise it's None.
            In either case, the raw buffer is supplied as the second argument for capture/diagnostics or OOB handling.

        :param max_payload_size_bytes: Frames containing more than this many bytes of payload
            (after escaping and not including the header, CRC, and delimiters) may be considered invalid.
            This is to shield the parser against OOM errors when subjected to an invalid stream of bytes.
        """
        if not (callable(callback) and max_payload_size_bytes > 0):
            raise ValueError("Invalid parameters")

        self._callback = callback
        self._max_frame_size_bytes = (
            SerialFrame.calc_cobs_size(
                max_payload_size_bytes + SerialFrame.NUM_OVERHEAD_BYTES_EXCEPT_DELIMITERS_AND_ESCAPING
            )
            + 2
        )
        self._buffer = bytearray()  # Entire frame including all delimiters.
        self._timestamp: typing.Optional[Timestamp] = None

    def process_next_chunk(self, chunk: typing.Union[bytes, bytearray, memoryview], timestamp: Timestamp) -> None:
        # TODO: PERFORMANCE WARNING: DECODE COBS ON THE FLY TO AVOID EXTRA COPYING
        for b in chunk:
            self._buffer.append(b)
            if b == SerialFrame.FRAME_DELIMITER_BYTE:
                self._finalize(known_invalid=self._outside_frame)
            else:
                if self._timestamp is None:
                    self._timestamp = timestamp  # https://github.com/UAVCAN/pyuavcan/issues/112

        if self._outside_frame or (len(self._buffer) > self._max_frame_size_bytes):
            self._finalize(known_invalid=True)

    @property
    def _outside_frame(self) -> bool:
        return self._timestamp is None

    def _finalize(self, known_invalid: bool) -> None:
        if not self._buffer or (len(self._buffer) == 1 and self._buffer[0] == SerialFrame.FRAME_DELIMITER_BYTE):
            # Avoid noise in the OOB output during normal operation.
            # TODO: this is a hack in place of the proper on-the-fly COBS parser.
            return

        buf = memoryview(self._buffer)
        self._buffer = bytearray()  # There are memoryview instances pointing to the old buffer!
        ts = self._timestamp or Timestamp.now()
        self._timestamp = None

        parsed: typing.Optional[SerialFrame] = None
        if (not known_invalid) and len(buf) <= self._max_frame_size_bytes:
            parsed = SerialFrame.parse_from_cobs_image(buf)

        self._callback(ts, buf, parsed)


def _unittest_stream_parser() -> None:
    from pytest import raises
    from pyuavcan.transport import Priority, MessageDataSpecifier

    ts = Timestamp.now()

    outputs: typing.List[typing.Tuple[Timestamp, memoryview, typing.Optional[SerialFrame]]] = []

    with raises(ValueError):
        sp = StreamParser(lambda *_: None, 0)

    sp = StreamParser(lambda ts, buf, item: outputs.append((ts, buf, item)), 4)
    print("sp._max_frame_size_bytes:", sp._max_frame_size_bytes)  # pylint: disable=protected-access

    def proc(
        b: typing.Union[bytes, memoryview]
    ) -> typing.Sequence[typing.Tuple[Timestamp, memoryview, typing.Optional[SerialFrame]]]:
        sp.process_next_chunk(b, ts)
        out = outputs[:]
        outputs.clear()
        for i, (t, bb, f) in enumerate(out):
            print(f"output {i + 1} of {len(out)}: ", t, bytes(bb), f)
        return out

    assert not outputs
    ((tsa, buf, a),) = proc(b"abcdef\x00")
    assert ts.monotonic_ns <= tsa.monotonic_ns <= Timestamp.now().monotonic_ns
    assert ts.system_ns <= tsa.system_ns <= Timestamp.now().system_ns
    assert a is None
    assert memoryview(b"abcdef\x00") == buf
    assert [] == proc(b"")

    # Valid frame.
    f1 = SerialFrame(
        priority=Priority.HIGH,
        source_node_id=SerialFrame.FRAME_DELIMITER_BYTE,
        destination_node_id=SerialFrame.FRAME_DELIMITER_BYTE,
        data_specifier=MessageDataSpecifier(2345),
        transfer_id=1234567890123456789,
        index=1234567,
        end_of_transfer=True,
        payload=memoryview(b"ab\x9E\x8E"),
    )  # 4 bytes of payload.
    ((tsa, buf, a),) = proc(f1.compile_into(bytearray(100)))
    assert tsa == ts
    assert isinstance(a, SerialFrame)
    assert SerialFrame.__eq__(f1, a)
    assert buf[-1] == 0  # Frame delimiters are in place.

    # Second valid frame is too long.
    f2 = SerialFrame(
        priority=Priority.HIGH,
        source_node_id=SerialFrame.FRAME_DELIMITER_BYTE,
        destination_node_id=SerialFrame.FRAME_DELIMITER_BYTE,
        data_specifier=MessageDataSpecifier(2345),
        transfer_id=1234567890123456789,
        index=1234567,
        end_of_transfer=True,
        payload=memoryview(bytes(f1.compile_into(bytearray(1000))) * 2),
    )
    assert len(f2.payload) == 43 * 2  # Cobs escaping
    ((tsa, buf, a),) = proc(f2.compile_into(bytearray(1000)))
    assert tsa == ts
    assert a is None
    assert buf[-1] == 0  # Frame delimiters are in place.

    # Create new instance with much larger frame size limit; feed both frames but let the first one be incomplete.
    sp = StreamParser(lambda ts, buf, item: outputs.append((ts, buf, item)), 10 ** 6)
    assert [] == proc(f1.compile_into(bytearray(200))[:-2])  # First one is ended abruptly.
    (tsa, _, a), (tsb, _, b), = proc(
        f2.compile_into(bytearray(200))
    )  # Then the second frame begins.
    assert tsa == ts
    assert tsb == ts
    assert a is None
    assert isinstance(b, SerialFrame)
