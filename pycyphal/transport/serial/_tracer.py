# Copyright (c) 2020 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import typing
import logging
import dataclasses
import pycyphal
import pycyphal.transport.serial
from pycyphal.transport import Trace, TransferTrace, Capture, AlienSessionSpecifier, AlienTransferMetadata
from pycyphal.transport import AlienTransfer, TransferFrom, Timestamp
from pycyphal.transport.commons.high_overhead_transport import AlienTransferReassembler, TransferReassembler
from ._frame import SerialFrame
from ._stream_parser import StreamParser


_logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class SerialCapture(pycyphal.transport.Capture):
    """
    Since Cyphal/serial operates on top of unstructured L1 data links, there is no native concept of framing.
    Therefore, the capture type defines only the timestamp, a raw chunk of bytes, and the direction (RX/TX).

    When capturing data from a live interface, it is guaranteed by this library that each capture will contain
    AT MOST one frame along with the delimiter bytes (at least the last byte of the fragment is zero).
    When reading data from a file, it is trivial to split the data into frames by looking for the frame separators,
    which are simply zero bytes.
    """

    fragment: memoryview

    own: bool
    """
    True if the captured fragment was sent by the local transport instance.
    False if it was received from the port.
    """

    def __repr__(self) -> str:
        """
        Captures that contain large fragments are truncated and appended with an ellipsis.
        """
        limit = 64
        if len(self.fragment) > limit:
            fragment = bytes(self.fragment[:limit]).hex() + f"...<+{len(self.fragment) - limit}B>..."
        else:
            fragment = bytes(self.fragment).hex()
        direction = "tx" if self.own else "rx"
        return pycyphal.util.repr_attributes(self, direction, fragment)

    @staticmethod
    def get_transport_type() -> typing.Type[pycyphal.transport.serial.SerialTransport]:
        return pycyphal.transport.serial.SerialTransport


@dataclasses.dataclass(frozen=True)
class SerialErrorTrace(pycyphal.transport.ErrorTrace):
    error: TransferReassembler.Error


@dataclasses.dataclass(frozen=True)
class SerialOutOfBandTrace(pycyphal.transport.ErrorTrace):
    """
    Out-of-band data or a malformed frame received. See :class:`pycyphal.serial.StreamParser`.
    """

    data: memoryview


class SerialTracer(pycyphal.transport.Tracer):
    """
    This tracer does not differentiate between input and output traces,
    but it keeps separate parsers for input and output captures such that there is no RX/TX state conflict.
    If necessary, the user can distinguish RX/TX traces by checking :attr:`SerialCapture.direction`
    before invoking :meth:`update`.

    Return types from :meth:`update`:

    - :class:`pycyphal.transport.TransferTrace`
    - :class:`SerialErrorTrace`
    - :class:`SerialOutOfBandTrace`
    """

    _MTU = 2**32
    """Effectively unlimited."""

    def __init__(self) -> None:
        self._parsers = [
            StreamParser(self._on_parsed, self._MTU),
            StreamParser(self._on_parsed, self._MTU),
        ]
        self._parser_output: typing.Optional[typing.Tuple[Timestamp, typing.Union[SerialFrame, memoryview]]] = None
        self._sessions: typing.Dict[AlienSessionSpecifier, _AlienSession] = {}

    def update(self, cap: Capture) -> typing.Optional[Trace]:
        """
        If the capture encapsulates more than one serialized frame, a :class:`ValueError` will be raised.
        To avoid this, always ensure that the captured fragments are split on the frame delimiters
        (which are simply zero bytes).
        Captures provided by PyCyphal are always fragmented correctly, but you may need to implement fragmentation
        manually when reading data from an external file.
        """
        if not isinstance(cap, SerialCapture):
            return None

        self._parsers[cap.own].process_next_chunk(cap.fragment, cap.timestamp)
        if self._parser_output is None:
            return None

        timestamp, item = self._parser_output
        self._parser_output = None
        if isinstance(item, memoryview):
            return SerialOutOfBandTrace(timestamp, item)

        if isinstance(item, SerialFrame):
            spec = AlienSessionSpecifier(
                source_node_id=item.source_node_id,
                destination_node_id=item.destination_node_id,
                data_specifier=item.data_specifier,
            )
            return self._get_session(spec).update(timestamp, item)

        assert False

    def _get_session(self, specifier: AlienSessionSpecifier) -> _AlienSession:
        try:
            return self._sessions[specifier]
        except KeyError:
            self._sessions[specifier] = _AlienSession(specifier)
        return self._sessions[specifier]

    def _on_parsed(self, timestamp: Timestamp, data: memoryview, frame: typing.Optional[SerialFrame]) -> None:
        _logger.debug(
            "Stream parser output (conflict: %s): %s <%d bytes> %s",
            bool(self._parser_output),
            timestamp,
            len(data),
            frame,
        )
        if self._parser_output is None:
            self._parser_output = timestamp, (data if frame is None else frame)
        else:
            self._parser_output = None
            raise ValueError(
                f"The supplied serial capture object contains more than one serialized entity. "
                f"Such arrangement cannot be processed correctly by this implementation. "
                f"Please update the caller code to always fragment the input byte stream at the frame delimiters, "
                f"which are simply zero bytes. "
                f"The timestamp of the offending capture is {timestamp}."
            )


class _AlienSession:
    def __init__(self, specifier: AlienSessionSpecifier) -> None:
        self._specifier = specifier
        src = specifier.source_node_id
        self._reassembler = AlienTransferReassembler(src) if src is not None else None

    def update(self, timestamp: Timestamp, frame: SerialFrame) -> typing.Optional[Trace]:
        reasm = self._reassembler
        tid_timeout = reasm.transfer_id_timeout if reasm is not None else 0.0

        tr: typing.Union[TransferFrom, TransferReassembler.Error, None]
        if reasm is not None:
            tr = reasm.process_frame(timestamp, frame)
        else:
            tr = TransferReassembler.construct_anonymous_transfer(timestamp, frame)

        if isinstance(tr, TransferReassembler.Error):
            return SerialErrorTrace(timestamp=timestamp, error=tr)

        if isinstance(tr, TransferFrom):
            meta = AlienTransferMetadata(tr.priority, tr.transfer_id, self._specifier)
            return TransferTrace(timestamp, AlienTransfer(meta, tr.fragmented_payload), tid_timeout)

        assert tr is None
        return None


# ----------------------------------------  TESTS GO BELOW THIS LINE  ----------------------------------------


def _unittest_serial_tracer() -> None:
    from pytest import raises, approx
    from pycyphal.transport import Priority, MessageDataSpecifier
    from pycyphal.transport.serial import SerialTransport

    tr = SerialTransport.make_tracer()
    ts = Timestamp.now()

    def tx(x: typing.Union[bytes, bytearray, memoryview]) -> typing.Optional[Trace]:
        return tr.update(SerialCapture(ts, memoryview(x), own=True))

    def rx(x: typing.Union[bytes, bytearray, memoryview]) -> typing.Optional[Trace]:
        return tr.update(SerialCapture(ts, memoryview(x), own=False))

    buf = SerialFrame(
        priority=Priority.SLOW,
        transfer_id=1234567890,
        index=0,
        end_of_transfer=True,
        payload=memoryview(b"abc"),
        source_node_id=1111,
        destination_node_id=None,
        data_specifier=MessageDataSpecifier(6666),
    ).compile_into(bytearray(100))
    head, tail = buf[:10], buf[10:]

    assert None is tx(head)  # Semi-complete.

    trace = tx(head)  # Double-head invalidates the previous one.
    assert isinstance(trace, SerialOutOfBandTrace)
    assert trace.timestamp == ts
    assert trace.data.tobytes().strip(b"\0") == head.tobytes().strip(b"\0")

    trace = tx(tail)
    assert isinstance(trace, TransferTrace)
    assert trace.timestamp == ts
    assert trace.transfer_id_timeout == approx(2.0)  # Initial value.
    assert trace.transfer.metadata.transfer_id == 1234567890
    assert trace.transfer.metadata.priority == Priority.SLOW
    assert trace.transfer.metadata.session_specifier.source_node_id == 1111
    assert trace.transfer.metadata.session_specifier.destination_node_id is None
    assert trace.transfer.metadata.session_specifier.data_specifier == MessageDataSpecifier(6666)
    assert trace.transfer.fragmented_payload == [memoryview(b"abc")]

    buf = SerialFrame(
        priority=Priority.SLOW,
        transfer_id=1234567890,
        index=0,
        end_of_transfer=True,
        payload=memoryview(b"abc"),
        source_node_id=None,
        destination_node_id=None,
        data_specifier=MessageDataSpecifier(6666),
    ).compile_into(bytearray(100))

    trace = rx(buf)
    assert isinstance(trace, TransferTrace)
    assert trace.timestamp == ts
    assert trace.transfer.metadata.transfer_id == 1234567890
    assert trace.transfer.metadata.session_specifier.source_node_id is None
    assert trace.transfer.metadata.session_specifier.destination_node_id is None

    assert None is tr.update(pycyphal.transport.Capture(ts))  # Wrong type, ignore.

    trace = tx(
        SerialFrame(
            priority=Priority.SLOW,
            transfer_id=1234567890,
            index=0,
            end_of_transfer=False,
            payload=memoryview(bytes(range(256))),
            source_node_id=3333,
            destination_node_id=None,
            data_specifier=MessageDataSpecifier(6666),
        ).compile_into(bytearray(10_000))
    )
    assert trace is None
    trace = tx(
        SerialFrame(
            priority=Priority.SLOW,
            transfer_id=1234567890,
            index=1,
            end_of_transfer=True,
            payload=memoryview(bytes(range(256))),
            source_node_id=3333,
            destination_node_id=None,
            data_specifier=MessageDataSpecifier(6666),
        ).compile_into(bytearray(10_000))
    )
    assert isinstance(trace, SerialErrorTrace)
    assert trace.error == TransferReassembler.Error.MULTIFRAME_INTEGRITY_ERROR

    with raises(ValueError, match=".*delimiters.*"):
        rx(b"".join([buf, buf]))
