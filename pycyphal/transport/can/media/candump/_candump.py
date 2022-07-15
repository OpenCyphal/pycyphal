# Copyright (c) 2022 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import re
import os
import signal
import time
from typing import Sequence, Iterable, TextIO
import asyncio
import logging
import queue
from pathlib import Path
from decimal import Decimal
import threading
import dataclasses
import pycyphal.util
from pycyphal.transport import Timestamp
from pycyphal.transport.can.media import Media, Envelope, FilterConfiguration, DataFrame, FrameFormat


_logger = logging.getLogger(__name__)


class CandumpMedia(Media):
    """
    This is a pseudo-media layer that replays standard SocketCAN candump log files.
    It can be used to perform postmortem analysis of a Cyphal/CAN network based on the standard log files
    collected by ``candump``.

    If the dump file contains frames collected from multiple interfaces,
    frames from only one of the interfaces will be read and the others will be skipped.
    The name of that interface is obtained from the first valid logged frame.
    If you want to process frames from other interfaces, use grep to filter them out.

    Please refer to the SocketCAN documentation for the format description.
    Here's an example::

        (1657800496.359233) slcan0 0C60647D#020000FB
        (1657800496.360136) slcan0 10606E7D#00000000000000BB
        (1657800496.360149) slcan1 10606E7D#000000000000001B
        (1657800496.360152) slcan0 10606E7D#000000000000003B
        (1657800496.360155) slcan0 10606E7D#0000C6565B
        (1657800496.360305) slcan2 1060787D#00000000000000BB
        (1657800496.360317) slcan0 1060787D#0000C07F147CB71B
        (1657800496.361011) slcan1 1060787D#412BCC7B
        (1657800496.361022) slcan2 10608C7D#73000000000000FB
        (1657800496.361026) slcan0 1060967D#00000000000000BB
        (1657800496.361028) slcan0 1060967D#00313E5B
        (1657800496.361258) slcan1 1460827D#7754A643E06A96BB
        (1657800496.361269) slcan0 1460827D#430000000000001B
        (1657800496.361273) slcan0 1460827D#EE3C7B
        (1657800496.362258) slcan0 1460A07D#335DB35CD85CFB
        (1657800496.362270) slcan0 107D557D#5F000000000000FB
        (1657800497.359273) slcan0 0C60647D#020000FC
        (1657800497.360146) slcan0 10606E7D#00000000000000BC
        (1657800497.360158) slcan0 10606E7D#000000000000001C
        (1657800497.360161) slcan2 10606E7D#000000000000003C

    Each line contains a CAN frame which is reported as received with the specified wall (system) timestamp.
    This media layer, naturally, cannot accept outgoing frames, so they are dropped (and logged).

    Usage example with `Yakut <https://github.com/OpenCyphal/yakut>`_::

        export UAVCAN__CAN__IFACE='candump:verification/integration/candump.log'
        y sub uavcan.node.heartbeat 10:reg.udral.service.common.readiness 130:reg.udral.service.actuator.common.status
        y mon

    .. note::

        Currently, there is no way for this media implementation to notify the upper layers that the end of the
        log file is reached.
        It should be addressed eventually as part of `#227 <https://github.com/OpenCyphal/pycyphal/issues/227>`_.
        Meanwhile, you can force the media layer to terminate its own process when the log file is fully replayed
        by setting the environment variable ``PYCYPHAL_CANDUMP_YOU_ARE_TERMINATED`` to a non-zero value.

        Ideally, there also should be a way to report how far along are we in the log file,
        but it is not clear how to reconcile that with the normal media implementations.

    ..  warning::

        The API of this class is experimental and subject to breaking changes.
    """

    GLOB_PATTERN = "candump*.log"

    _BATCH_SIZE_LIMIT = 100

    _ENV_EXIT_AT_END = "PYCYPHAL_CANDUMP_YOU_ARE_TERMINATED"

    def __init__(self, file: str | Path | TextIO) -> None:
        """
        :param file: Path to the candump log file, or a text-IO instance.
        """
        self._f: TextIO = (
            open(file, "r", encoding="utf8")  # pylint: disable=consider-using-with
            if isinstance(file, (str, Path))
            else file
        )
        self._thread: threading.Thread | None = None
        self._iface_name: str | None = None
        self._acceptance_filters_queue: queue.Queue[Sequence[FilterConfiguration]] = queue.Queue()

    @property
    def interface_name(self) -> str:
        """
        The name of the log file.
        """
        return self._f.name

    @property
    def mtu(self) -> int:
        return max(Media.VALID_MTU_SET)

    @property
    def number_of_acceptance_filters(self) -> int:
        return 1

    def start(self, handler: Media.ReceivedFramesHandler, no_automatic_retransmission: bool) -> None:
        _ = no_automatic_retransmission
        if self._thread is not None:
            raise RuntimeError(f"{self!r}: Already started")
        self._thread = threading.Thread(
            target=self._thread_function, name=str(self), args=(handler, asyncio.get_event_loop()), daemon=True
        )
        self._thread.start()

    def configure_acceptance_filters(self, configuration: Sequence[FilterConfiguration]) -> None:
        self._acceptance_filters_queue.put_nowait(configuration)

    async def send(self, frames: Iterable[Envelope], monotonic_deadline: float) -> int:
        """
        Sent frames are dropped.
        """
        _logger.debug(
            "%r: Sending not supported, TX frames with monotonic_deadline=%r dropped: %r",
            self,
            monotonic_deadline,
            list(frames),
        )
        return 0

    def close(self) -> None:
        if self._thread is not None:
            self._f.close()
            self._thread, thd = None, self._thread
            assert thd is not None
            thd.join(timeout=1)

    @property
    def _is_closed(self) -> bool:
        return self._thread is None

    def _thread_function(self, handler: Media.ReceivedFramesHandler, loop: asyncio.AbstractEventLoop) -> None:
        def forward(batch: list[DataFrameRecord]) -> None:
            if not self._is_closed:  # Don't call after closure to prevent race conditions and use-after-close.
                pycyphal.util.broadcast([handler])(
                    [
                        (
                            rec.ts,
                            Envelope(
                                frame=DataFrame(format=rec.fmt, identifier=rec.can_id, data=bytearray(rec.can_payload)),
                                loopback=False,
                            ),
                        )
                        for rec in batch
                    ]
                )

        try:
            _logger.debug("%r: Waiting for the acceptance filters to be configured before proceeding...", self)
            while True:
                try:
                    self._acceptance_filters_queue.get(timeout=1.0)
                except queue.Empty:
                    pass
                else:
                    break
            _logger.debug("%r: Acceptance filters configured, starting to read frames", self)
            batch: list[DataFrameRecord] = []
            time_offset: float | None = None
            for idx, line in enumerate(self._f):
                rec = Record.parse(line)
                if not rec:
                    _logger.warning("%r: Cannot parse line %d: %r", self, idx + 1, line)
                    continue
                _logger.debug("%r: Parsed line %d: %r -> %s", self, idx + 1, line, rec)
                if not isinstance(rec, DataFrameRecord):
                    continue
                if self._iface_name is None:
                    self._iface_name = rec.iface_name
                    _logger.info("%r: Interface filter auto-set to: %r", self, self._iface_name)
                if rec.iface_name != self._iface_name:
                    _logger.debug(
                        "%r: Line %d skipped: iface mismatch: %r != %r",
                        self,
                        idx + 1,
                        rec.iface_name,
                        self._iface_name,
                    )
                    continue
                now_mono = time.monotonic()
                ts = float(rec.ts.system)
                if time_offset is None:
                    time_offset = ts - now_mono
                target_mono = ts - time_offset
                sleep_duration = target_mono - now_mono
                if sleep_duration > 0 or len(batch) > self._BATCH_SIZE_LIMIT:
                    loop.call_soon_threadsafe(forward, batch)
                    batch = []
                    if sleep_duration > 0:
                        time.sleep(sleep_duration)
                batch.append(rec)
            loop.call_soon_threadsafe(forward, batch)
        except BaseException as ex:  # pylint: disable=broad-except
            if not self._is_closed:
                _logger.exception("%r: Log file reader failed: %s", self, ex)
        _logger.debug("%r: Reader thread exiting, bye bye", self)
        self._f.close()
        # FIXME: this should be addressed properly as part of https://github.com/OpenCyphal/pycyphal/issues/227
        # Perhaps we should send some notification to the upper layers that the media is toast.
        if os.getenv(self._ENV_EXIT_AT_END, "0") != "0":
            _logger.warning(
                "%r: Terminating the process because reached the end of the log file and the envvar %s is set. "
                "This is a workaround for https://github.com/OpenCyphal/pycyphal/issues/227",
                self,
                self._ENV_EXIT_AT_END,
            )
            os.kill(os.getpid(), signal.SIGINT)

    @staticmethod
    def list_available_interface_names(*, recurse: bool = False) -> Iterable[str]:
        """
        Returns the list of candump log files in the current working directory.
        """
        directory = Path.cwd()
        glo = directory.rglob if recurse else directory.glob
        return [str(x) for x in glo(CandumpMedia.GLOB_PATTERN)]


_RE_REC_REMOTE = re.compile(r"(?a)^\s*\((\d+\.\d+)\)\s+([\w-]+)\s+([\da-fA-F]+)#R")
_RE_REC_DATA = re.compile(r"(?a)^\s*\((\d+\.\d+)\)\s+([\w-]+)\s+([\da-fA-F]+)#([\da-fA-F]*)")


@dataclasses.dataclass(frozen=True)
class Record:
    @staticmethod
    def parse(line: str) -> None | Record:
        try:
            if _RE_REC_REMOTE.match(line):
                return UnsupportedRecord()
            match = _RE_REC_DATA.match(line)
            if not match:
                return None
            s_ts, iface_name, s_canid, s_data = match.groups()
            return DataFrameRecord(
                ts=Timestamp(
                    system_ns=int(Decimal(s_ts) * Decimal("1e9")),
                    monotonic_ns=time.monotonic_ns(),
                ),
                iface_name=iface_name,
                fmt=FrameFormat.EXTENDED if len(s_canid) > 3 else FrameFormat.BASE,
                can_id=int(s_canid, 16),
                can_payload=bytes.fromhex(s_data),
            )
        except ValueError as ex:
            _logger.debug("Cannot convert values from line %r: %r", line, ex)
            return None


@dataclasses.dataclass(frozen=True)
class UnsupportedRecord(Record):
    pass


@dataclasses.dataclass(frozen=True)
class DataFrameRecord(Record):
    ts: Timestamp
    iface_name: str
    fmt: FrameFormat
    can_id: int
    can_payload: bytes

    def __str__(self) -> str:
        if self.fmt == FrameFormat.EXTENDED:
            s_id = f"{self.can_id:08x}"
        elif self.fmt == FrameFormat.BASE:
            s_id = f"{self.can_id:03x}"
        else:
            assert False
        return f"{self.ts} {self.iface_name!r} {s_id}#{self.can_payload.hex()}"


def _unittest_record_parse() -> None:
    rec = Record.parse("(1657800496.359233) slcan0 0C60647D#020000FB\n")
    assert isinstance(rec, DataFrameRecord)
    assert rec.ts.system_ns == 1657800496_359233000
    assert rec.iface_name == "slcan0"
    assert rec.fmt == FrameFormat.EXTENDED
    assert rec.can_id == 0x0C60647D
    assert rec.can_payload == bytes.fromhex("020000FB")
    print(rec)

    rec = Record.parse("(1657800496.359233) slcan0 0C6#\n")
    assert isinstance(rec, DataFrameRecord)
    assert rec.ts.system_ns == 1657800496_359233000
    assert rec.iface_name == "slcan0"
    assert rec.fmt == FrameFormat.BASE
    assert rec.can_id == 0x0C6
    assert rec.can_payload == bytes()
    print(rec)

    rec = Record.parse("(1657805304.099792) slcan0 123#R\n")
    assert isinstance(rec, UnsupportedRecord)

    rec = Record.parse("whatever\n")
    assert rec is None
    rec = Record.parse("")
    assert rec is None
