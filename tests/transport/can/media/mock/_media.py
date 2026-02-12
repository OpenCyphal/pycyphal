# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import typing
import asyncio
import pytest
import pycyphal.transport
from pycyphal.transport import Timestamp
from pycyphal.transport.can.media import Media, Envelope, FilterConfiguration, DataFrame, FrameFormat

pytestmark = pytest.mark.asyncio


class MockMedia(Media):
    def __init__(self, peers: typing.Set[MockMedia], mtu: int, number_of_acceptance_filters: int):
        self._peers = peers
        peers.add(self)

        self._mtu = int(mtu)

        self._rx_handler: Media.ReceivedFramesHandler = lambda _: None  # pragma: no cover
        self._acceptance_filters = [
            self._make_dead_filter()  # By default drop (almost) all frames
            for _ in range(int(number_of_acceptance_filters))
        ]
        self._automatic_retransmission_enabled = False  # This is the default per the media interface spec
        self._closed = False

        self._raise_on_send_once: typing.Optional[Exception] = None

        super().__init__()

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return asyncio.get_event_loop()

    @property
    def interface_name(self) -> str:
        return f"mock@{id(self._peers):08x}"

    @property
    def mtu(self) -> int:
        return self._mtu

    @property
    def number_of_acceptance_filters(self) -> int:
        return len(self._acceptance_filters)

    def start(
        self,
        handler: Media.ReceivedFramesHandler,
        no_automatic_retransmission: bool,
        error_handler: Media.ErrorHandler | None = None,
    ) -> None:
        if self._closed:
            raise pycyphal.transport.ResourceClosedError

        assert callable(handler)
        self._rx_handler = handler
        assert isinstance(no_automatic_retransmission, bool)
        self._automatic_retransmission_enabled = not no_automatic_retransmission

    def configure_acceptance_filters(self, configuration: typing.Sequence[FilterConfiguration]) -> None:
        if self._closed:
            raise pycyphal.transport.ResourceClosedError

        configuration = list(configuration)  # Do not mutate the argument
        while len(configuration) < len(self._acceptance_filters):
            configuration.append(self._make_dead_filter())

        assert len(configuration) == len(self._acceptance_filters)
        self._acceptance_filters = configuration

    @property
    def automatic_retransmission_enabled(self) -> bool:
        return self._automatic_retransmission_enabled

    @property
    def acceptance_filters(self) -> typing.List[FilterConfiguration]:
        return list(self._acceptance_filters)

    async def send(self, frames: typing.Iterable[Envelope], monotonic_deadline: float) -> int:
        del monotonic_deadline  # Unused
        if self._closed:
            raise pycyphal.transport.ResourceClosedError

        if self._raise_on_send_once:
            self._raise_on_send_once, ex = None, self._raise_on_send_once
            assert isinstance(ex, Exception)
            raise ex

        frames = list(frames)
        assert len(frames) > 0, "Interface constraint violation: empty transmission set"
        assert min(map(lambda x: len(x.frame.data), frames)) >= 1, "CAN frames with empty payload are not valid"
        # The media interface spec says that it is guaranteed that the CAN ID is the same across the set; enforce this.
        assert len(set(map(lambda x: x.frame.identifier, frames))) == 1, "Interface constraint violation: nonuniform ID"

        timestamp = Timestamp.now()

        # Broadcast across the virtual bus we're emulating here.
        for p in self._peers:
            if p is not self:
                # Unconditionally clear the loopback flag because for the other side these are
                # regular received frames, not loopback frames.
                p._receive(  # pylint: disable=protected-access
                    (timestamp, Envelope(f.frame, loopback=False)) for f in frames
                )

        # Simple loopback emulation with acceptance filtering.
        self._receive((timestamp, f) for f in frames if f.loopback)
        return len(frames)

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._peers.remove(self)

    def raise_on_send_once(self, ex: Exception) -> None:
        self._raise_on_send_once = ex

    def inject_received(self, frames: typing.Iterable[typing.Union[Envelope, DataFrame]]) -> None:
        timestamp = Timestamp.now()
        self._receive(
            (
                timestamp,
                (f if isinstance(f, Envelope) else Envelope(frame=f, loopback=False)),
            )
            for f in frames
        )

    def _receive(self, frames: typing.Iterable[typing.Tuple[Timestamp, Envelope]]) -> None:
        frames = list(filter(lambda item: self._test_acceptance(item[1].frame), frames))
        if frames:  # Where are the assignment expressions when you need them?
            self._rx_handler(frames)

    def _test_acceptance(self, frame: DataFrame) -> bool:
        return any(
            map(
                lambda f: frame.identifier & f.mask == f.identifier & f.mask
                and (f.format is None or frame.format == f.format),
                self._acceptance_filters,
            )
        )

    @staticmethod
    def list_available_interface_names() -> typing.Iterable[str]:
        return []  # pragma: no cover

    @staticmethod
    def _make_dead_filter() -> FilterConfiguration:
        fmt = FrameFormat.BASE
        return FilterConfiguration(0, 2 ** int(fmt) - 1, fmt)


async def _unittest_can_mock_media() -> None:
    peers: typing.Set[MockMedia] = set()

    me = MockMedia(peers, 64, 3)
    assert len(peers) == 1 and me in peers
    assert me.mtu == 64
    assert me.number_of_acceptance_filters == 3
    assert not me.automatic_retransmission_enabled
    assert str(me) == f"MockMedia('mock@{id(peers):08x}', mtu=64)"

    me_collector = FrameCollector()
    me.start(me_collector.give, False)
    assert me.automatic_retransmission_enabled

    # Will drop the loopback because of the acceptance filters
    await me.send(
        [
            Envelope(DataFrame(FrameFormat.EXTENDED, 123, bytearray(b"abc")), loopback=False),
            Envelope(DataFrame(FrameFormat.EXTENDED, 123, bytearray(b"def")), loopback=True),
        ],
        asyncio.get_event_loop().time() + 1.0,
    )
    assert me_collector.empty

    me.configure_acceptance_filters([FilterConfiguration.new_promiscuous()])
    # Now the loopback will be accepted because we have reconfigured the filters
    await me.send(
        [
            Envelope(DataFrame(FrameFormat.EXTENDED, 123, bytearray(b"abc")), loopback=False),
            Envelope(DataFrame(FrameFormat.EXTENDED, 123, bytearray(b"def")), loopback=True),
        ],
        asyncio.get_event_loop().time() + 1.0,
    )
    assert me_collector.pop()[1].frame == DataFrame(FrameFormat.EXTENDED, 123, bytearray(b"def"))
    assert me_collector.empty

    pe = MockMedia(peers, 8, 1)
    assert peers == {me, pe}

    pe_collector = FrameCollector()
    pe.start(pe_collector.give, False)

    me.raise_on_send_once(RuntimeError("Hello world!"))
    with pytest.raises(RuntimeError, match="Hello world!"):
        await me.send([], asyncio.get_event_loop().time() + 1.0)

    await me.send(
        [
            Envelope(DataFrame(FrameFormat.EXTENDED, 123, bytearray(b"abc")), loopback=False),
            Envelope(DataFrame(FrameFormat.EXTENDED, 123, bytearray(b"def")), loopback=True),
        ],
        asyncio.get_event_loop().time() + 1.0,
    )
    assert pe_collector.empty

    pe.configure_acceptance_filters([FilterConfiguration(123, 127, None)])
    await me.send(
        [
            Envelope(DataFrame(FrameFormat.EXTENDED, 123, bytearray(b"abc")), loopback=False),
            Envelope(DataFrame(FrameFormat.EXTENDED, 123, bytearray(b"def")), loopback=True),
        ],
        asyncio.get_event_loop().time() + 1.0,
    )
    await me.send(
        [
            Envelope(DataFrame(FrameFormat.EXTENDED, 456, bytearray(b"ghi")), loopback=False),  # Dropped by the filters
        ],
        asyncio.get_event_loop().time() + 1.0,
    )
    assert pe_collector.pop()[1].frame == DataFrame(FrameFormat.EXTENDED, 123, bytearray(b"abc"))
    assert pe_collector.pop()[1].frame == DataFrame(FrameFormat.EXTENDED, 123, bytearray(b"def"))
    assert pe_collector.empty

    me.close()
    me.close()  # Idempotency.
    assert peers == {pe}
    with pytest.raises(pycyphal.transport.ResourceClosedError):
        await me.send([], asyncio.get_event_loop().time() + 1.0)
    with pytest.raises(pycyphal.transport.ResourceClosedError):
        me.configure_acceptance_filters([])
    await asyncio.sleep(1)  # Let all pending tasks finalize properly to avoid stack traces in the output.


class FrameCollector:
    def __init__(self) -> None:
        self._collected: typing.List[typing.Tuple[Timestamp, Envelope]] = []

    def give(self, frames: typing.Iterable[typing.Tuple[Timestamp, Envelope]]) -> None:
        frames = list(frames)
        assert all(map(lambda x: isinstance(x[0], Timestamp) and isinstance(x[1], Envelope), frames))
        self._collected += frames

    def pop(self) -> typing.Tuple[Timestamp, Envelope]:
        head, *self._collected = self._collected
        return head

    @property
    def empty(self) -> bool:
        return len(self._collected) == 0

    def __repr__(self) -> str:  # pragma: no cover
        return f"{type(self).__name__}({str(self._collected)})"
