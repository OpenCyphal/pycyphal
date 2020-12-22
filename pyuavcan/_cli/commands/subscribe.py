# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing
import asyncio
import logging
import argparse
import contextlib
import pyuavcan
from . import _util, _subsystems
from ._base import Command, SubsystemFactory


_M = typing.TypeVar("_M", bound=pyuavcan.dsdl.CompositeObject)


_logger = logging.getLogger(__name__)


class SubscribeCommand(Command):
    @property
    def names(self) -> typing.Sequence[str]:
        return ["subscribe", "sub"]

    @property
    def help(self) -> str:
        return """
Subscribe to the specified subject, receive and print messages into stdout. This command does not instantiate a local
node; the bus is accessed directly at the presentation layer, so many instances can be cheaply executed concurrently
to subscribe to multiple message streams.

Each emitted output unit is a key-value mapping where the number of elements equals the number of subjects the command
is asked to subscribe to; the keys are subject-IDs and values are the received message objects. The output format is
configurable.
""".strip()

    @property
    def examples(self) -> typing.Optional[str]:
        return f"""
pyuavcan sub uavcan.node.Heartbeat.1.0
""".strip()

    @property
    def subsystem_factories(self) -> typing.Sequence[SubsystemFactory]:
        return [
            _subsystems.transport.TransportFactory(),
            _subsystems.formatter.FormatterFactory(),
        ]

    def register_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "subject_spec",
            metavar="[SUBJECT_ID.]FULL_MESSAGE_TYPE_NAME.MAJOR.MINOR",
            nargs="+",
            help="""
A set of full message type names with version and optional subject-ID for each. The subject-ID can be omitted if a
fixed one is defined for the data type. If multiple subjects are selected, a synchronizing subscription will be used,
reporting received messages in synchronous groups.

Forward or backward slashes can be used instead of "."; version numbers can be also separated using underscores.

Examples:
    1234.uavcan.node.Heartbeat.1.0 (using subject-ID 1234)
    uavcan/node/Heartbeat_1_0 (using the fixed subject-ID 7509, non-canonical notation)
""".strip(),
        )
        parser.add_argument(
            "--with-metadata",
            "-M",
            action="store_true",
            help="""
Emit metadata together with each message. The metadata fields will be contained under the key "_metadata_".
""".strip(),
        )
        parser.add_argument(
            "--count",
            "-C",
            type=int,
            metavar="NATURAL",
            help="""
Exit automatically after this many messages (or synchronous message groups) have been received. No limit by default.
""".strip(),
        )

    def execute(self, args: argparse.Namespace, subsystems: typing.Sequence[object]) -> int:
        transport, formatter = subsystems
        assert isinstance(transport, pyuavcan.transport.Transport)
        assert callable(formatter)

        subject_specs = [_util.construct_port_id_and_type(ds) for ds in args.subject_spec]

        _logger.debug(
            f"Starting the subscriber with transport={transport}, subject_specs={subject_specs}, "
            f"formatter={formatter}, with_metadata={args.with_metadata}"
        )

        with contextlib.closing(pyuavcan.presentation.Presentation(transport)) as presentation:
            subscriber = self._make_subscriber(args, presentation)  # type: ignore
            try:
                asyncio.get_event_loop().run_until_complete(
                    _run(
                        subscriber=subscriber,
                        formatter=formatter,
                        with_metadata=args.with_metadata,
                        count=int(args.count) if args.count is not None else (2 ** 64),
                    )
                )
            except KeyboardInterrupt:
                pass

            if _logger.isEnabledFor(logging.INFO):
                _logger.info("%s", presentation.transport.sample_statistics())
                _logger.info("%s", subscriber.sample_statistics())

        return 0

    @staticmethod
    def _make_subscriber(
        args: argparse.Namespace, presentation: pyuavcan.presentation.Presentation
    ) -> pyuavcan.presentation.Subscriber[_M]:
        # TODO: the return type will probably have to be changed when multi-subject subscription is supported.
        subject_specs = [_util.construct_port_id_and_type(ds) for ds in args.subject_spec]
        if len(subject_specs) < 1:
            raise ValueError("Nothing to do: no subjects specified")
        elif len(subject_specs) == 1:
            subject_id, dtype = subject_specs[0]
            return presentation.make_subscriber(dtype, subject_id)  # type: ignore
        else:
            raise NotImplementedError(
                "Multi-subject subscription is not yet implemented, sorry! "
                "See https://github.com/UAVCAN/pyuavcan/issues/65"
            )


async def _run(
    subscriber: pyuavcan.presentation.Subscriber[_M],
    formatter: _subsystems.formatter.Formatter,
    with_metadata: bool,
    count: int,
) -> None:
    async for msg, transfer in subscriber:
        assert isinstance(transfer, pyuavcan.transport.TransferFrom)
        outer: typing.Dict[int, typing.Dict[str, typing.Any]] = {}

        bi: typing.Dict[str, typing.Any] = {}  # We use updates to ensure proper dict ordering: metadata before data
        if with_metadata:
            bi.update(_util.convert_transfer_metadata_to_builtin(transfer))
        bi.update(pyuavcan.dsdl.to_builtin(msg))
        outer[subscriber.port_id] = bi

        print(formatter(outer))

        count -= 1
        if count <= 0:
            _logger.debug("Reached the specified message count, stopping")
            break
