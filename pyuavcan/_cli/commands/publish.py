# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import typing
import asyncio
import logging
import argparse
import contextlib
import pyuavcan
from . import _util, _subsystems
from ._argparse_helpers import make_enum_action
from ._yaml import YAMLLoader
from ._base import Command, SubsystemFactory


_logger = logging.getLogger(__name__)


class PublishCommand(Command):
    @property
    def names(self) -> typing.Sequence[str]:
        return ["publish", "pub"]

    @property
    def help(self) -> str:
        return """
Publish messages of the specified subject with the fixed contents. The local node will also publish heartbeat and
respond to GetInfo, unless it is configured to be anonymous.
""".strip()

    @property
    def examples(self) -> typing.Optional[str]:
        return """
pyuavcan pub uavcan.diagnostic.Record.1.1 '{text: "Hello world!"}'
""".strip()

    @property
    def subsystem_factories(self) -> typing.Sequence[SubsystemFactory]:
        return [
            _subsystems.node.NodeFactory(node_name_suffix=self.names[0], allow_anonymous=True),
        ]

    def register_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "subject_spec",
            metavar="[SUBJECT_ID.]FULL_MESSAGE_TYPE_NAME.MAJOR.MINOR YAML_FIELDS",
            nargs="*",
            help="""
The full message type name with version and optional subject-ID, followed by the YAML (or JSON, which is a subset of
YAML)-formatted contents of the message (separated by whitespace). Missing fields will be left at their default values.
Use empty dict as "{}" to construct a default-initialized message. For more info about the YAML representation, read
the PyUAVCAN documentation on builtin-based representations.

The subject-ID can be omitted if a fixed one is defined for the data type.

The number of such pairs can be arbitrary; all defined messages will be published synchronously. If no such pairs are
specified, nothing will be published, unless the local node is not anonymous. Per the specification, a non-anonymous
node must publish heartbeat; this requirement is respected. Additionally, the recommended standard service
uavcan.node.GetInfo is served.

Forward or backward slashes can be used instead of "."; version numbers can be also separated using underscores.

Examples:
    1234.uavcan.diagnostic.Record.1.1 '{"text": "Hello world!"}'
    uavcan/diagnostic/Record_1_1 '{"text": "Hello world!"}'
""".strip(),
        )
        parser.add_argument(
            "--period",
            "-P",
            type=float,
            default=1.0,
            metavar="SECONDS",
            help="""
Message publication period. All messages are published synchronously, so the period setting applies to all specified
subjects. Besides, the period of heartbeat is defined as min((--period), MAX_PUBLICATION_PERIOD); i.e., unless this
value exceeds the maximum period defined for heartbeat by the specification, it is used for heartbeat as well. Note
that anonymous nodes do not publish heartbeat.

The send timeout for all publishers will equal the publication period.

Default: %(default)s
""".strip(),
        )
        parser.add_argument(
            "--count",
            "-C",
            type=int,
            default=1,
            metavar="NATURAL",
            help="""
Number of synchronous publication cycles before exiting normally. The duration therefore equals (--period) * (--count).
Default: %(default)s
""".strip(),
        )
        parser.add_argument(
            "--priority",
            default=pyuavcan.presentation.DEFAULT_PRIORITY,
            action=make_enum_action(pyuavcan.transport.Priority),
            help="""
Priority of published message transfers. Applies to the heartbeat as well.
Default: %(default)s
""".strip(),
        )

    def execute(self, args: argparse.Namespace, subsystems: typing.Sequence[object]) -> int:
        import pyuavcan.application
        import pyuavcan.application.heartbeat_publisher

        (node,) = subsystems
        assert isinstance(node, pyuavcan.application.Node)

        with contextlib.closing(node):
            node.heartbeat_publisher.priority = args.priority
            node.heartbeat_publisher.period = min(
                pyuavcan.application.heartbeat_publisher.Heartbeat.MAX_PUBLICATION_PERIOD, args.period
            )

            raw_ss = args.subject_spec
            if len(raw_ss) % 2 != 0:
                raise ValueError(
                    "Mismatching arguments: each subject specifier must be matched with its field specifier, like: "
                    "subject-a field-a [subject-b field-b] [...]"
                )
            publications: typing.List[Publication] = []
            for subject_spec, field_spec in (raw_ss[i : i + 2] for i in range(0, len(raw_ss), 2)):
                publications.append(
                    Publication(
                        subject_spec=subject_spec,
                        field_spec=field_spec,
                        presentation=node.presentation,
                        priority=args.priority,
                        send_timeout=args.period,
                    )
                )
            _logger.info("Publication set: %r", publications)

            try:
                asyncio.get_event_loop().run_until_complete(
                    self._run(node=node, count=int(args.count), period=float(args.period), publications=publications)
                )
            except KeyboardInterrupt:
                pass

            if _logger.isEnabledFor(logging.INFO):
                _logger.info("%s", node.presentation.transport.sample_statistics())
                for s in node.presentation.transport.output_sessions:
                    ds = s.specifier.data_specifier
                    if isinstance(ds, pyuavcan.transport.MessageDataSpecifier):
                        _logger.info("Subject %d: %s", ds.subject_id, s.sample_statistics())

        return 0

    @staticmethod
    async def _run(node: object, count: int, period: float, publications: typing.Sequence[Publication]) -> None:
        import pyuavcan.application

        assert isinstance(node, pyuavcan.application.Node)
        node.start()

        sleep_until = asyncio.get_event_loop().time()
        for c in range(count):
            out = await asyncio.gather(*[p.publish() for p in publications])
            assert len(out) == len(publications)
            assert all(isinstance(x, bool) for x in out)
            if not all(out):
                log_elements = "\n\t".join(f"#{idx}: {publications[idx]}" for idx, res in enumerate(out) if not res)
                _logger.error("The following publications have timed out:\n\t" + log_elements)

            sleep_until += period
            _logger.info(
                "Publication cycle %d of %d completed; sleeping for %.3f seconds",
                c + 1,
                count,
                sleep_until - asyncio.get_event_loop().time(),
            )
            await asyncio.sleep(sleep_until - asyncio.get_event_loop().time())


class Publication:
    _YAML_LOADER = YAMLLoader()

    def __init__(
        self,
        subject_spec: str,
        field_spec: str,
        presentation: pyuavcan.presentation.Presentation,
        priority: pyuavcan.transport.Priority,
        send_timeout: float,
    ):
        subject_id, dtype = _util.construct_port_id_and_type(subject_spec)
        content = self._YAML_LOADER.load(field_spec)

        self._message = pyuavcan.dsdl.update_from_builtin(dtype(), content)
        self._publisher = presentation.make_publisher(dtype, subject_id)
        self._publisher.priority = priority
        self._publisher.send_timeout = send_timeout

    async def publish(self) -> bool:
        return await self._publisher.publish(self._message)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._message}, {self._publisher})"
