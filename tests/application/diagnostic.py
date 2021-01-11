# Copyright (c) 2020 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import re
import typing
import asyncio
import logging
import pytest
import pyuavcan
from pyuavcan.transport.loopback import LoopbackTransport
from pyuavcan.presentation import Presentation


@pytest.mark.asyncio  # type: ignore
async def _unittest_slow_diagnostic(
    generated_packages: typing.List[pyuavcan.dsdl.GeneratedPackageInfo], caplog: typing.Any
) -> None:
    from pyuavcan.application import diagnostic
    from uavcan.time import SynchronizedTimestamp_1_0

    assert generated_packages

    pres = Presentation(LoopbackTransport(2222))
    pub = pres.make_publisher_with_fixed_subject_id(diagnostic.Record)
    diag = diagnostic.DiagnosticSubscriber(pres)

    diag.start()

    caplog.clear()
    await pub.publish(
        diagnostic.Record(
            timestamp=SynchronizedTimestamp_1_0(123456789),
            severity=diagnostic.Severity(diagnostic.Severity.INFO),
            text="Hello world!",
        )
    )
    await asyncio.sleep(1.0)
    print("Captured log records:")
    for lr in caplog.records:
        print("   ", lr)
        assert isinstance(lr, logging.LogRecord)
        pat = """
Received uavcan.diagnostic.Record from node 2222; severity 2; remote ts 123.456789 s, local ts [^;]*; text:
    Hello world!
""".strip()
        if lr.levelno == logging.INFO and re.match(pat, lr.message):
            break
    else:
        assert False, "Expected log message not captured"

    diag.close()
    pub.close()
    pres.close()
    await asyncio.sleep(1.0)  # Let the background tasks terminate.
