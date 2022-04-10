# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>


def _unittest_transport_primitives() -> None:
    from pytest import raises
    from pycyphal.transport import InputSessionSpecifier, OutputSessionSpecifier
    from pycyphal.transport import MessageDataSpecifier, ServiceDataSpecifier, PayloadMetadata

    with raises(ValueError):
        MessageDataSpecifier(-1)

    with raises(ValueError):
        MessageDataSpecifier(32768)

    with raises(ValueError):
        ServiceDataSpecifier(-1, ServiceDataSpecifier.Role.REQUEST)

    with raises(ValueError):
        InputSessionSpecifier(MessageDataSpecifier(123), -1)

    with raises(ValueError):
        OutputSessionSpecifier(ServiceDataSpecifier(100, ServiceDataSpecifier.Role.RESPONSE), None)

    with raises(ValueError):
        PayloadMetadata(-1)
