# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>


def _unittest_transport_primitives() -> None:
    from pytest import raises
    from pyuavcan.transport import InputSessionSpecifier, OutputSessionSpecifier
    from pyuavcan.transport import MessageDataSpecifier, ServiceDataSpecifier, PayloadMetadata

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
