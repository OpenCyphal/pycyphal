#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#


def _unittest_transport_primitives() -> None:
    from pytest import raises
    from pyuavcan.transport import SessionSpecifier, MessageDataSpecifier, ServiceDataSpecifier, PayloadMetadata

    with raises(ValueError):
        MessageDataSpecifier(-1)

    with raises(ValueError):
        MessageDataSpecifier(32768)

    with raises(ValueError):
        ServiceDataSpecifier(-1, ServiceDataSpecifier.Role.CLIENT)

    with raises(ValueError):
        SessionSpecifier(MessageDataSpecifier(123), -1)

    with raises(ValueError):
        PayloadMetadata(-1, 0)

    with raises(ValueError):
        PayloadMetadata(2 ** 64, 0)

    with raises(ValueError):
        PayloadMetadata(0, -1)
