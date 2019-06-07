#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import pytest
import pyuavcan
import pyuavcan.transport.can
import tests.transport.can


@pytest.mark.asyncio    # type: ignore
async def _unittest_slow_presentation_pub_sub(generated_packages: typing.List[pyuavcan.dsdl.GeneratedPackageInfo]) \
        -> None:
    assert generated_packages
    import uavcan.node
    import uavcan.diagnostic

    bus: typing.Set[tests.transport.can.media.mock.MockMedia] = set()
    media_a = tests.transport.can.media.mock.MockMedia(bus, 8, 1)
    media_b = tests.transport.can.media.mock.MockMedia(bus, 64, 2)      # Look, a heterogeneous setup!
    assert bus == {media_a, media_b}

    tran_a = pyuavcan.transport.can.CANTransport(media_a)
    tran_b = pyuavcan.transport.can.CANTransport(media_b)

    pres_a = pyuavcan.presentation.Presentation(tran_a)
    pres_b = pyuavcan.presentation.Presentation(tran_b)

    pub_heart = await pres_a.get_publisher_with_fixed_subject_id(uavcan.node.Heartbeat_1_0)
    pub_record = await pres_b.get_publisher_with_fixed_subject_id(uavcan.diagnostic.Record_1_0)

    sub_heart = await pres_a.get_subscriber_with_fixed_subject_id(uavcan.node.Heartbeat_1_0)
    sub_record = await pres_a.get_subscriber_with_fixed_subject_id(uavcan.diagnostic.Record_1_0)

    pass
