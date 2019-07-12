#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import asyncio

import pytest

import pyuavcan
import pyuavcan.transport.can
import tests.transport.can


# noinspection PyProtectedMember
@pytest.mark.asyncio    # type: ignore
async def _unittest_slow_presentation_rpc(generated_packages: typing.List[pyuavcan.dsdl.GeneratedPackageInfo]) -> None:
    assert generated_packages
    import uavcan.register
    import uavcan.primitive
    import uavcan.time
    from pyuavcan.transport import Priority, Timestamp

    bus: typing.Set[tests.transport.can.media.mock.MockMedia] = set()
    media_a = tests.transport.can.media.mock.MockMedia(bus, 8, 1)
    media_b = tests.transport.can.media.mock.MockMedia(bus, 64, 2)      # Look, a heterogeneous setup!
    assert bus == {media_a, media_b}

    tran_a = pyuavcan.transport.can.CANTransport(media_a)
    tran_b = pyuavcan.transport.can.CANTransport(media_b)

    tran_a.set_local_node_id(123)
    tran_b.set_local_node_id(42)

    pres_a = pyuavcan.presentation.Presentation(tran_a)
    pres_b = pyuavcan.presentation.Presentation(tran_b)

    assert pres_a.transport is tran_a

    server = pres_a.get_server_with_fixed_service_id(uavcan.register.Access_0_1)
    assert server is pres_a.get_server_with_fixed_service_id(uavcan.register.Access_0_1)

    client0 = pres_b.make_client_with_fixed_service_id(uavcan.register.Access_0_1, 123)
    client1 = pres_b.make_client_with_fixed_service_id(uavcan.register.Access_0_1, 123)
    client_dead = pres_b.make_client_with_fixed_service_id(uavcan.register.Access_0_1, 111)
    assert client0 is not client1
    assert client0._maybe_impl is not None
    assert client1._maybe_impl is not None
    assert client0._maybe_impl is client1._maybe_impl
    assert client0._maybe_impl is not client_dead._maybe_impl
    assert client0._maybe_impl.proxy_count == 2
    assert client_dead._maybe_impl.proxy_count == 1

    with pytest.raises(TypeError):
        # noinspection PyTypeChecker
        pres_a.make_publisher_with_fixed_subject_id(uavcan.register.Access_0_1)  # type: ignore
    with pytest.raises(TypeError):
        # noinspection PyTypeChecker
        pres_a.make_subscriber_with_fixed_subject_id(uavcan.register.Access_0_1)  # type: ignore

    assert client0.response_timeout == pytest.approx(1.0)
    client0.response_timeout = 0.1
    assert client0.response_timeout == pytest.approx(0.1)
    client0.priority = Priority.SLOW

    last_request = uavcan.register.Access_0_1.Request()
    last_metadata = pyuavcan.presentation.ServiceRequestMetadata(timestamp=Timestamp(0, 0),
                                                                 priority=Priority(0),
                                                                 transfer_id=0,
                                                                 client_node_id=0)
    response: typing.Optional[uavcan.register.Access_0_1.Response] = None

    async def server_handler(request: uavcan.register.Access_0_1.Request,
                             metadata: pyuavcan.presentation.ServiceRequestMetadata) \
            -> typing.Optional[uavcan.register.Access_0_1.Response]:
        nonlocal last_metadata
        print('SERVICE REQUEST:', request, metadata)
        assert isinstance(request, server.dtype.Request) and isinstance(request, uavcan.register.Access_0_1.Request)
        assert repr(last_request) == repr(request)
        last_metadata = metadata
        return response

    # TODO: fix the type annotations!
    server.serve_in_background(server_handler)  # type: ignore

    last_request = uavcan.register.Access_0_1.Request(
        name=uavcan.register.Name_0_1('Hello world!'),
        value=uavcan.register.Value_0_1(string=uavcan.primitive.String_1_0('Profanity will not be tolerated')))
    # TODO: fix the type annotations!
    result = await client0.try_call_with_transfer(last_request)  # type: ignore
    assert result is None, 'Expected to fail'
    assert last_metadata.client_node_id == 42
    assert last_metadata.transfer_id == 0
    assert last_metadata.priority == Priority.SLOW

    last_request = uavcan.register.Access_0_1.Request(name=uavcan.register.Name_0_1('security.uber_secure_password'))
    response = uavcan.register.Access_0_1.Response(
        timestamp=uavcan.time.SynchronizedTimestamp_1_0(123456789),
        mutable=True,
        persistent=False,
        value=uavcan.register.Value_0_1(string=uavcan.primitive.String_1_0('hunter2'))
    )
    client0.priority = Priority.IMMEDIATE
    # TODO: fix the type annotations!
    result = await client0.try_call(last_request)  # type: ignore
    assert repr(result) == repr(response)
    assert last_metadata.client_node_id == 42
    assert last_metadata.transfer_id == 1
    assert last_metadata.priority == Priority.IMMEDIATE

    server.close()
    client0.close()
    client1.close()
    client_dead.close()
    # Double-close has no effect (no error either):
    server.close()
    client0.close()
    client1.close()
    client_dead.close()

    # Allow the tasks to finish
    await asyncio.sleep(0.1)

    # All disposed of?
    assert list(pres_a.sessions) == []
    assert list(pres_b.sessions) == []

    # Make sure the transport sessions have been closed properly, this is supremely important.
    assert list(pres_a.transport.input_sessions) == []
    assert list(pres_b.transport.input_sessions) == []
    assert list(pres_a.transport.output_sessions) == []
    assert list(pres_b.transport.output_sessions) == []
