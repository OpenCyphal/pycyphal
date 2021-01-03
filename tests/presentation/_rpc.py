# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing
import asyncio
import pytest
import pyuavcan
from . import TRANSPORT_FACTORIES, TransportFactory


# noinspection PyProtectedMember
@pytest.mark.parametrize("transport_factory", TRANSPORT_FACTORIES)  # type: ignore
@pytest.mark.asyncio  # type: ignore
async def _unittest_slow_presentation_rpc(
    generated_packages: typing.List[pyuavcan.dsdl.GeneratedPackageInfo], transport_factory: TransportFactory
) -> None:
    assert generated_packages
    import uavcan.register
    import uavcan.primitive
    import uavcan.time
    from pyuavcan.transport import Priority, Timestamp

    asyncio.get_running_loop().slow_callback_duration = 5.0

    tran_a, tran_b, _ = transport_factory(123, 42)
    assert tran_a.local_node_id == 123
    assert tran_b.local_node_id == 42

    pres_a = pyuavcan.presentation.Presentation(tran_a)
    pres_b = pyuavcan.presentation.Presentation(tran_b)

    assert pres_a.transport is tran_a

    server = pres_a.get_server_with_fixed_service_id(uavcan.register.Access_1_0)
    assert server is pres_a.get_server_with_fixed_service_id(uavcan.register.Access_1_0)

    client0 = pres_b.make_client_with_fixed_service_id(uavcan.register.Access_1_0, 123)
    client1 = pres_b.make_client_with_fixed_service_id(uavcan.register.Access_1_0, 123)
    client_dead = pres_b.make_client_with_fixed_service_id(uavcan.register.Access_1_0, 111)
    assert client0 is not client1
    assert client0._maybe_impl is not None  # pylint: disable=protected-access
    assert client1._maybe_impl is not None  # pylint: disable=protected-access
    assert client0._maybe_impl is client1._maybe_impl  # pylint: disable=protected-access
    assert client0._maybe_impl is not client_dead._maybe_impl  # pylint: disable=protected-access
    assert client0._maybe_impl.proxy_count == 2  # pylint: disable=protected-access
    assert client_dead._maybe_impl is not None  # pylint: disable=protected-access
    assert client_dead._maybe_impl.proxy_count == 1  # pylint: disable=protected-access

    with pytest.raises(TypeError):
        # noinspection PyTypeChecker
        pres_a.make_publisher_with_fixed_subject_id(uavcan.register.Access_1_0)  # type: ignore
    with pytest.raises(TypeError):
        # noinspection PyTypeChecker
        pres_a.make_subscriber_with_fixed_subject_id(uavcan.register.Access_1_0)  # type: ignore

    assert client0.response_timeout == pytest.approx(1.0)
    client0.response_timeout = 0.1
    assert client0.response_timeout == pytest.approx(0.1)
    client0.priority = Priority.SLOW

    last_request = uavcan.register.Access_1_0.Request()
    last_metadata = pyuavcan.presentation.ServiceRequestMetadata(
        timestamp=Timestamp(0, 0), priority=Priority(0), transfer_id=0, client_node_id=0
    )
    response: typing.Optional[uavcan.register.Access_1_0.Response] = None

    async def server_handler(
        request: uavcan.register.Access_1_0.Request, metadata: pyuavcan.presentation.ServiceRequestMetadata
    ) -> typing.Optional[uavcan.register.Access_1_0.Response]:
        nonlocal last_metadata
        print("SERVICE REQUEST:", request, metadata)
        assert isinstance(request, server.dtype.Request) and isinstance(request, uavcan.register.Access_1_0.Request)
        assert repr(last_request) == repr(request)
        last_metadata = metadata
        return response

    server.serve_in_background(server_handler)

    last_request = uavcan.register.Access_1_0.Request(
        name=uavcan.register.Name_1_0("Hello world!"),
        value=uavcan.register.Value_1_0(string=uavcan.primitive.String_1_0("Profanity will not be tolerated")),
    )
    result_a = await client0.call(last_request)
    assert result_a is None, "Expected to fail"
    assert last_metadata.client_node_id == 42
    assert last_metadata.transfer_id == 0
    assert last_metadata.priority == Priority.SLOW

    client0.response_timeout = 2.0  # Increase the timeout back because otherwise the test fails on slow systems.

    last_request = uavcan.register.Access_1_0.Request(name=uavcan.register.Name_1_0("security.uber_secure_password"))
    response = uavcan.register.Access_1_0.Response(
        timestamp=uavcan.time.SynchronizedTimestamp_1_0(123456789),
        mutable=True,
        persistent=False,
        value=uavcan.register.Value_1_0(string=uavcan.primitive.String_1_0("hunter2")),
    )
    client0.priority = Priority.IMMEDIATE
    result_b = (await client0.call(last_request))[0]  # type: ignore
    assert repr(result_b) == repr(response)
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

    # Make sure the transport sessions have been closed properly, this is supremely important.
    assert list(pres_a.transport.input_sessions) == []
    assert list(pres_b.transport.input_sessions) == []
    assert list(pres_a.transport.output_sessions) == []
    assert list(pres_b.transport.output_sessions) == []

    pres_a.close()
    pres_b.close()

    await asyncio.sleep(1)  # Let all pending tasks finalize properly to avoid stack traces in the output.
