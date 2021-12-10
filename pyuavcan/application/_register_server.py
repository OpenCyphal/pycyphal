# Copyright (C) 2021  UAVCAN Consortium  <uavcan.org>
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
from typing import Optional
import logging
import pyuavcan
import pyuavcan.application
from pyuavcan.presentation import ServiceRequestMetadata
from uavcan.register import Access_1_0 as Access
from uavcan.register import List_1_0 as List
from uavcan.register import Name_1_0 as Name
from .register import ValueConversionError, ValueProxyWithFlags


class RegisterServer:
    # noinspection PyUnresolvedReferences,PyTypeChecker
    """
    Implementation of the standard network service ``uavcan.register``; specifically, List and Access.

    This server implements automatic type conversion by invoking
    :meth:`pyuavcan.application.register.ValueProxy.assign` on every set request.
    This means that, for example, one can successfully modify a register of type
    ``bool[x]`` by sending a set request of type ``real64[x]``, or ``string`` with ``unstructured``, etc.

    Here is a demo. Set up a node -- it will instantiate a register server automatically:

    >>> import pyuavcan
    >>> from pyuavcan.transport.loopback import LoopbackTransport
    >>> from pyuavcan.application.register import Registry, Value, ValueProxy, Integer64, Real16, Unstructured
    >>> node = pyuavcan.application.make_node(pyuavcan.application.NodeInfo(), transport=LoopbackTransport(1))
    >>> node.registry.setdefault("foo", Value(integer64=Integer64([1, 20, -100]))).ints
    [1, 20, -100]
    >>> node.start()

    List registers:

    >>> import uavcan.register
    >>> from asyncio import get_event_loop
    >>> cln_list = node.make_client(uavcan.register.List_1_0, server_node_id=1)
    >>> response, _ = get_event_loop().run_until_complete(cln_list.call(uavcan.register.List_1_0.Request(index=0)))
    >>> response.name.name.tobytes().decode()   # The dummy register we created above.
    'foo'
    >>> response, _ = get_event_loop().run_until_complete(cln_list.call(uavcan.register.List_1_0.Request(index=99)))
    >>> response.name.name.tobytes().decode()   # Out of range -- empty string returned to indicate that.
    ''

    Get the dummy register created above:

    >>> cln_access = node.make_client(uavcan.register.Access_1_0, server_node_id=1)
    >>> request = uavcan.register.Access_1_0.Request()
    >>> request.name.name = "foo"
    >>> response, _ = get_event_loop().run_until_complete(cln_access.call(request))
    >>> response.mutable, response.persistent
    (True, False)
    >>> ValueProxy(response.value).ints
    [1, 20, -100]

    Set a new value and read it back.
    Notice that the type does not match but it is automatically converted by the server.

    >>> request.value.real16 = Real16([3.14159, 2.71828, -500])  # <-- the type is different but it's okay.
    >>> response, _ = get_event_loop().run_until_complete(cln_access.call(request))
    >>> ValueProxy(response.value).ints     # Automatically converted.
    [3, 3, -500]
    >>> node.registry["foo"].ints           # Yup, the register is, indeed, updated by the server.
    [3, 3, -500]

    If the type cannot be converted or the register is immutable, the write is ignored,
    as prescribed by the register network service definition:

    >>> request.value.unstructured = Unstructured(b'Hello world!')
    >>> response, _ = get_event_loop().run_until_complete(cln_access.call(request))
    >>> ValueProxy(response.value).ints  # Conversion is not possible, same value retained.
    [3, 3, -500]

    An attempt to access a non-existent register returns an empty value:

    >>> request.name.name = 'bar'
    >>> response, _ = get_event_loop().run_until_complete(cln_access.call(request))
    >>> response.value.empty is not None
    True

    >>> node.close()
    """

    def __init__(self, node: pyuavcan.application.Node) -> None:
        """
        :param node: The node instance to serve the register API for.
        """
        self._node = node

        srv_list = self.node.get_server(List)
        srv_access = self.node.get_server(Access)

        def start() -> None:
            srv_list.serve_in_background(self._handle_list)
            srv_access.serve_in_background(self._handle_access)

        def close() -> None:
            srv_list.close()
            srv_access.close()

        node.add_lifetime_hooks(start, close)

    @property
    def node(self) -> pyuavcan.application.Node:
        return self._node

    async def _handle_list(self, request: List.Request, metadata: ServiceRequestMetadata) -> List.Response:
        name = self.node.registry.index(request.index)
        _logger.debug("%r: List request index %r name %r %r", self, request.index, name, metadata)
        if name is not None:
            return List.Response(Name(name))
        return List.Response()

    async def _handle_access(self, request: Access.Request, metadata: ServiceRequestMetadata) -> Access.Response:
        name = request.name.name.tobytes().decode("utf8", "ignore")
        try:
            v: Optional[ValueProxyWithFlags] = self.node.registry[name]
        except KeyError:
            v = None

        if v is not None and v.mutable and not request.value.empty:
            try:
                v.assign(request.value)
                self.node.registry[name] = v
            except ValueConversionError as ex:
                _logger.debug("%r: Conversion from %r to %r is not possible: %s", self, request.value, v.value, ex)
            # Read back one more time just in case to confirm write.
            try:
                v = self.node.registry[name]
            except KeyError:
                v = None

        if v is not None:
            response = Access.Response(
                mutable=v.mutable,
                persistent=v.persistent,
                value=v.value,
            )
        else:
            response = Access.Response()  # No such register
        _logger.debug("%r: Access %r: %r %r", self, metadata, request, response)
        return response


_logger = logging.getLogger(__name__)
