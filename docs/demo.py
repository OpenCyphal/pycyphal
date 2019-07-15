#!/usr/bin/env python

import sys
import typing
import pathlib
import asyncio
import tempfile
import importlib
import pyuavcan

#
# We will need a directory to store the generated Python packages in.
#
# It is perfectly acceptable to just use a random temp directory at every run, but the disadvantage of that approach
# is that the packages will be re-generated from scratch every time the program is started, which may be undesirable.
#
# So in this example we select a fixed temp dir name (make sure it's unique enough) and shard its contents by the
# library version. The sharding helps us ensure that we won't attempt to use a package generated for an older library
# version with a newer one, as they may be incompatible.
#
# Another sensible location for the generated package directory is somewhere in the application data directory,
# like "~/.my-app/dsdl/{pyuavcan.__version__}/"; or, for Windows: "%APPDATA%/my-app/dsdl/{pyuavcan.__version__}/".
#
dsdl_generated_dir = pathlib.Path(tempfile.gettempdir(), 'dsdl-for-my-program', f'pyuavcan-v{pyuavcan.__version__}')
dsdl_generated_dir.mkdir(parents=True, exist_ok=True)
print('Generated DSDL packages will be stored in:', dsdl_generated_dir)

#
# We will need to import the packages once they are generated, so we should update the module import look-up path set.
# If you're using an IDE for development, add this path to its look-up set as well for code completion to work.
#
sys.path.insert(0, str(dsdl_generated_dir))

#
# Now we can import our packages. If import fails, invoke the code generator, then import again.
#
try:
    import sirius_cyber_corp     # This is our vendor-specific root namespace. Custom data types.
    import pyuavcan.application  # The application module requires the standard types from the root namespace "uavcan".
except ImportError:
    # Generate our vendor-specific namespace. It may make use of the standard data types (most namespaces do,
    # because the standard root namespace contains important basic types), so we include it in the lookup path set.
    # The paths are hard-coded here for the sake of conciseness.
    pyuavcan.dsdl.generate_package(package_parent_directory=dsdl_generated_dir,
                                   root_namespace_directory='../tests/dsdl/namespaces/sirius_cyber_corp/',
                                   lookup_directories=[
                                       '../tests/public_regulated_data_types/uavcan',
                                   ])

    # Generate the standard namespace. The order actually doesn't matter.
    pyuavcan.dsdl.generate_package(package_parent_directory=dsdl_generated_dir,
                                   root_namespace_directory='../tests/public_regulated_data_types/uavcan',
                                   lookup_directories=[])

    # Okay, we can try importing again. We need to clear the import cache first because Python's import machinery
    # requires that; see the docs for importlib.invalidate_caches() for more info.
    importlib.invalidate_caches()
    import sirius_cyber_corp
    import pyuavcan.application


async def _serve_linear_least_squares_request(request:  sirius_cyber_corp.PerformLinearLeastSquaresFit_1_0.Request,
                                              metadata: pyuavcan.presentation.ServiceRequestMetadata) \
        -> typing.Optional[sirius_cyber_corp.PerformLinearLeastSquaresFit_1_0.Response]:
    """
    This is the request handler for the linear least squares service. The request is passed in along with its
    metadata (the second argument); the response is returned back. We can also return None to instruct the library
    that this request need not be answered (as if the request was never received).
    Notice that this is an async function.
    """
    # TODO: instead of printing, publish diagnostic records.
    print(f'Least squares request from {metadata.client_node_id} received at {metadata.timestamp.system} '
          f'with priority {metadata.priority} and transfer-ID {metadata.transfer_id}.')
    sum_x = sum(map(lambda p: p.x, request.points))
    sum_y = sum(map(lambda p: p.y, request.points))
    a = sum_x * sum_y - len(request.points) * sum(map(lambda p: p.x * p.y, request.points))
    b = sum_x * sum_x - len(request.points) * sum(map(lambda p: p.x ** 2, request.points))
    try:
        slope = a / b
        y_intercept = (sum_y - slope * sum_x) / len(request.points)
        print(f'Solution for {request.points}: slope={slope}, y_intercept={y_intercept}')
        return sirius_cyber_corp.PerformLinearLeastSquaresFit_1_0.Response(slope=slope, y_intercept=y_intercept)
    except ZeroDivisionError:
        print(f'There is no solution for input set: {request.points}')
        # We return None, no response will be sent back. This practice is actually discouraged; we do it here
        # only to demonstrate the library capabilities.
        return None


def main() -> None:
    import pyuavcan.transport.can
    import pyuavcan.transport.can.media.socketcan

    # Make sure to initialize the virtual CAN interface. For example (run as root):
    #   modprobe vcan
    #   ip link add dev vcan0 type vcan
    #   ip link set up vcan0
    #   ip link set vcan0 mtu 72
    #   ifconfig vcan0 up
    media = pyuavcan.transport.can.media.socketcan.SocketCANMedia('vcan0', mtu=64)
    transport = pyuavcan.transport.can.CANTransport(media)

    # Populate the node info for use with the Node class. Please see the DSDL type definition of uavcan.node.GetInfo.
    import uavcan.node
    node_info = uavcan.node.GetInfo_0_1.Response(
        # Version of the protocol supported by the library, and hence by our node.
        protocol_version=uavcan.node.Version_1_0(*pyuavcan.UAVCAN_SPECIFICATION_VERSION),
        # There is a similar field for hardware version, but we don't populate it because it's a software-only node.
        software_version=uavcan.node.Version_1_0(major=1, minor=0),
        # The name of the local node. Should be a reversed Internet domain name, like a Java package.
        name='org.uavcan.pyuavcan.demo',
        # We've left the optional fields default-initialized here.
    )

    # That's it, here is our node, immediately ready to be used. It will serve GetInfo requests and publish its
    # heartbeat automatically (unless it's anonymous). Read the source code of the Node class for more details.
    node = pyuavcan.application.Node(transport, node_info)

    # Now we can create our session objects as necessary. They can be created or destroyed later at any point
    # after initialization. It's not necessary to set everything up during the initialization.
    least_squares_server = node.get_server(sirius_cyber_corp.PerformLinearLeastSquaresFit_1_0, 123)
    least_squares_server.serve_in_background(_serve_linear_least_squares_request)  # Will run until node is close()d.

    # By default, the node operates in anonymous mode, without a node-ID.
    # In this mode, some of the protocol features are unavailable (read Specification for more info).
    # For example, anonymous node cannot be a server, since without an ID it cannot be addressed.
    assert node.local_node_id is None

    # Here, we assign a node-ID statically, because this is a simplified demo. Most applications would need this
    # to be configurable, some may support the plug-and-play node-ID allocation protocol.
    node.set_local_node_id(42)

    # The node and the server objects have created internal tasks, which we need to run now.
    asyncio.get_event_loop().run_forever()

    # Don't forget to close the node. This will dispose the underlying transport and media resources as well.
    node.close()


if __name__ == '__main__':
    main()
