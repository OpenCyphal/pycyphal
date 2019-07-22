#!/usr/bin/env python3
#
# A basic PyUAVCAN demo. This file is included in the user documentation, please keep it tidy.
#
# Distributed under CC0 1.0 Universal (CC0 1.0) Public Domain Dedication. To the extent possible under law, the
# UAVCAN Development Team has waived all copyright and related or neighboring rights to this work.
#

import os
import sys
import typing
import pathlib
import asyncio
import tempfile
import importlib
import pyuavcan
# Explicitly import transports and media sub-layers that we're going to use.
import pyuavcan.transport.can
import pyuavcan.transport.can.media.socketcan

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
# Beware that the directory may have to be cleaned manually when you update any of your namespaces.
dsdl_generated_dir = pathlib.Path(tempfile.gettempdir(), 'dsdl-for-my-program', f'pyuavcan-v{pyuavcan.__version__}')
dsdl_generated_dir.mkdir(parents=True, exist_ok=True)
print('Generated DSDL packages will be stored in:', dsdl_generated_dir, file=sys.stderr)

# We will need to import the packages once they are generated, so we should update the module import look-up path set.
# If you're using an IDE for development, add this path to its look-up set as well for code completion to work.
sys.path.insert(0, str(dsdl_generated_dir))

# Now we can import our packages. If import fails, invoke the code generator, then import again.
try:
    import sirius_cyber_corp     # This is our vendor-specific root namespace. Custom data types.
    import pyuavcan.application  # The application module requires the standard types from the root namespace "uavcan".
except ImportError:
    script_path = os.path.abspath(os.path.dirname(__file__))
    # Generate our vendor-specific namespace. It may make use of the standard data types (most namespaces do,
    # because the standard root namespace contains important basic types), so we include it in the lookup path set.
    # The paths are hard-coded here for the sake of conciseness.
    pyuavcan.dsdl.generate_package(
        package_parent_directory=dsdl_generated_dir,
        root_namespace_directory=os.path.join(script_path, '../dsdl/namespaces/sirius_cyber_corp/'),
        lookup_directories=[os.path.join(script_path, '../public_regulated_data_types/uavcan')]
    )
    # Generate the standard namespace. The order actually doesn't matter.
    pyuavcan.dsdl.generate_package(
        package_parent_directory=dsdl_generated_dir,
        root_namespace_directory=os.path.join(script_path, '../public_regulated_data_types/uavcan'),
        lookup_directories=[]
    )
    # Okay, we can try importing again. We need to clear the import cache first because Python's import machinery
    # requires that; see the docs for importlib.invalidate_caches() for more info.
    importlib.invalidate_caches()
    import sirius_cyber_corp
    import pyuavcan.application

# Import other namespaces we're planning to use. Nested namespaces are not auto-imported, so in order to reach,
# say, "uavcan.node.Heartbeat", you have to do "import uavcan.node".
import uavcan.node              # noqa E402
import uavcan.diagnostic        # noqa E402
import uavcan.si.temperature    # noqa E402


class DemoApplication:
    def __init__(self):
        if sys.platform == 'linux':
            # Make sure to initialize the virtual CAN interface. For example (run as root):
            #   modprobe vcan
            #   ip link add dev vcan0 type vcan
            #   ip link set up vcan0
            #   ip link set vcan0 mtu 72
            #   ifconfig vcan0 up
            # CAN interfaces can me monitored using can-utils:
            #   candump -decaxta any
            media = pyuavcan.transport.can.media.socketcan.SocketCANMedia('vcan0', mtu=64)
            transport = pyuavcan.transport.can.CANTransport(media)

        elif 'win' in sys.platform:
            raise RuntimeError('This demo does not yet support MS Windows; please submit patches!')

        else:
            raise RuntimeError(f'Unknown platform: {sys.platform!r}')

        # Populate the node info for use with the Node class. Please see the DSDL definition of uavcan.node.GetInfo.
        node_info = uavcan.node.GetInfo_1_0.Response(
            # Version of the protocol supported by the library, and hence by our node.
            protocol_version=uavcan.node.Version_1_0(*pyuavcan.UAVCAN_SPECIFICATION_VERSION),
            # There is a similar field for hardware version, but we don't populate it because it's a software-only node.
            software_version=uavcan.node.Version_1_0(major=1, minor=0),
            # The name of the local node. Should be a reversed Internet domain name, like a Java package.
            name='org.uavcan.pyuavcan.demo.basic_usage',
            # We've left the optional fields default-initialized here.
        )

        # That's it, here is our node, immediately ready to be used. It will serve GetInfo requests and publish its
        # heartbeat automatically (unless it's anonymous). Read the source code of the Node class for more details.
        self._node = pyuavcan.application.Node(transport, node_info)

        # Published heartbeat fields can be configured trivially by assigning them on the heartbeat publisher instance.
        self._node.heartbeat_publisher.mode = uavcan.node.Heartbeat_1_0.MODE_OPERATIONAL
        # In this example here we assign the local process' PID to the vendor-specific status code (VSSC) and make
        # sure that the valid range is not exceeded.
        self._node.heartbeat_publisher.vendor_specific_status_code = \
            os.getpid() & (2 ** min(pyuavcan.dsdl.get_model(uavcan.node.Heartbeat_1_0)[
                'vendor_specific_status_code'].data_type.bit_length_set) - 1)

        # Now we can create our session objects as necessary. They can be created or destroyed later at any point
        # after initialization. It's not necessary to set everything up during the initialization.
        srv_least_squares = self._node.get_server(sirius_cyber_corp.PerformLinearLeastSquaresFit_1_0, 123)
        # Will run until self._node is close()d:
        srv_least_squares.serve_in_background(self._serve_linear_least_squares_request)

        # Create another server using shorthand for fixed port ID. We could also use it with an application-specific
        # service-ID as well, of course:
        #   get_server(uavcan.node.ExecuteCommand_1_0, 42).serve_in_background(self._serve_execute_command)
        self._node.get_server_with_fixed_service_id(
            uavcan.node.ExecuteCommand_1_0
        ).serve_in_background(self._serve_execute_command)

        # By default, the node operates in anonymous mode, without a node-ID.
        # In this mode, some of the protocol features are unavailable (read Specification for more info).
        # For example, anonymous node cannot be a server, since without an ID it cannot be addressed.
        assert self._node.local_node_id is None

        # Here, we assign a node-ID statically, because this is a simplified demo. Most applications would need this
        # to be configurable, some may support the plug-and-play node-ID allocation protocol.
        self._node.set_local_node_id(42)

        # We'll be publishing diagnostic messages using this publisher instance. The method we use is a shortcut for:
        #   make_publisher(uavcan.diagnostic.Record_1_0, pyuavcan.dsdl.get_fixed_port_id(uavcan.diagnostic.Record_1_0))
        self._pub_diagnostic_record = self._node.make_publisher_with_fixed_subject_id(uavcan.diagnostic.Record_1_0)
        self._pub_diagnostic_record.priority = pyuavcan.transport.Priority.OPTIONAL
        self._pub_diagnostic_record.send_timeout = 2.0

        # A message subscription.
        self._sub_temperature = self._node.make_subscriber(uavcan.si.temperature.Scalar_1_0, 12345)
        self._sub_temperature.receive_in_background(self._handle_temperature)

    async def _serve_linear_least_squares_request(self,
                                                  request:  sirius_cyber_corp.PerformLinearLeastSquaresFit_1_0.Request,
                                                  metadata: pyuavcan.presentation.ServiceRequestMetadata) \
            -> typing.Optional[sirius_cyber_corp.PerformLinearLeastSquaresFit_1_0.Response]:
        """
        This is the request handler for the linear least squares service. The request is passed in along with its
        metadata (the second argument); the response is returned back. We can also return None to instruct the library
        that this request need not be answered (as if the request was never received).
        If this handler raises an exception, it will be suppressed and logged, and no response will be sent back.
        Notice that this is an async function.
        """
        # Publish the message like this. Here, we use await, blocking this task until the message is pushed down to
        # the media layer. This normally should not be done from within a service handler because it may make the
        # service call to time out. Instead use publish_soon(), as shown below.
        diagnostic_msg = uavcan.diagnostic.Record_1_0(
            severity=uavcan.diagnostic.Severity_1_0(uavcan.diagnostic.Severity_1_0.DEBUG),
            text=f'Least squares request from {metadata.client_node_id} time={metadata.timestamp.system} '
                 f'tid={metadata.transfer_id} prio={metadata.priority}',
        )
        if not await self._pub_diagnostic_record.publish(diagnostic_msg):
            print('Diagnostic message could not be sent in', self._pub_diagnostic_record.send_timeout, 'seconds',
                  file=sys.stderr)

        # This is just the business logic.
        sum_x = sum(map(lambda p: p.x, request.points))
        sum_y = sum(map(lambda p: p.y, request.points))
        a = sum_x * sum_y - len(request.points) * sum(map(lambda p: p.x * p.y, request.points))
        b = sum_x * sum_x - len(request.points) * sum(map(lambda p: p.x ** 2, request.points))
        try:
            slope = a / b
            y_intercept = (sum_y - slope * sum_x) / len(request.points)
        except ZeroDivisionError:
            # The method "publish_soon()" launches a background task instead of waiting for the operation to complete.
            self._pub_diagnostic_record.publish_soon(uavcan.diagnostic.Record_1_0(
                severity=uavcan.diagnostic.Severity_1_0(uavcan.diagnostic.Severity_1_0.WARNING),
                text=f'There is no solution for input set: {request.points}',
            ))
            # We return None, no response will be sent back. This practice is actually discouraged; we do it here
            # only to demonstrate the library capabilities.
            return None
        else:
            self._pub_diagnostic_record.publish_soon(uavcan.diagnostic.Record_1_0(
                severity=uavcan.diagnostic.Severity_1_0(uavcan.diagnostic.Severity_1_0.INFO),
                text=f'Solution for {",".join(f"({p.x},{p.y})" for p in request.points)}: {slope}, {y_intercept}',
            ))
            return sirius_cyber_corp.PerformLinearLeastSquaresFit_1_0.Response(slope=slope, y_intercept=y_intercept)

    async def _serve_execute_command(self,
                                     request:  uavcan.node.ExecuteCommand_1_0.Request,
                                     metadata: pyuavcan.presentation.ServiceRequestMetadata) \
            -> uavcan.node.ExecuteCommand_1_0.Response:
        """
        This is another service handler, like the other one.
        """
        print(f'EXECUTE COMMAND REQUEST {request} (with metadata {metadata})')

        if request.command == uavcan.node.ExecuteCommand_1_0.Request.COMMAND_POWER_OFF:
            async def do_delayed_shutdown() -> None:
                await asyncio.sleep(1.0)
                # This will close the underlying presentation, transport, and media layer resources.
                # All pending tasks such as serve_in_background() will notice this and exit automatically.
                # This is convenient as it relieves the application from having to keep track of all objects.
                self._node.close()

            asyncio.ensure_future(do_delayed_shutdown())  # Delay shutdown to let the transport emit the response.
            return uavcan.node.ExecuteCommand_1_0.Response(uavcan.node.ExecuteCommand_1_0.Response.STATUS_SUCCESS)

        elif request.command == 23456:
            # This is a custom application-specific command. Just print the string parameter and do nothing.
            parameter_text = request.parameter.tobytes().decode(errors='replace')
            print('CUSTOM COMMAND PARAMETER:', parameter_text)
            return uavcan.node.ExecuteCommand_1_0.Response(uavcan.node.ExecuteCommand_1_0.Response.STATUS_SUCCESS)

        else:
            # Command not supported.
            return uavcan.node.ExecuteCommand_1_0.Response(uavcan.node.ExecuteCommand_1_0.Response.STATUS_BAD_COMMAND)

    async def _handle_temperature(self,
                                  msg:      uavcan.si.temperature.Scalar_1_0,
                                  metadata: pyuavcan.transport.TransferFrom) -> None:
        """
        A subscription message handler. This is also an async function, so we can block inside if necessary.
        The received message object is passed in along with the information about the transfer that delivered it.
        """
        print('TEMPERATURE', msg.kelvin - 273.15, 'C')

        if not await self._pub_diagnostic_record.publish(uavcan.diagnostic.Record_1_0(
            severity=uavcan.diagnostic.Severity_1_0(uavcan.diagnostic.Severity_1_0.TRACE),
            text=f'Temperature {msg.kelvin:0.3f} K from {metadata.source_node_id} '
                 f'time={metadata.timestamp.system} tid={metadata.transfer_id} prio={metadata.priority}',
        )):
            print('Diagnostic message could not be sent in', self._pub_diagnostic_record.send_timeout, 'seconds',
                  file=sys.stderr)


if __name__ == '__main__':
    app = DemoApplication()
    app_tasks = asyncio.Task.all_tasks()

    async def list_tasks_periodically() -> None:
        """Print active tasks periodically for demo purposes."""
        while True:  # The splitting and slicing mess here is to abridge the strings to make them fit in one line.
            print('Active tasks:\n' + '\n'.join('  ' + str(t).split(' wait_for=')[0].split(' cb=')[0][len('<Task '):]
                                                for t in asyncio.Task.all_tasks()), file=sys.stderr)
            await asyncio.sleep(10)

    asyncio.get_event_loop().create_task(list_tasks_periodically())

    # The node and PyUAVCAN objects have created internal tasks, which we need to run now.
    # In this case we want to automatically stop and exit when no tasks are left to run.
    asyncio.get_event_loop().run_until_complete(asyncio.gather(*app_tasks))
