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
# Explicitly import transports and media sub-layers that we may need here.
import pyuavcan.transport.can
import pyuavcan.transport.can.media.socketcan
import pyuavcan.transport.serial
import pyuavcan.transport.udp
import pyuavcan.transport.redundant

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
except (ImportError, AttributeError):
    script_path = os.path.abspath(os.path.dirname(__file__))
    # Generate our vendor-specific namespace. It may make use of the standard data types (most namespaces do,
    # because the standard root namespace contains important basic types), so we include it in the lookup path set.
    # The paths are hard-coded here for the sake of conciseness.
    pyuavcan.dsdl.generate_package(
        root_namespace_directory=os.path.join(script_path, '../dsdl/namespaces/sirius_cyber_corp/'),
        lookup_directories=[os.path.join(script_path, '../public_regulated_data_types/uavcan')],
        output_directory=dsdl_generated_dir,
    )
    # Generate the standard namespace. The order actually doesn't matter.
    pyuavcan.dsdl.generate_package(
        root_namespace_directory=os.path.join(script_path, '../public_regulated_data_types/uavcan'),
        output_directory=dsdl_generated_dir,
    )
    # Okay, we can try importing again. We need to clear the import cache first because Python's import machinery
    # requires that; see the docs for importlib.invalidate_caches() for more info.
    importlib.invalidate_caches()
    import sirius_cyber_corp
    import pyuavcan.application

# Import other namespaces we're planning to use. Nested namespaces are not auto-imported, so in order to reach,
# say, "uavcan.node.Heartbeat", you have to do "import uavcan.node".
import uavcan.node                      # noqa E402
import uavcan.diagnostic                # noqa E402
import uavcan.si.sample.temperature     # noqa E402


class DemoApplication:
    def __init__(self):
        # The interface to run the demo against is selected via the environment variable with a default option provided.
        # Virtual CAN bus is supported only on GNU/Linux, but other interfaces used here should be compatible
        # with at least Windows as well.
        # Frankly, the main reason we need this here is to simplify automatic testing of this demo script.
        # Feel free to remove the selection logic and just hard-code whatever interface you need.
        interface_kind = os.environ.get('DEMO_INTERFACE_KIND', '').lower()
        # The node-ID is configured per transport instance.
        # Some transports (e.g., UDP/IP) derive the node-ID value from the configuration of the underlying layers.
        # Other transports (e.g., CAN or serial) must be provided with the node-ID value explicitly during
        # initialization, or None can be used to select the anonymous mode.
        # Some of the protocol features are unavailable in the anonymous mode (read Specification for more info).
        # For example, anonymous node cannot be a server, since without an ID it cannot be addressed.
        # Here, we assign a node-ID statically, because this is a simplified demo.
        # Most applications would need this to be configurable, some may support the PnP node-ID allocation protocol.
        if interface_kind == 'udp' or not interface_kind:  # This is the default.
            # The UDP/IP transport in this example runs on the local loopback interface, so no setup is needed.
            # The UDP transport requires us to assign the IP address; the node-ID equals the value of several least
            # significant bits of its IP address. If you want an anonymous UDP/IPv4 node, just use the subnet's
            # broadcast address as its local IP address (e.g., 127.255.255.255/8, 192.168.0.255/24, and so on).
            # For more info, please read the API documentation.
            transport = pyuavcan.transport.udp.UDPTransport('127.0.0.42/8')

        elif interface_kind == 'serial':
            # For demo purposes we're using not an actual serial port (which could have been specified like "COM9"
            # for example) but a virtualized TCP/IP tunnel. The background is explained in the API documentation
            # for the serial transport, please read that. For a quick start, just install Ncat (part of Nmap) and run:
            #   ncat --broker --listen -p 50905
            transport = pyuavcan.transport.serial.SerialTransport('socket://localhost:50905', local_node_id=42)

        elif interface_kind == 'can':
            # Make sure to initialize the virtual CAN interface. For example (run as root):
            #   modprobe vcan
            #   ip link add dev vcan0 type vcan
            #   ip link set vcan0 mtu 72
            #   ip link set up vcan0
            # CAN interfaces can me monitored using can-utils:
            #   candump -decaxta any
            # Here we select CAN 2.0 by setting MTU=8 bytes. We can switch to CAN FD by simply increasing the MTU.
            media = pyuavcan.transport.can.media.socketcan.SocketCANMedia('vcan0', mtu=8)
            transport = pyuavcan.transport.can.CANTransport(media, local_node_id=42)

        elif interface_kind == 'can_can_can':
            # One of the selling points of UAVCAN is the built-in support for modular redundancy.
            # In this section, we set up a triply modular redundant (TMR) CAN bus.
            transport = pyuavcan.transport.redundant.RedundantTransport()
            # Like vcan0, this case requires vcan1 and vcan2 to be available as well.
            media_0 = pyuavcan.transport.can.media.socketcan.SocketCANMedia(f'vcan0', mtu=8)
            media_1 = pyuavcan.transport.can.media.socketcan.SocketCANMedia(f'vcan1', mtu=32)
            media_2 = pyuavcan.transport.can.media.socketcan.SocketCANMedia(f'vcan2', mtu=64)
            # All transports in a redundant group MUST share the same node-ID.
            transport.attach_inferior(pyuavcan.transport.can.CANTransport(media_0, local_node_id=42))
            transport.attach_inferior(pyuavcan.transport.can.CANTransport(media_1, local_node_id=42))
            transport.attach_inferior(pyuavcan.transport.can.CANTransport(media_2, local_node_id=42))
            assert len(transport.inferiors) == 3  # Yup, it's a triply redundant transport.

        elif interface_kind == 'udp_serial':
            # UAVCAN supports dissimilar transport redundancy for safety-critical/high-reliability systems.
            # In this example, we set up a transport that operates over UDP and serial concurrently.
            # This is just an example, however. Major advantages of dissimilar redundant architectures
            # may be observed with wired+wireless links used concurrently; see https://forum.uavcan.org/t/557.
            # All transports in a redundant group MUST share the same node-ID.
            transport = pyuavcan.transport.redundant.RedundantTransport()
            transport.attach_inferior(pyuavcan.transport.udp.UDPTransport('127.0.0.42/8'))
            transport.attach_inferior(pyuavcan.transport.serial.SerialTransport('socket://localhost:50905',
                                                                                local_node_id=42))

        else:
            raise RuntimeError(f'Unrecognized interface kind: {interface_kind}')  # pragma: no cover

        assert transport.local_node_id == 42  # Yup, the node-ID is configured.

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

        # The transport layer is ready; next layer up the protocol stack is the presentation layer. Construct it here.
        presentation = pyuavcan.presentation.Presentation(transport)

        # The application layer is next -- construct the node instance. It will serve GetInfo requests and publish its
        # heartbeat automatically (unless it's anonymous). Read the source code of the Node class for more details.
        self._node = pyuavcan.application.Node(presentation, node_info)

        # Published heartbeat fields can be configured trivially by assigning them on the heartbeat publisher instance.
        self._node.heartbeat_publisher.mode = uavcan.node.Heartbeat_1_0.MODE_OPERATIONAL
        # In this example here we assign the local process' PID to the vendor-specific status code (VSSC) and make
        # sure that the valid range is not exceeded.
        self._node.heartbeat_publisher.vendor_specific_status_code = \
            os.getpid() & (2 ** min(pyuavcan.dsdl.get_model(uavcan.node.Heartbeat_1_0)[
                'vendor_specific_status_code'].data_type.bit_length_set) - 1)

        # Now we can create our session objects as necessary. They can be created or destroyed later at any point
        # after initialization. It's not necessary to set everything up during the initialization.
        srv_least_squares = self._node.presentation.get_server(sirius_cyber_corp.PerformLinearLeastSquaresFit_1_0, 123)
        # Will run until self._node is close()d:
        srv_least_squares.serve_in_background(self._serve_linear_least_squares_request)

        # Create another server using shorthand for fixed port ID. We could also use it with an application-specific
        # service-ID as well, of course:
        #   get_server(uavcan.node.ExecuteCommand_1_0, 42).serve_in_background(self._serve_execute_command)
        # If the transport does not yet have a node-ID, the server will stay idle until a node-ID is assigned
        # because the node won't be able to receive unicast transfers carrying service requests.
        self._node.presentation.get_server_with_fixed_service_id(
            uavcan.node.ExecuteCommand_1_0
        ).serve_in_background(self._serve_execute_command)

        # We'll be publishing diagnostic messages using this publisher instance. The method we use is a shortcut for:
        #   make_publisher(uavcan.diagnostic.Record_1_0, pyuavcan.dsdl.get_fixed_port_id(uavcan.diagnostic.Record_1_0))
        self._pub_diagnostic_record = \
            self._node.presentation.make_publisher_with_fixed_subject_id(uavcan.diagnostic.Record_1_0)
        self._pub_diagnostic_record.priority = pyuavcan.transport.Priority.OPTIONAL
        self._pub_diagnostic_record.send_timeout = 2.0

        # A message subscription.
        self._sub_temperature = self._node.presentation.make_subscriber(uavcan.si.sample.temperature.Scalar_1_0, 12345)
        self._sub_temperature.receive_in_background(self._handle_temperature)

        # When all is initialized, don't forget to start the node!
        self._node.start()

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
        # Publish the message asynchronously using publish_soon() because we don't want to block the service handler.
        diagnostic_msg = uavcan.diagnostic.Record_1_0(
            severity=uavcan.diagnostic.Severity_1_0(uavcan.diagnostic.Severity_1_0.DEBUG),
            text=f'Least squares request from {metadata.client_node_id} time={metadata.timestamp.system} '
                 f'tid={metadata.transfer_id} prio={metadata.priority}',
        )
        print('Least squares request:', request, file=sys.stderr)
        self._pub_diagnostic_record.publish_soon(diagnostic_msg)

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
                                  msg:      uavcan.si.sample.temperature.Scalar_1_0,
                                  metadata: pyuavcan.transport.TransferFrom) -> None:
        """
        A subscription message handler. This is also an async function, so we can block inside if necessary.
        The received message object is passed in along with the information about the transfer that delivered it.
        """
        print('TEMPERATURE', msg.kelvin - 273.15, 'C')

        # Publish the message synchronously, using await, blocking this task until the message is pushed down to
        # the media layer.
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
        import re

        def repr_task(t: asyncio.Task) -> str:
            try:
                out, = re.findall(r'^<([^<]+<[^>]+>)', str(t))
            except ValueError:
                out = str(t)
            return out

        while True:
            print('\nActive tasks:\n' + '\n'.join(map(repr_task, asyncio.Task.all_tasks())), file=sys.stderr)
            await asyncio.sleep(10)

    asyncio.get_event_loop().create_task(list_tasks_periodically())

    # The node and PyUAVCAN objects have created internal tasks, which we need to run now.
    # In this case we want to automatically stop and exit when no tasks are left to run.
    asyncio.get_event_loop().run_until_complete(asyncio.gather(*app_tasks))
