#!/usr/bin/env python3
# Distributed under CC0 1.0 Universal (CC0 1.0) Public Domain Dedication.
# pylint: disable=ungrouped-imports,wrong-import-position

import os
import sys
import pathlib
import asyncio
import logging
import importlib
import pyuavcan

# Production applications are recommended to compile their DSDL namespaces as part of the build process. The enclosed
# file "setup.py" provides an example of how to do that. The output path we specify here shall match that of "setup.py".
# Here we use lazy generation to demonstrate an alternative.
compiled_dsdl_dir = pathlib.Path(__file__).resolve().parent / ".demo_dsdl_compiled"

# Make the compilation outputs importable. Let your IDE index this directory as sources to enable code completion.
sys.path.insert(0, str(compiled_dsdl_dir))

try:
    import sirius_cyber_corp  # This is our vendor-specific root namespace. Custom data types.
    import pyuavcan.application  # This module requires the root namespace "uavcan" to be transcompiled.
except (ImportError, AttributeError):  # Redistributable applications typically don't need this section.
    logging.warning("Transcompiling DSDL, this may take a while")
    src_dir = pathlib.Path(__file__).resolve().parent
    pyuavcan.dsdl.compile_all(
        [
            src_dir / "custom_data_types/sirius_cyber_corp",
            src_dir / "public_regulated_data_types/uavcan/",
        ],
        output_directory=compiled_dsdl_dir,
    )
    importlib.invalidate_caches()  # Python runtime requires this.
    import sirius_cyber_corp
    import pyuavcan.application

# Import other namespaces we're planning to use. Nested namespaces are not auto-imported, so in order to reach,
# say, "uavcan.node.Heartbeat", you have to "import uavcan.node".
import uavcan.node  # noqa
import uavcan.si.sample.temperature  # noqa
import uavcan.si.unit.temperature  # noqa
import uavcan.si.unit.voltage  # noqa


class DemoApplication:
    REGISTER_FILE = "demo_app.db"
    """
    The register file stores configuration parameters of the local application/node. The registers can be modified
    at launch via environment variables and at runtime via RPC-service "uavcan.register.Access".
    The file will be created automatically if it doesn't exist.
    """

    def __init__(self) -> None:
        from pyuavcan.application.register import Value, Real32

        node_info = uavcan.node.GetInfo_1_0.Response(
            software_version=uavcan.node.Version_1_0(major=1, minor=0),
            name="org.uavcan.pyuavcan.demo.demo_app",
        )
        # The Node class is basically the central part of the library -- it is the bridge between the application and
        # the UAVCAN network. Also, it implements certain standard application-layer functions, such as publishing
        # heartbeats and port introspection messages, responding to GetInfo, serving the register API, etc.
        # The file "my_registers.db" stores the registers of our node (see DSDL namespace uavcan.register).
        # This is optional though; if the application does not require persistent states, this parameter may be omitted,
        # in which case the register file will be stored in-memory.
        self._node = pyuavcan.application.make_node(
            node_info,
            DemoApplication.REGISTER_FILE,
            {  # Register types and defaults are defined at the initialization stage like this.
                "thermostat.pid.gains": Value(real32=Real32([0.12, 0.18, 0.01])),
            },
        )

        # Published heartbeat fields can be configured as follows.
        self._node.heartbeat_publisher.mode = uavcan.node.Mode_1_0.OPERATIONAL  # type: ignore
        self._node.heartbeat_publisher.vendor_specific_status_code = os.getpid() % 100

        # Now we can create ports to interact with the network.
        # They can also be created or destroyed later at any point after initialization.
        # A port is created by specifying its data type and its name (similar to topic names in ROS or DDS).
        # The subject-ID is obtained from the standard register named "uavcan.sub.temperature_setpoint.id".
        # The register can also be modified via environment variable "UAVCAN__SUB__TEMPERATURE_SETPOINT__ID__NATURAL16".
        self._sub_t_sp = self._node.make_subscriber(uavcan.si.unit.temperature.Scalar_1_0, "temperature_setpoint")

        # As you may probably guess by looking at the port names, we are building a basic thermostat here.
        # We subscribe to the temperature setpoint, temperature measurement (process variable), and publish voltage.
        # The corresponding registers are "uavcan.sub.temperature_measurement.id" and "uavcan.pub.heater_voltage.id".
        self._sub_t_pv = self._node.make_subscriber(uavcan.si.sample.temperature.Scalar_1_0, "temperature_measurement")
        self._pub_v_cmd = self._node.make_publisher(uavcan.si.unit.voltage.Scalar_1_0, "heater_voltage")

        # Create an RPC-server. The service-ID is read from standard register "uavcan.srv.least_squares.id".
        # This service is optional: if the service-ID is not specified, we simply don't provide it.
        try:
            srv_least_sq = self._node.get_server(sirius_cyber_corp.PerformLinearLeastSquaresFit_1_0, "least_squares")
            srv_least_sq.serve_in_background(self._serve_linear_least_squares)
        except pyuavcan.application.register.MissingRegisterError:
            logging.info("The least squares service is disabled by configuration")

        # Create another RPC-server using a standard service type for which a fixed service-ID is defined.
        # We don't specify the port name so the service-ID defaults to the fixed port-ID.
        # We could, of course, use it with a different service-ID as well, if needed.
        self._node.get_server(uavcan.node.ExecuteCommand_1_1).serve_in_background(self._serve_execute_command)

        self._node.start()  # Don't forget to start the node!

    @staticmethod
    async def _serve_linear_least_squares(
        request: sirius_cyber_corp.PerformLinearLeastSquaresFit_1_0.Request,
        metadata: pyuavcan.presentation.ServiceRequestMetadata,
    ) -> sirius_cyber_corp.PerformLinearLeastSquaresFit_1_0.Response:
        logging.info("Least squares request %s from node %d", request, metadata.client_node_id)
        sum_x = sum(map(lambda p: p.x, request.points))  # type: ignore
        sum_y = sum(map(lambda p: p.y, request.points))  # type: ignore
        a = sum_x * sum_y - len(request.points) * sum(map(lambda p: p.x * p.y, request.points))  # type: ignore
        b = sum_x * sum_x - len(request.points) * sum(map(lambda p: p.x ** 2, request.points))  # type: ignore
        try:
            slope = a / b
            y_intercept = (sum_y - slope * sum_x) / len(request.points)
        except ZeroDivisionError:
            slope = float("nan")
            y_intercept = float("nan")
        return sirius_cyber_corp.PerformLinearLeastSquaresFit_1_0.Response(slope=slope, y_intercept=y_intercept)

    @staticmethod
    async def _serve_execute_command(
        request: uavcan.node.ExecuteCommand_1_1.Request,
        metadata: pyuavcan.presentation.ServiceRequestMetadata,
    ) -> uavcan.node.ExecuteCommand_1_1.Response:
        logging.info("Execute command request %s from node %d", request, metadata.client_node_id)
        if request.command == uavcan.node.ExecuteCommand_1_1.Request.COMMAND_FACTORY_RESET:
            try:
                os.unlink(DemoApplication.REGISTER_FILE)  # Reset to defaults by removing the register file.
            except OSError:  # Do nothing if already removed.
                pass
            return uavcan.node.ExecuteCommand_1_1.Response(uavcan.node.ExecuteCommand_1_1.Response.STATUS_SUCCESS)
        return uavcan.node.ExecuteCommand_1_1.Response(uavcan.node.ExecuteCommand_1_1.Response.STATUS_BAD_COMMAND)

    async def run(self) -> None:
        """
        The main method that runs the business logic. It is also possible to use the library in an IoC-style
        by using receive_in_background() for all subscriptions if desired.
        """
        from pyuavcan.application.register import Value, Real32

        temperature_setpoint = 0.0
        temperature_error = 0.0

        # Expose internal states to external observers for diagnostic purposes. Here, we define read-only registers.
        # Since they are computed at every invocation, they are never stored in the register file.
        self._node.new_register("thermostat.error", lambda: Value(real32=Real32([temperature_error])))
        self._node.new_register("thermostat.setpoint", lambda: Value(real32=Real32([temperature_setpoint])))

        async def on_setpoint(msg: uavcan.si.unit.temperature.Scalar_1_0, _: pyuavcan.transport.TransferFrom) -> None:
            nonlocal temperature_setpoint
            temperature_setpoint = msg.kelvin

        self._sub_t_sp.receive_in_background(on_setpoint)  # IoC-style handler.

        # Read the application settings from the registry.
        gain_p, gain_i, gain_d = self._node.registry["thermostat.pid.gains"].floats

        logging.info("Application started with PID gains: %.3f %.3f %.3f", gain_p, gain_i, gain_d)

        # This loop will exit automatically when the node is close()d. It is also possible to use receive() instead.
        async for m, _metadata in self._sub_t_pv:
            assert isinstance(m, uavcan.si.sample.temperature.Scalar_1_0)
            temperature_error = temperature_setpoint - m.kelvin
            voltage_output = temperature_error * gain_p  # Suppose this is a basic P-controller.
            await self._pub_v_cmd.publish(uavcan.si.unit.voltage.Scalar_1_0(voltage_output))

    def close(self) -> None:
        """
        This will close all the underlying resources down to the transport interface and all publishers/servers/etc.
        All pending tasks such as serve_in_background()/receive_in_background() will notice this and exit automatically.
        """
        self._node.close()


if __name__ == "__main__":
    app = DemoApplication()
    logging.root.setLevel(logging.INFO)
    try:
        asyncio.get_event_loop().run_until_complete(app.run())
    except KeyboardInterrupt:
        pass
    finally:
        app.close()
