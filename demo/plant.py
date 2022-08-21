#!/usr/bin/env python3
# Distributed under CC0 1.0 Universal (CC0 1.0) Public Domain Dedication.
"""
This application simulates the plant controlled by the thermostat node: it takes a voltage command,
runs a crude thermodynamics simulation, and publishes the temperature (i.e., one subscription, one publication).
"""

import time
import asyncio
import pycyphal

# Import DSDL's after pycyphal import hook is installed
import uavcan.si.unit.voltage
import uavcan.si.sample.temperature
import uavcan.time
from pycyphal.application.heartbeat_publisher import Health
from pycyphal.application import make_node, NodeInfo, register


UPDATE_PERIOD = 0.5

heater_voltage = 0.0
saturation = False


def handle_command(msg: uavcan.si.unit.voltage.Scalar_1, _metadata: pycyphal.transport.TransferFrom) -> None:
    global heater_voltage, saturation
    if msg.volt < 0.0:
        heater_voltage = 0.0
        saturation = True
    elif msg.volt > 50.0:
        heater_voltage = 50.0
        saturation = True
    else:
        heater_voltage = msg.volt
        saturation = False


async def main() -> None:
    with make_node(NodeInfo(name="org.opencyphal.pycyphal.demo.plant"), "plant.db") as node:
        # Expose internal states for diagnostics.
        node.registry["status.saturation"] = lambda: saturation  # The register type will be deduced as "bit[1]".

        # Initialize values from the registry. The temperature is in kelvin because in UAVCAN everything follows SI.
        # Here, we specify the type explicitly as "real32[1]". If we pass a native float, it would be "real64[1]".
        temp_environment = float(node.registry.setdefault("model.environment.temperature", register.Real32([292.15])))
        temp_plant = temp_environment

        # Set up the ports.
        pub_meas = node.make_publisher(uavcan.si.sample.temperature.Scalar_1, "temperature")
        pub_meas.priority = pycyphal.transport.Priority.HIGH
        sub_volt = node.make_subscriber(uavcan.si.unit.voltage.Scalar_1, "voltage")
        sub_volt.receive_in_background(handle_command)

        # Run the main loop forever.
        next_update_at = asyncio.get_running_loop().time()
        while True:
            # Publish new measurement and update node health.
            await pub_meas.publish(
                uavcan.si.sample.temperature.Scalar_1(
                    timestamp=uavcan.time.SynchronizedTimestamp_1(microsecond=int(time.time() * 1e6)),
                    kelvin=temp_plant,
                )
            )
            node.heartbeat_publisher.health = Health.ADVISORY if saturation else Health.NOMINAL

            # Sleep until the next iteration.
            next_update_at += UPDATE_PERIOD
            await asyncio.sleep(next_update_at - asyncio.get_running_loop().time())

            # Update the simulation.
            temp_plant += heater_voltage * 0.1 * UPDATE_PERIOD  # Energy input from the heater.
            temp_plant -= (temp_plant - temp_environment) * 0.05 * UPDATE_PERIOD  # Dissipation.


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
