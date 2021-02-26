#!/usr/bin/env python3
# Distributed under CC0 1.0 Universal (CC0 1.0) Public Domain Dedication.
"""
This application simulates the plant controlled by the thermostat node: it takes a voltage command,
runs a crude thermodynamics simulation, and publishes the temperature (i.e., one subscription, one publication).
"""

import time
import asyncio
import uavcan.si.unit.voltage
import uavcan.si.sample.temperature
import uavcan.time
import pyuavcan
from pyuavcan.application.heartbeat_publisher import Health
from pyuavcan.application import make_node, NodeInfo, register


UPDATE_PERIOD = 0.5

heater_voltage = 0.0
saturation = False


async def handle_command(msg: uavcan.si.unit.voltage.Scalar_1_0, _metadata: pyuavcan.transport.TransferFrom) -> None:
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
    with make_node(NodeInfo(name="org.uavcan.pyuavcan.demo.plant"), "plant.db") as node:
        # Expose internal states for diagnostics.
        node.registry["status.saturation"] = lambda: register.Value(bit=register.Bit([saturation]))

        # Initialize values from the registry.
        temp_environment = float(  # [kelvin]
            node.registry.setdefault("model.environment.temperature", register.Value(real32=register.Real32([292.15])))
        )
        temp_plant = temp_environment

        # Set up the ports.
        pub_meas = node.make_publisher(uavcan.si.sample.temperature.Scalar_1_0, "temperature")
        pub_meas.priority = pyuavcan.transport.Priority.HIGH
        sub_volt = node.make_subscriber(uavcan.si.unit.voltage.Scalar_1_0, "voltage")
        sub_volt.receive_in_background(handle_command)

        # Run the main loop forever.
        next_update_at = node.loop.time()
        while True:
            # Publish new measurement and update node health.
            await pub_meas.publish(
                uavcan.si.sample.temperature.Scalar_1_0(
                    timestamp=uavcan.time.SynchronizedTimestamp_1_0(microsecond=int(time.time() * 1e6)),
                    kelvin=temp_plant,
                )
            )
            node.heartbeat_publisher.health = Health.ADVISORY if saturation else Health.NOMINAL

            # Sleep until the next iteration.
            next_update_at += UPDATE_PERIOD
            await asyncio.sleep(next_update_at - node.loop.time())

            # Update the simulation.
            temp_plant += heater_voltage * 0.1 * UPDATE_PERIOD  # Energy input from the heater.
            temp_plant -= (temp_plant - temp_environment) * 0.05 * UPDATE_PERIOD  # Dissipation.


if __name__ == "__main__":
    try:
        asyncio.get_event_loop().run_until_complete(main())
    except KeyboardInterrupt:
        pass
