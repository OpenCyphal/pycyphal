.. _architecture:

Architecture
============

The UAVCAN protocol itself is designed to support multiple transport protocols such as CAN bus, UDP, serial, and so on.
Generally, a real-time safety-critical implementation of UAVCAN would choose to support a limited subset of
transports defined by the protocol, which is a valid strategy for high-reliability software.
PyUAVCAN is different because it is created for user-facing software rather than deeply embedded systems;
that is, PyUAVCAN can't be put onboard a vehicle, but it can be put onto the computer of an engineer or a researcher
building said vehicle to help them implement, understand, validate, verify, and diagnose the onboard network.

Hence, PyUAVCAN trades off simplicity and constraindness (desirable for embedded systems)
for extensibility and repurposeability (desirable for user-facing software).
The library consists of an abstract core which implements the higher levels of the UAVCAN protocol,
DSDL code generation, and object serialization.
These features are generic and transport-agnostic.
The core defines an abstract *transport model* which interfaces it with transport-specific logic.

.. computron-injection::
   :filename: transport_summary.py
