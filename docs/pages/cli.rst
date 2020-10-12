.. _cli:

Command-line tool
=================

The command-line tool can be invoked in one of the following ways:

- Using its full name, same as the library: ``pyuavcan``.
- Using the alias ``uvc``. It's easier to type, but may conflict with other commands under the same name,
  so its availability depends on the configuration of the local system.
  The acronym ``uvc`` stands for *Uncomplicated Vehicular Computing*, which derives from the full form
  *Uncomplicated Application-level Vehicular Computing and Networking* (UAVCAN).
- By explicitly invoking the Python package: ``python -m pyuavcan``.

There is an unlisted optional dependency ``coloredlogs``.
As the name suggests, if this library is installed, the log messages emitted into stderr by the CLI tool
will be nicely colored.

The information contained below can also be accessed via ``--help``.

.. computron-injection::
    :filename: synth/cli_help.py
