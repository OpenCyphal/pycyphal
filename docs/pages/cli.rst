.. _cli:

Command-line tool
=================

The information contained here can also be accessed via ``--help``.

The command-line tool has an alias named ``uc``.
It's easier to type, but may conflict with other commands under the same name,
so its availability depends on the configuration of the local system.

There is an unlisted optional dependency ``coloredlogs``.
As the name suggests, if this library is installed, the log messages emitted into stderr by the CLI tool
will be nicely colored.

.. computron-injection::
    :filename: synth/cli_help.py
