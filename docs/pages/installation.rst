.. _installation:

Installation
============

Install the library from PyPI; the package name is ``pyuavcan``.
Specify the installation options (known as "package extras" in parseltongue)
depending on which UAVCAN transports and features you are planning to use.

Installation options
--------------------

Most of the installation options enable a particular transport or a particular media sublayer implementation
for a transport.
Those options are named uniformly following the pattern
``transport_<transport-name>_<media-name>``, for example: ``transport_can_pythoncan``.
If there is no media sub-layer, or the media dependencies are shared, or there is a common
installation option for all media types of the transport, the media part is omitted from the key;
for example: ``transport_serial``.
Installation options whose names do not begin with ``transport_`` enable other optional features.

.. computron-injection::
   :filename: synth/installation_option_matrix.py

Use from source
---------------

PyUAVCAN requires no unconventional installation steps and is usable directly in its source form.
If installation from PyPI is considered undesirable,
the library sources can be just directly embedded into the user's codebase
(as a git submodule/subtree or copy-paste).

When doing so, don't forget to let others know that you use PyUAVCAN (it's MIT-licensed),
and make sure to include at least its core dependencies, which are:

.. computron-injection::

    import configparser, textwrap
    cp = configparser.ConfigParser()
    cp.read('../setup.cfg')
    print('.. code-block::\n')
    print(textwrap.indent(cp['options']['install_requires'].strip(), ' '))

Legacy considerations
---------------------

A legacy library titled ``uavcan`` (note the lack of the ``py`` prefix) is also available from PyPI,
which implements an early experimental version of the protocol known as UAVCAN v0
that is no longer recommended for new designs.
It should not be confused with this library which implements the long-term stable version
of the protocol known as UAVCAN v1.

Having both ``pyuavcan`` and the old ``uavcan`` libraries installed in the same environment is not recommended,
because PyUAVCAN generates Python packages from DSDL namespaces,
and since the standard DSDL types are stored in the root namespace named ``uavcan``,
it would conflict with the old library.
Hence, when installing PyUAVCAN, make sure you don't have the legacy library around: ``pip uninstall -y uavcan``.
