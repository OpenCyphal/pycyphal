Installation
============

PyUAVCAN requires **Python v3.7** or newer.

The recommended installation approach is to use PIP.
When invoking PIP, specify the installation options (also known as "package extras", in Python speak |:snake:|)
depending on which UAVCAN transports you are planning to use.
The options are specified at the package installation time in square brackets after the package name.

Installation options
--------------------

.. computron-injection::
   :filename: installation_option_matrix.py

Legacy considerations
---------------------

A similar library titled ``uavcan`` (note the lack of the ``py`` prefix) is also available from PIP,
which implements an early experimental version of the protocol known as UAVCAN v0
that is no longer recommended for new designs.
It should not be confused with this library (titled ``pyuavcan``, mind the difference)
which implements the long-term stable version of the protocol known as UAVCAN v1.0.
Further, having both ``pyuavcan`` and the old ``uavcan`` libraries installed in the same environment is actually
not recommended, because PyUAVCAN generates Python packages from DSDL namespaces, and since the standard
DSDL types are stored in the root namespace named ``uavcan``, it would conflict with the old library.
Hence, when installing PyUAVCAN, make sure you don't have the legacy library around: ``pip uninstall -y uavcan``.

Use from source
---------------

PyUAVCAN requires no unconventional installation steps and is usable directly in its source form.
If sourcing the package from PyPI is considered undesirable,
the library sources can be just directly embedded into the user's codebase
(as a git submodule/subtree or otherwise).

When doing so, don't forget to let others know that you use PyUAVCAN (it's MIT-licensed),
and make sure to include at least its core dependencies, which are:

.. computron-injection::

    import configparser, textwrap
    cp = configparser.ConfigParser()
    cp.read('../setup.cfg')
    print('.. code-block::\n')
    print(textwrap.indent(cp['options']['install_requires'].strip(), ' '))
