Installation
============

The recommended way is to install PyUAVCAN from PIP:

.. code-block:: shell

   pip install pyuavcan

It is also possible to integrate the library by submoduling its git repository or otherwise embedding its sources
directly into your application's codebase.
If you prefer this approach, make sure to include its dependencies as well:

- `Nunavut <https://nunavut.readthedocs.io>`_ (a DSDL code generation library)
- `NumPy <https://www.numpy.org/>`_

PyUAVCAN requires **Python v3.7** or newer.

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
