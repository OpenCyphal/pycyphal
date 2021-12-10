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
``transport-<transport-name>-<media-name>``, for example: ``transport-can-pythoncan``.
If there is no media sub-layer, or the media dependencies are shared, or there is a common
installation option for all media types of the transport, the media part is omitted from the key;
for example: ``transport-serial``.
Installation options whose names do not begin with ``transport-`` enable other optional features.

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
