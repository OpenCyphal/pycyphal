Architecture
============

.. computron_injection::

    import re, pyuavcan
    pyuavcan.util.import_submodules(pyuavcan.transport)
    for cls in pyuavcan.util.iter_descendants(pyuavcan.transport.Transport):
        export_module_name = re.sub(r'\._[_a-zA-Z0-9]*', '', cls.__module__)
        print(f'- :class:`{export_module_name}.{cls.__name__}`')
