#!/bin/bash

status=0

# Initializing the system-wide test environment.
sudo modprobe can
sudo modprobe can_raw
sudo modprobe vcan
sudo ip link add dev vcan0 type vcan &> /dev/null
sudo ip link set up vcan0            &> /dev/null
sudo ip link set vcan0 mtu 72
sudo ifconfig vcan0 up

# Unit testing with coverage.
export PYTHONASYNCIODEBUG=1
# https://docs.pytest.org/en/latest/pythonpath.html#invoking-pytest-versus-python-m-pytest
if coverage run -m pytest $@
then
    coverage combine
    coverage report
    coverage xml -i -o .coverage.xml
else
    status=1
fi

# Static type checking.
if ! mypy --strict --strict-equality --no-implicit-reexport --config-file=setup.cfg
then
    # TODO: re-enable MyPy enforcement when it's fixed. MyPy 0.701 and 0.711 are broken.
    #status=1
    echo "WARNING: MYPY HAS FAILED BUT THE FAILURE IS IGNORED"
fi

# PEP8 code style enforcement.
if ! pycodestyle pyuavcan_cli tests
then
    status=1
fi

exit $status
