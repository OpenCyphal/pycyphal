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

# Downloading the public regulated types - they are needed for testing.
if [[ ! -d public_regulated_data_types.cache ]]
then
    git clone https://github.com/UAVCAN/public_regulated_data_types --branch=uavcan-v1.0 \
        public_regulated_data_types.cache || exit 1
fi

# Unit testing with coverage.
export PYTHONASYNCIODEBUG=1
# The directory must exist before coverage.py is invoked in order for it to track it.
mkdir .test_dsdl_generated 2> /dev/null
# https://docs.pytest.org/en/latest/pythonpath.html#invoking-pytest-versus-python-m-pytest
if coverage run --source pyuavcan,tests,.test_dsdl_generated -m pytest $@
then
    coverage report
    coverage xml -i -o .coverage.xml
else
    status=1
fi

# Static type checking.
# We postpone type checking until after the tests have run in order to be able to typecheck the
# generated Python packages as well.
export MYPYPATH=".test_dsdl_generated"
if ! mypy --strict --strict-equality --no-implicit-reexport --show-traceback --config-file=setup.cfg \
         pyuavcan tests .test_dsdl_generated
then
    # TODO: re-enable MyPy enforcement when it's fixed. MyPy 0.701 and 0.711 are broken.
    #status=1
    echo "WARNING: MYPY HAS FAILED BUT THE FAILURE IS IGNORED"
fi

# PEP8 code style enforcement.
if ! pycodestyle --show-source pyuavcan tests
then
    status=1
fi

exit $status
