#!/bin/bash

status=0

# Static type checking
if ! mypy --strict --config-file=setup.cfg pyuavcan
then
    status=1
fi

# Code style checking
if ! pycodestyle --show-source pyuavcan
then
    status=1
fi

# Unit tests
if coverage run --source pyuavcan -m pytest --capture=no -vv pyuavcan
then
    coverage report
else
    status=1
fi

exit $status
