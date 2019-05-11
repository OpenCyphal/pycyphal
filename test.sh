#!/bin/bash

status=0

# Static type checking
if ! mypy --strict --config-file=setup.cfg pyuavcan tests
then
    status=1
fi

# Code style checking
if ! pycodestyle --show-source pyuavcan tests
then
    status=1
fi

# Unit tests
# https://docs.pytest.org/en/latest/pythonpath.html#invoking-pytest-versus-python-m-pytest
if coverage run --source pyuavcan,tests -m pytest --capture=no -vv pyuavcan tests
then
    coverage report
else
    status=1
fi

exit $status
