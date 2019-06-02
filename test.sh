#!/bin/bash

status=0

# Downloading the public regulated types - they are needed for testing.
if [[ ! -d public_regulated_data_types.cache ]]
then
    git clone https://github.com/UAVCAN/public_regulated_data_types --branch=uavcan-v1.0 \
        public_regulated_data_types.cache || exit 1
fi

# Unit testing.
# https://docs.pytest.org/en/latest/pythonpath.html#invoking-pytest-versus-python-m-pytest
export PYTHONASYNCIODEBUG=1
if coverage run --source pyuavcan,tests,.test_dsdl_generated -m pytest -v pyuavcan tests $@
then
    coverage report
else
    status=1
fi

# Static type checking.
# We postpone type checking until after the tests have run in order to be able to typecheck the
# generated Python packages as well.
export MYPYPATH=".test_dsdl_generated"
if ! mypy --strict --config-file=setup.cfg pyuavcan tests .test_dsdl_generated
then
    status=1
fi

# PEP8 code style enforcement.
if ! pycodestyle --show-source pyuavcan tests
then
    status=1
fi

exit $status
