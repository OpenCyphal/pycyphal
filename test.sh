#!/bin/bash
#
# This global test script is designed to be invokable on a completely clean environment (e.g., on a CI server).
# It configures the environment and then runs unit tests, static code analysis, and builds the docs.
# Success is reported only if all of the steps are executed successfully.
#
# Observe that additional static analysis tools may be invoked separately afterwards; e.g., the SonarQube scanner.
# Such tools shall not be invoked before the script because they may be dependent on its side effects, such as
# generation of the coverage data.
#
# Running the script may take several minutes or more. To speed things up, switch off slow unit tests by configuring
# appropriate environment variables. Please see the unit test sources for details, or grep them for 'PYUAVCAN_'.
#

set -o nounset

function die()
{
    echo >&2 "FAILURE: $@"
    exit 1
}

function banner()
{
    local text="$@"
    local fill_seq=$(seq 1 $((${#text} + 2)))
    [[ -t 0 && -t 1 ]] && printf >&2 '\033[1;36m'
    printf >&2 '+'
    printf >&2 '%.0s-' $fill_seq
    printf >&2 '+\n| %s |\n+' "$text"
    printf >&2 '%.0s-' $fill_seq
    printf >&2 '+\a\n'
    [[ -t 0 && -t 1 ]] && printf >&2 '\033[0m'
}

# ---------------------------------------------------------------------------------------------------------------------

banner ENVIRONMENT CONFIGURATION

./clean.sh || die "Failed to clean"

# Installing the dependencies.
pip install -r requirements.txt     || die "Could not install runtime dependencies"
pip install -r requirements-dev.txt || die "Could not install development dependencies"

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
        public_regulated_data_types.cache || die "Could not clone the public regulated DSDL repository for testing"
fi

# ---------------------------------------------------------------------------------------------------------------------

banner TEST EXECUTION

export PYTHONASYNCIODEBUG=1

# The directory must exist before coverage.py is invoked in order for it to track it.
mkdir .test_dsdl_generated 2> /dev/null

# https://docs.pytest.org/en/latest/pythonpath.html#invoking-pytest-versus-python-m-pytest
coverage run -m pytest           || die "PyTest returned $?"

coverage combine                 || die "Could not combine coverage data"
coverage xml -i -o .coverage.xml || die "Could not generate coverage XML (needed for SonarQube)"

coverage html
coverage report

# ---------------------------------------------------------------------------------------------------------------------

banner STATIC ANALYSIS

# We typecheck after the tests have run in order to be able to typecheck the generated Python packages as well.
# TODO: re-enable MyPy enforcement when it's fixed. MyPy 0.701 and 0.711 are broken.
mypy --strict --strict-equality --no-implicit-reexport --config-file=setup.cfg pyuavcan tests .test_dsdl_generated \
    || echo >&2 "!!! WARNING: MYPY HAS FAILED WITH STATUS $? BUT THE FAILURE IS IGNORED !!!"
#   || die "MyPy returned $?"

pycodestyle pyuavcan tests || die "pycodestyle returned $?"

# ---------------------------------------------------------------------------------------------------------------------

banner DOCUMENTATION

cd docs
make html || die "Documentation build returned $?"
cd -

# ---------------------------------------------------------------------------------------------------------------------

banner SUCCESS
