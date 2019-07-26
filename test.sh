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
    echo >&2 "FAILURE: $*"
    exit 1
}

function banner()
{
    local text="$*"
    # shellcheck disable=SC2155
    local fill_seq=$(seq 1 $((${#text} + 2)))
    [[ -t 0 && -t 1 ]] && printf >&2 '\033[1;36m'
    printf >&2 '+'
    printf >&2 '%.0s-' $fill_seq
    printf >&2 '+\n| %s |\n+' "$text"
    printf >&2 '%.0s-' $fill_seq
    printf >&2 '+\n'
    [[ -t 0 && -t 1 ]] && printf >&2 '\033[0m' || :
}

# ---------------------------------------------------------------------------------------------------------------------

banner ENVIRONMENT CONFIGURATION

cd "${0%/*}" || die "Couldn't cd"  # cd to this script's directory

# Extend PYTHONPATH to make sitecustomize.py/usercustomize.py importable.
if [[ -z "${PYTHONPATH:-}" ]]
then
    export PYTHONPATH="$PWD"
else
    export PYTHONPATH="$PYTHONPATH:$PWD"
fi
echo "PYTHONPATH: $PYTHONPATH"

export PYTHONASYNCIODEBUG=1

which dot || die "Please install graphviz. On Debian-based: apt-get install graphviz"

./clean.sh || die "Failed to clean"

pip install -r requirements.txt || die "Could not install dependencies"

# Initializing the system-wide test environment.
sudo modprobe can
sudo modprobe can_raw
sudo modprobe vcan
sudo ip link add dev vcan0 type vcan &> /dev/null
sudo ip link set up vcan0            &> /dev/null
sudo ip link set vcan0 mtu 72
sudo ifconfig vcan0 up

# ---------------------------------------------------------------------------------------------------------------------

banner TEST EXECUTION

# Launch the background candump logger.
# Hide Heartbeat frames because we just don't care.
candump -decaxta any | grep -iv 7D55 &
candump_pid="$!"

mkdir .test_dsdl_generated 2> /dev/null       # The directory must exist before coverage is invoked

# TODO: run the tests with the minimal dependency configuration. Set up a new environment here.
# Note that we do not invoke coverage.py explicitly here; this is handled by usercustomize.py. Relevant docs:
#   - https://coverage.readthedocs.io/en/coverage-4.2/subprocess.html
#   - https://docs.python.org/3/library/site.html
pytest                  || die "Core PyTest returned $?"
pytest pyuavcan/_cli    || die "CLI PyTest returned $?"

# Every time we launch a Python process, a new coverage file is created, so there may be a lot of those,
# possibly nested in sub-directories.
find ./*/ -name '.coverage*' -type f -print -exec mv {} . \;  || die "Could not lift coverage files"
ls -l .coverage*
coverage combine                                              || die "Could not combine coverage data"

coverage xml -i -o .coverage.xml || die "Could not generate coverage XML (needed for SonarQube)"
coverage html
coverage report

kill $candump_pid || echo "Couldn't kill candump. Who cares?"

# ---------------------------------------------------------------------------------------------------------------------

banner STATIC ANALYSIS

# We typecheck after the tests have run in order to be able to typecheck the generated Python packages as well.
mypy --strict --strict-equality --no-implicit-reexport --config-file=setup.cfg pyuavcan tests .test_dsdl_generated \
    || die "MyPy returned $?"

pycodestyle pyuavcan tests || die "pycodestyle returned $?"

# ---------------------------------------------------------------------------------------------------------------------

banner DOCUMENTATION

pushd docs || die "Couldn't change directory"
./build.sh || die "Documentation build returned $?"
popd       || die "Couldn't change directory"

# ---------------------------------------------------------------------------------------------------------------------

banner SUCCESS
