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

started_at=$(python3 -c 'import time; print(time.monotonic())')

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
    # shellcheck disable=SC2086
    printf >&2 '%.0s-' $fill_seq
    printf >&2 '+\n| %s |\n+' "$text"
    # shellcheck disable=SC2086
    printf >&2 '%.0s-' $fill_seq
    printf >&2 '+\n'
    # shellcheck disable=SC2015
    [[ -t 0 && -t 1 ]] && printf >&2 '\033[0m' || :
}

# ---------------------------------------------------------------------------------------------------------------------

banner ENVIRONMENT CONFIGURATION

python -c "import sys; exit('linux' not in sys.platform)" || die "This script can only be run on GNU/Linux"

cd "${0%/*}" || die "Couldn't cd into this script's directory"

# Extend PYTHONPATH to make sitecustomize.py/usercustomize.py importable.
if [[ -z "${PYTHONPATH:-}" ]]
then
    export PYTHONPATH="$PWD"
else
    export PYTHONPATH="$PYTHONPATH:$PWD"
fi
echo "PYTHONPATH: $PYTHONPATH"

export PYTHONASYNCIODEBUG=1

command -v dot  || die "Please install graphviz. On Debian-based: apt install graphviz"
command -v ncat || die "Please install nmap. On Debian-based: apt install nmap"

./clean.sh || die "Failed to clean"

pip install -r requirements.txt || die "Could not install dependencies"

# Initializing the system-wide test environment.
# SocketCAN virtual bus.
sudo modprobe can
sudo modprobe can_raw
sudo modprobe vcan
for index in 0 1 2  # Multiple interfaces are needed for testing redundant transports.
do
    iface="vcan$index"
    sudo ip link add dev $iface type vcan
    sudo ip link set $iface mtu 72        || die "Could not configure MTU on $iface"
    sudo ip link set up $iface            || die "Could not bring up $iface"
done

# TCP broker for serial port testing.
ncat --broker --listen -p 50905 &>/dev/null &
# shellcheck disable=SC2064
trap "kill $! || echo 'Could not kill child $!'" SIGINT SIGTERM EXIT

# ---------------------------------------------------------------------------------------------------------------------

banner TEST EXECUTION

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

# ---------------------------------------------------------------------------------------------------------------------

banner STATIC ANALYSIS

# We typecheck after the tests have run in order to be able to typecheck the generated Python packages as well.
# Also, dear mypy, please stuff your caching, it's fucking broken.
rm -rf .mypy_cache/ &> /dev/null
echo 'YOU SHALL NOT PASS' > .mypy_cache
chmod 444 .mypy_cache
mypy --strict --strict-equality --no-implicit-reexport --config-file=setup.cfg pyuavcan tests .test_dsdl_generated \
    || die "MyPy returned $?"

pycodestyle pyuavcan tests || die "pycodestyle returned $?"

# ---------------------------------------------------------------------------------------------------------------------

banner DOCUMENTATION

pushd docs || die "Couldn't change directory"
./build.sh || die "Documentation build returned $?"
popd       || die "Couldn't change directory"

# ---------------------------------------------------------------------------------------------------------------------

python3 -c "import time; print(f'Done in {(time.monotonic() - $started_at) / 60:0.0f} minutes')"

banner SUCCESS
