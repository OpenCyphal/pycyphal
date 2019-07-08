#!/bin/bash
#
# PyPI release automation.
# https://gist.github.com/boppreh/ac7522b3a4ac46b4f6010eecddc57f21
#
# All packages where the version number is changed will be packaged and published on PyPI.
# Packages whose version numbers remain unchanged will not be published (push will be rejected by PyPI).
#

function clean()
{
    rm -rf dist build *.egg-info &> /dev/null
}

function release_directory()
{
    echo "Releasing directory $1"
    cd $1
    clean
    ./setup.py sdist bdist_wheel   || exit 1
    python3 -m twine upload dist/* || exit 2
    clean
    cd -
}

release_directory pyuavcan
release_directory pyuavcan_cli
