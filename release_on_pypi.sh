#!/bin/bash
#
# PyPI release automation.
# https://gist.github.com/boppreh/ac7522b3a4ac46b4f6010eecddc57f21
#

function clean()
{
    rm -rf dist build *.egg-info &> /dev/null
}

clean

python3 -m pip install twine wheel

./setup.py sdist bdist_wheel

python3 -m twine upload dist/*

clean
