#!/bin/bash
#
# PyPI release automation.
# https://gist.github.com/boppreh/ac7522b3a4ac46b4f6010eecddc57f21
#

set -e
set -o nounset

./test.sh
./clean.sh
./setup.py sdist bdist_wheel
python3 -m twine upload dist/*
./clean.sh
