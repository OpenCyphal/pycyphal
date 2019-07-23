#!/bin/bash

cd "${0%/*}"    # cd to this script's directory

rm -rf .*_generated .coverage* _build &> /dev/null

sphinx-build -b html -W --keep-going -j4 . _build/html $@
