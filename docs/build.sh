#!/bin/bash

cd "${0%/*}"    # cd to this script's directory

rm -rf .*_generated _build &> /dev/null

sphinx-build -M html . _build $@
