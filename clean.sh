#!/bin/bash

rm -rf dist build *.egg-info .coverage* htmlcov .*_generated &> /dev/null

pushd docs
make clean
popd
