#!/bin/bash

rm -rf dist build *.egg-info .coverage* htmlcov .*_generated &> /dev/null

cd docs
make clean
cd -
