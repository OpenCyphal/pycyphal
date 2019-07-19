#!/bin/bash

rm -rf dist build *.egg-info .coverage* htmlcov .*_generated &> /dev/null

pushd docs
rm -rf _build .coverage* .*_generated
popd
