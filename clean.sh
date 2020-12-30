#!/bin/bash

rm -rf dist build ./*.egg-info .coverage* htmlcov .*cache .*_generated *.log *.tmp &> /dev/null

pushd docs || exit 1
rm -rf _build .coverage* .*_generated
popd || exit 1
