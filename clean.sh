#!/bin/bash

rm -rf dist build ./*.egg-info .coverage* htmlcov .*cache .*_generated *.log *.tmp &> /dev/null

# DSDL-generated packages
rm -rf uavcan sirius_cyber_corp test_dsdl_namespace &> /dev/null

pushd docs || exit 1
rm -rf _build .coverage* .*_generated
popd || exit 1
