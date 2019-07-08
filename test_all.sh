#!/bin/bash
#
# This script runs the full test suite, bootstrapping from a clean system configuration,
# and outputs combined coverage stats. Use it in the CI.
#

function die()
{
    echo >&2 "TEST FAILED: $@"
    exit 1
}

function test_one()
{
    cd $1 || die "Could not cd into $1"

    rm -rf build dist *.egg-info .coverage* &> /dev/null

    pip install -r requirements.txt || die "Could not install deps in $1"
    ./test.sh                       || die "The test has failed in $1"
    ls -l .coverage                 || die "Coverage file not created in $1"

    cd -
}


rm -rf .coverage* htmlcov
pip install -r requirements-common-dev.txt

for subdir in */
do
    test_one $subdir
done

coverage combine */.coverage        || die "Could not combine coverage data"
coverage xml -i -o .coverage.xml    || die "Could not generate coverage XML (needed for further static analysis)"
coverage html                       || die "Could not generate HTML coverage report"
echo "CUMULATIVE COVERAGE REPORT:"
coverage report

echo "Success."
