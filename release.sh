#!/bin/bash
#
# PyPI release automation.
# https://gist.github.com/boppreh/ac7522b3a4ac46b4f6010eecddc57f21
#

set -o nounset

function die()
{
    echo >&2 "FAILURE: $*"
    exit 1
}

[[ "$(git rev-parse --abbrev-ref HEAD)" = 'master' ]]  || die "Can only release from the master branch."
[[ -z "$(git diff)" ]]                                 || die "Please commit all changes, then try again."
[[ -z "$(git log '@{u}..')" ]]                         || die "Please push all commits, then try again."

./clean.sh ||\
    die "Clean failed. Cleaning is required to prevent unnecessary files from being included in the release package."

./setup.py sdist bdist_wheel   || die "Execution of setup.py has failed."
python3 -m twine upload dist/* || die "Twine upload has failed."
./clean.sh  # May fail, we don't care.

version=$(cat pyuavcan/VERSION)
(git tag -a "$version" -m "$version" && git push --tags) || die "Could not tag the release. Please do it manually."
