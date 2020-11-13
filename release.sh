#!/bin/bash
# PyPI release automation. This script can be invoked manually or from a CI pipeline.
# https://gist.github.com/boppreh/ac7522b3a4ac46b4f6010eecddc57f21

set -o nounset

function die()
{
    echo >&2 "FAILURE: $*"
    exit 1
}

python3 -m pip uninstall pyuavcan -y &> /dev/null  # Avoid conflicts

version=$(cat pyuavcan/VERSION)

[[ -z "$(git diff)" ]]              || die "Commit all changes, then try again"
[[ -z "$(git log '@{u}..')" ]]      || die "Push all commits, then try again"
git tag -a "$version" -m "$version" || die "Could not tag the release. Did you forget to bump the version number?"

./clean.sh || die "Clean failed. It is required to prevent unnecessary files from being included in the package."

./setup.py sdist bdist_wheel   || die "Execution of setup.py has failed."
python3 -m twine upload dist/* || die "Twine upload has failed."
./clean.sh  # May fail, we don't care.

git push --tags || die "Could not push the new tag upstream. Please tag the release manually."
