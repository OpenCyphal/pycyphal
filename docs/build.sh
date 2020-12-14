#!/bin/bash

set -o nounset

function die()
{
    echo >&2 "FAILURE: $*"
    exit 1
}

cd "${0%/*}" || die "Could not cd into the script directory"

command -v dot || die "Please install graphviz. It may be available from your package manager as 'graphviz'."

rm -rf .*_generated .coverage* _build &> /dev/null

# shellcheck disable=SC2068
sphinx-build -b html -W --keep-going -j4 . _build/html $@
exit $?
