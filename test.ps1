#
# Auto-test for MS Windows PowerShell.
# You may need this:
#     Set-ExecutionPolicy unrestricted -scope CurrentUser
#

$ErrorActionPreference = "Stop"

$root = Resolve-Path .

$env:PYTHONPATH += ";$root"
$env:PYTHONASYNCIODEBUG = "1"

python -m pip install -r requirements.txt

rm .coverage*

python -m pytest
