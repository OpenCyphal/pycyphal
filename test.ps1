# Auto-test for MS Windows PowerShell.
# You may need this:
#     Set-ExecutionPolicy unrestricted -scope CurrentUser

$root = Resolve-Path .

$env:PYTHONPATH += ";$root"
$env:PYTHONASYNCIODEBUG = "1"

#
# ENVIRONMENT CONFIGURATION
#

Remove-Item .coverage*

# Reconfigure the system timer to run at a higher resolution. This may be necessary for real-time tests to pass.
python -c @"
import ctypes
t = ctypes.c_ulong()
ctypes.WinDLL('NTDLL.DLL').NtSetTimerResolution(5000, 1, ctypes.byref(t))
print('System timer resolution:', t.value / 10e3, 'ms')
"@

python -m pip install -r requirements.txt

# Run the TCP broker for serial transport tests in background.
$ncat_proc = Start-Process '.test_deps/ncat.exe' -Args '-vv --broker --listen localhost 50905' -PassThru

#
# TESTING
#

# The DSDL gen directory shall exist before coverage is invoked if we want to track its coverage.
Remove-Item -Recurse -Force ".test_dsdl_generated" -ErrorAction SilentlyContinue
New-Item -Path . -Name ".test_dsdl_generated" -ItemType Directory

# Due to the fact that the real-time performance of Windows is bad, our tests may fail spuriously.
# We work around that by re-running everything again on failure.
# If the second run succeeds, the tests are considered to pass.
$test_attempts = 2
$test_ok = False
For ($i=1; ($i -le $test_attempts) -and -not $test_ok; $i++)
{
    Write-Host "Running the tests, attempt $i of $test_attempts..."
    python -m coverage run -m pytest
    $test_ok = $?
    Write-Host "Attempt $i of $test_attempts completed; success: $test_ok"
}

$ncat_proc | Stop-Process

if ($test_ok) {
    python -m coverage combine
    python -m coverage report
}

Write-Host "Test OK: $test_ok"
exit ! $test_ok
