#
# Auto-test for MS Windows PowerShell.
# You may need this:
#     Set-ExecutionPolicy unrestricted -scope CurrentUser
#

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

# Install Ncat. The unpacking procedure is inspired by:
# https://community.idera.com/database-tools/powershell/powertips/b/tips/posts/extract-specific-files-from-zip-archive
Invoke-WebRequest http://nmap.org/dist/ncat-portable-5.59BETA1.zip -OutFile ncat.zip
Add-Type -AssemblyName System.IO.Compression.FileSystem
$zip = [System.IO.Compression.ZipFile]::OpenRead("$root\\ncat.zip")
$zip.Entries |
    Where-Object { $_.FullName -like "*.exe" } |
    ForEach-Object {
        $FileName = $_.Name
        [System.IO.Compression.ZipFileExtensions]::ExtractToFile($_, "$root\\$FileName", $true)
    }
$zip.Dispose()
Remove-Item ncat.zip

# Run the TCP broker for serial transport tests in background.
$ncat_proc = Start-Process ncat -Args '-vv --broker --listen localhost 50905' -PassThru

#
# TESTING
#

# Too much logging may break real-time tests because console output is extremely slow on Windows.
python -m pytest --override-ini log_cli=0 --capture=fd
$test_ok = $?

$ncat_proc | Stop-Process

if ($test_ok) {
    python -m coverage combine
    python -m coverage report
}

Write-Host "Test OK: $test_ok"
exit ! $test_ok
