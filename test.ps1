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

# Configure the NPF service. This is part of WinPCap, and it is needed for testing the UDP transport.
# We don't need the wpcap.dll and packet.dll because they are shipped with the libpcap dependency.
Invoke-WebRequest `
    "https://github.com/3gstudent/Winpcap_Install/raw/6ff3c2ee4056fa9ccba6e972ece5455628b03026/npf_x64.sys" `
    -OutFile npf.sys
Move-Item npf.sys "$env:SystemRoot/system32/drivers/npf.sys"
sc.exe create npf `
    binPath=    system32\drivers\npf.sys `
    type=       kernel `
    start=      demand `
    error=      normal `
    tag=        no `
    DisplayName= "WinPCap NetGroup Packet Filter (NPF) Driver"
sc.exe start npf

python -m pip install -r requirements.txt

# Install Ncat. The unpacking procedure is inspired by:
# https://community.idera.com/database-tools/powershell/powertips/b/tips/posts/extract-specific-files-from-zip-archive
Invoke-WebRequest "http://nmap.org/dist/ncat-portable-5.59BETA1.zip" -OutFile ncat.zip
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

# The DSDL gen directory shall exist before coverage is invoked, otherwise its coverage won't be tracked.
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
    python -m pytest
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
