# Test dependencies

This directory contains external dependencies necessary for running the intergration test suite
that cannot be sourced from package managers.
To see how these components are used, refer to the test scripts.

Please keep this document in sync with the contents of this directory.

## Nmap project binaries

### Portable Ncat

Ncat is needed for brokering TCP connections that emulate serial port connections.
This is needed for testing the UAVCAN/serial transport without having to access a physical serial port
(which would be difficult to set up on a CI server).

The binary comes with the following statement by its developers:

> This is a portable (statically compiled) Win32 version of Ncat.
> You should be able to take the ncat.exe and run it on other systems without having to also copy over
> a bunch of DLLs, etc.
>
> More information on Ncat: http://nmap.org/ncat/
>
> You can get the version number of this ncat by runnign "ncat --version".
> We don't create a new Ncat portable for each Ncat release,
> so you will have to compile your own if you want a newer version.
> Instructions for doing so are available at: https://secwiki.org/w/Nmap/Ncat_Portable
>
> Ncat is distributed under the same free and open source license as Nmap.
> See http://nmap.org/book/man-legal.html.


### Npcap installer

Npcap is needed for testing the network sniffer of the UAVCAN/UDP transport implementation on Windows.

Npcap is distributed under the terms of Nmap Public Source License: https://nmap.org/npsl/.


## SonarQube scanner

New versions can be obtained from https://binaries.sonarsource.com/Distribution/sonar-scanner-cli/.
