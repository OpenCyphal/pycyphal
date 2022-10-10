.. _dev:

Development guide
=================

This document is intended for library developers only.
If you just want to use the library, you don't need to read it.


Source directory layout
-----------------------

Most of the package configuration can be gathered by reading ``setup.cfg``.
When adding new tools and such, try storing all their configuration there to keep everything in one place.

All shippable entities are located exclusively inside the directory ``pycyphal/``.
The entirety of the directory is packaged for distribution.

The submodule ``demo/public_regulated_data_types/`` is needed only for demo, testing, and documentation building.
It should be kept reasonably up-to-date, but remember that it does not affect the final product in any way.
We no longer ship DSDL namespaces with code for reasons explained in the user documentation.

Please desist from adding any new VCS submodules or subtrees.

The demos that are included in the user documentation are located under ``demo/``.
Whenever the test suite is run, it tests the demo application as well in order to ensure that it is correct and
compatible with the current version of the library -- keeping the docs up-to-date is vitally important.

All development automation is managed by Nox.
Please look into ``/noxfile.py`` to see how everything it set up; it is intended to be mostly self-documenting.
The CI configuration files located nearby should be looked at as well to gather what manual steps need to be
taken to configure the environment for local testing.


Third-party dependencies
------------------------

The general rule is that external dependencies are to be avoided unless doing so would increase the complexity
of the codebase considerably.
There are two kinds of 3rd-party dependencies used by this library:

- **Core dependencies.** Those are absolutely required to use the library.
  The list of core deps contains two libraries: Nunavut and NumPy, and it is probably not going to be extended ever
  (technically, there is also PyDSDL, but it is a co-dependency of Nunavut).
  They must be available regardless of the context the library is used in.
  Please don't submit patches that add new core dependencies.

- **Transport-specific dependencies.** Certain transports or some of their media sub-layer implementations may
  have third-party dependencies of their own. Those are not included in the list of main dependencies;
  instead, they are registered as *package extras*. Please read the detailed documentation and the applicable
  conventions in the user documentation and in ``setup.cfg``.


Coding conventions
------------------

Consistent code formatting is enforced automatically with `Black <https://github.com/psf/black>`_.
The only non-default (and non-PEP8) setting is that the line length is set to 120 characters.

Ensure that none of the entities, including sub-modules,
that are not part of the library API are reachable from outside the package.
This means that every entity defined in the library should be named with a leading underscore
or hidden inside a private subpackage unless it a part of the public library API
(relevant: `<https://github.com/sphinx-doc/sphinx/issues/6574#issuecomment-511122156>`_).
Violation of this rule may result in an obscure API structure and its unintended breakage between minor revisions.
This rule does not apply to the ``tests`` package.

When re-exporting entities from a package-level ``__init__.py``,
always use the form ``import ... as ...`` even if the name is not changed
to signal static analysis tools that the name is intended to be re-exported
(unless the aliased name starts with an underscore).
This is enforced with MyPy (it is set up with ``implicit_reexport=False``).

Excepting the above described case of package-level API re-export, it is best to avoid importing specific entities;
instead, try importing only the module itself and then use verbose references, as shown below.
This helps reduce scope contamination and avoid naming conflicts.

::

    from pycyphal.transport import Transport    # Avoid this if you can.
    import pycyphal.transport                   # Prefer this.


Semantic and behavioral conventions
-----------------------------------

Do not raise exceptions from properties. Generally, a property should always return its value.
If the availability of the value is conditional, consider using a getter method instead.

Methods and functions that command a new state should be idempotent;
i.e., if the commanded state is already reached, do nothing instead of raising an error.
Example: ``start()`` -- do nothing if already started; ``close()`` -- do nothing if already closed.

If you intend to implement some form of RAII with the help of object finalizers ``__del__()``,
beware that if the object is accidentally resurrected in the process, the finalizer may or may not be invoked
again later, which breaks the RAII logic.
This may happen, for instance, if the object is passed to a logging call.

API functions and methods that contain the following parameters should adhere to the semantic naming conventions:

+--------------------------------------+-----------------------+-------------------------------------------------------+
|Type                                  |Name                   |Purpose                                                |
+======================================+=======================+=======================================================+
|``pydsdl.*Type``                      |``model``              |PyDSDL type model (descriptor).                        |
+--------------------------------------+-----------------------+-------------------------------------------------------+
|``pycyphal.dsdl.*Object``             |``obj``                |Instance of a generated class implementing DSDL type.  |
+--------------------------------------+-----------------------+-------------------------------------------------------+
|``typing.Type[pycyphal.dsdl.*Object]``|``dtype``              |Generated class implementing a DSDL type.              |
+--------------------------------------+-----------------------+-------------------------------------------------------+
|``float``                             |``monotonic_deadline`` |Abort operation if not completed **by** this time.     |
|                                      |                       |Time system is ``AbstractEventLoop.time()``.           |
+--------------------------------------+-----------------------+-------------------------------------------------------+
|``float``                             |``timeout``            |Abort operation if not completed **in** this time.     |
+--------------------------------------+-----------------------+-------------------------------------------------------+
|``int``                               |``node_id``            |A node identifier.                                     |
+--------------------------------------+-----------------------+-------------------------------------------------------+


Documentation
-------------

Usage semantics should be expressed in the code whenever possible, particularly though the type system.
Documentation is the last resort; use prose only for things that cannot be concisely conveyed through the code.

For simple cases prefer doctests to regular test functions because they address two problems at once:
testing and documentation.

When documenting attributes and variables, use the standard docstring syntax instead of comments::

    THE_ANSWER = 42
    """
    What do you get when you multiply six by nine.
    """

Avoid stating obvious things in the docs. It is best to write no doc at all than restating things that
are evident from the code::

    def get_thing(self):                            # Bad, don't do this.
        """
        Gets the thing or returns None if the thing is gone.
        """
        return self._maybe_thing

    def get_thing(self) -> typing.Optional[Thing]:  # Good.
        return self._maybe_thing


Testing
-------


Setup
.....

In order to set up the local environment, execute the setup commands listed in the CI configuration files.
It is assumed that library development and code analysis is done on a GNU/Linux system.

There is a dedicated directory ``.test_deps/`` in the project root that stores third-party dependencies
that cannot be easily procured from package managers.
Naturally, these are mostly Windows-specific utilities.

Testing, analysis, and documentation generation are automated with Nox via ``noxfile.py``.
Do look at this file to see what actions are available and how the automation is set up.
If you need to test a specific module or part thereof, consider invoking PyTest directly to speed things up
(see section below).

If you want to run the full test suite locally, you'll need to install ``ncat`` and ``nox``:

- ``ncat``

    sudo apt-get -y install ncat    # Debian and derivatives
    sudo pacman -s nmap             # Arch and derivatives
    brew install nmap               # macOS

- ``nox``

    pip install nox

Make sure that you have updated the included submodules:

    cd ~/pycyphal
    git submodule update --init --recursive

..  tip:: macOS

    In order to run certain tests you'll need to have special permissions to perform low-level network packet capture.
    The easiest way to get around this is by installing `Wireshark <https://www.wireshark.org/>`_.
    Run the program and it will (automatically) ask you to update certain permissions
    (otherwise check `here <https://stackoverflow.com/questions/41126943/wireshark-you-dont-have-permission-to-capture-on-that-device-mac/>`_).

Now you should be able to run the tests, you can use the following commands:

    nox --list                  # shows all the different sessions that are available
    nox --sessions test-3.10    # run the tests using Python 3.10

To abort on first error::

    nox -x -- -x


Running a subset of tests
.........................

Sometimes during development it might be necessary to only run a certain subset of unit tests related to the
newly developed functionality.

As we're invoking ``pytest`` directly outside of ``nox``, we should first set ``CYPHAL_PATH`` to contain
a list of all the paths where the DSDL root namespace directories are to be found
(modify the values to match your environment).

..  code-block:: sh

    export CYPHAL_PATH="$HOME/pycyphal/demo/custom_data_types:$HOME/pycyphal/demo/public_regulated_data_types"

Next, open 2 terminal windows.

In the first, run:

    ncat --broker --listen -p 50905

In the second one:

    cd ~/pycyphal
    export PYTHONASYNCIODEBUG=1         # should be set while running tests
    nox --sessions test-3.10            # this will setup a virual environment for your tests
    source .nox/test-3-10/bin/activate  # activate the virtual environment
    pytest -k udp                       # only tests which match the given substring will be run


Writing tests
.............

When writing tests, aim to cover at least 90% of branches.
Ensure that your tests do not emit any errors or warnings into stderr output upon successful execution,
because that may distract the developer from noticing true abnormalities
(you may use ``caplog.at_level('CRITICAL')`` to suppress undesirable output).

Write unit tests as functions without arguments prefixed with ``_unittest_``.
Generally, simple test functions should be located as close as possible to the tested code,
preferably at the end of the same Python module; exception applies to several directories listed in ``setup.cfg``,
which are unconditionally excluded from unit test discovery because they rely on DSDL autogenerated code
or optional third-party dependencies,
meaning that if you write your unit test function in there it will never be invoked.

Complex functions that require sophisticated setup and teardown process or that can't be located near the
tested code for other reasons should be defined in the ``tests`` package.
Specifically, scenarios that depend on particular host configuration (like packet capture being configured
or virtual interfaces being set up) can only be defined in the dedicated test package
because the required environment configuration activities may not be performed until the test package is initialized.
Further, test functions that are located inside the library are shipped together with the library,
which makes having complex testing logic inside the main codebase undesirable.

Tests that are implemented inside the main codebase shall not use any external components that are not
listed among the core runtime library dependencies; for example, ``pytest`` cannot be imported
because it will break the library outside of test-enabled environments.

Many of the integration tests require real-time execution.
The host system should be sufficiently responsive and it should not be burdened with
unrelated tasks while running the test suite.

When adding new transports, make sure to extend the test suite so that the presentation layer
and other higher-level components are tested against them.
At least the following locations should be checked first:

- ``tests/presentation`` -- generic presentation layer test cases.
- ``tests/demo`` -- demo test cases.
- The list may not be exhaustive, please grep the sources to locate all relevant modules.

Many tests rely on the DSDL-generated packages being available for importing.
The DSDL package generation is implemented in ``tests/dsdl``.
After the packages are generated, the output is cached on disk to permit fast re-testing during development.
The cache can be invalidated manually by running ``nox -s clean``.

On GNU/Linux, the amount of memory available for the test process is artificially limited to a few gibibytes
to catch possible memory hogs (like https://github.com/OpenCyphal/pydsdl/issues/23 ).
See ``conftest.py`` for details.


Supporting newer versions of Python
...................................

Normally, this should be done a few months after a new version of CPython is released:

1. Update the CI/CD pipelines to enable the new Python version.
2. Update the CD configuration to make sure that the library is released using the newest version of Python.
3. Bump the version number using the ``.dev`` suffix to indicate that it is not release-ready until tested.

When the CI/CD pipelines pass, you are all set.


Releasing
---------

PyCyphal is versioned by following `Semantic Versioning <https://semver.org>`_.

Please update ``/CHANGELOG.rst`` whenever you introduce externally visible changes.
Changes that only affect the internal structure of the library (like test rigging, internal refactorings, etc.)
should not be mentioned in the changelog.

CI/CD automation uploads a new release to PyPI and pushes a new tag upstream on every push to ``master``.
It is therefore necessary to ensure that the library version (see ``pycyphal/_version.py``) is bumped whenever
a new commit is merged into ``master``;
otherwise, the automation will fail with an explicit tag conflict error instead of deploying the release.


Tools
-----

We recommend the `JetBrains PyCharm <https://www.jetbrains.com/pycharm/>`_ IDE for development.
Inspections that are already covered by the CI/CD toolchain should be disabled to avoid polluting the code
with suppression comments.

Configure a File Watcher to run Black on save (make sure to disable running it on external file changes though).

The test suite stores compiled DSDL into ``.compiled/`` in the current working directory
(when using Nox, the current working directory may be under a virtualenv private directory).
Make sure to mark it as a source directory to enable code completion and type analysis in the IDE
(for PyCharm: right click -> Mark Directory As -> Sources Root).
Alternatively, you can just compile DSDL manually directly in the project root.
