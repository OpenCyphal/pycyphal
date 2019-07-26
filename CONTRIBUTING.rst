.. _dev:

Development guide
=================

This document is intended for library developers only.
If you just want to use the library, you don't need to read it.


Source directory layout
-----------------------

Most of the package configuration can be gathered by reading ``setup.cfg``.
When adding new tools and such, please put all their configuration there to keep everything in one place.

All shippable entities are located exclusively inside the directory ``pyuavcan/``.
The entirety of the directory is shipped.

The submodule ``tests/public_regulated_data_types/`` is needed only for testing and documentation building.
It should be kept reasonably up-to-date, but remember that it does not affect the final product in any way.
We no longer ship DSDL namespaces with code for reasons explained in the user documentation.

Please desist from adding any new submodules.

The usage demo scripts that are included in the user documentation are located under ``tests/demo/``.
This is probably mildly surprising since one would expect to find docs under ``docs/``,
but it is done this way to facilitate testing and static analysis of the demo scripts.
Whenever the test suite is run, it tests the demo application as well in order to ensure that it is correct and
compatible with the current version of the library -- keeping the docs up-to-date is vitally important.

The CLI tool is auto-tested as well, the tests are located under ``tests/cli/``.
It's somewhat trickier than with the rest of the code because it requires us to
launch new processes and keep track of their code coverage metrics;
the details are explained in a dedicated section.

There are major automation scripts located in the source root directory.
You will need them if you are developing the library; please open them and read the comments inside to understand
how they work and how to use them.
The CI configuration files located nearby should be looked at as well to gather what manual steps need to be
taken to configure the environment for local testing.


Third-party dependencies
------------------------

The general rule is that external dependencies are to be avoided unless doing so would increase the complexity
of the codebase considerably.
There are three kinds of 3rd-party dependencies used by this library:

- **Core dependencies.** Those are absolutely required to use the library.
  The list of core deps contains two libraries: Nunavut and NumPy, and it is probably not going to be extended ever
  (technically, there is also PyDSDL, but it is a co-dependency of Nunavut).
  They must be available regardless of the context the library is used in.
  Please don't submit patches that add new core dependencies.

- **Transport-specific dependencies.** Certain transports or some of their media sub-layer implementations may
  have third-party dependencies of their own. Those are not included in the list of main dependencies;
  instead, they are registered as *package extras*. Please read the detailed documentation and the applicable
  conventions in the user documentation and in ``setup.cfg``.
  When developing new transports or media sub-layers, try to avoid adding new dependencies.

- **Other dependencies.** Those are needed for some minor optional components and features of the library,
  such as the CLI tool.


Coding conventions
------------------

Please follow the `Zubax Python coding conventions <https://kb.zubax.com/x/_oAh>`_.
Compliance is mostly enforced automatically by the test suite.
Some of the rules cannot be easily enforced automatically, so please keep an eye out for those.

It is particularly important to ensure that none of the entities, including sub-modules,
that are not part of the library API are reachable from outside the package.
This means that every module, class, function, etc. defined in the library must be named with a leading underscore,
unless it a part of the public library API
(relevant: `<https://github.com/sphinx-doc/sphinx/issues/6574#issuecomment-511122156>`_).
Violation of this rule may result in an obscure API structure and its unintended breakage between minor revisions.
This rule does not apply to the auto-test package.

When re-exporting entities from a package-level ``__init__.py``,
always use the form ``import ... as ...`` even if the name is not changed,
to signal static analysis tools that the name is intended to be re-exported
(unless the aliased name starts with an underscore).
This is partially enforced with MyPy.

Excepting the above described case of package-level API re-export, do not import specific entities;
instead, import only the module itself and then use verbose references, as shown below.
If you really need to import a specific entity, consider prefixing it with an underscore to prevent scope leakage.
Exception applies to well-encapsulated submodules which are not part of the library API
(i.e., prefixed with an underscore) -- you can import whatever you want provided that the
visibility scope of the module is sufficiently narrow.

::

    from pyuavcan.transport import Transport    # Avoid this.
    import pyuavcan.transport                   # Good. Use like: pyuavcan.transport.Transport


Semantic and behavioral conventions
-----------------------------------

Do not raise exceptions from properties. Generally, a property should always return its value.
If the availability of the value is conditional, consider using a getter method instead.

Methods and functions that command a new state should be idempotent;
i.e., if the commanded state is already reached, do nothing.
Example: ``start()`` -- do nothing if already started; ``close()`` -- do nothing if already closed.

API functions and methods that contain the following parameters should adhere to the semantic naming conventions:

+-----------------------------------------+-------------------------+-----------------------------------------------------------+
|Type                                     | Name                    | Purpose                                                   |
+=========================================+=========================+===========================================================+
|``pydsdl.*Type``                         | ``model``               | PyDSDL type model (descriptor).                           |
+-----------------------------------------+-------------------------+-----------------------------------------------------------+
|``pyuavcan.dsdl.*Object``                | ``obj``                 | Instance of a generated class implementing a DSDL type.   |
+-----------------------------------------+-------------------------+-----------------------------------------------------------+
|``typing.Type[pyuavcan.dsdl.*Object]``   | ``dtype``               | Generated class implementing a DSDL type.                 |
+-----------------------------------------+-------------------------+-----------------------------------------------------------+
|``float``                                | ``monotonic_deadline``  | Abort operation if not completed **by** this time.        |
|                                         |                         | Time system is ``AbstractEventLoop.time()``.              |
+-----------------------------------------+-------------------------+-----------------------------------------------------------+
|``float``                                | ``timeout``             | Abort operation if not completed **in** this time.        |
+-----------------------------------------+-------------------------+-----------------------------------------------------------+
|``int``                                  | ``node_id``             | A node identifier.                                        |
+-----------------------------------------+-------------------------+-----------------------------------------------------------+


Documentation
-------------

Usage semantics should be expressed in the code whenever possible, particularly though the type system.
Documentation is the last resort; use prose only for things that cannot be concisely conveyed through the code.

For simple cases prefer doctests to regular test functions because they address two problems at once:
testing and documentation.

When documenting attributes and variables, use the prefix octothorp-colon syntax::

    #: What do you get when you multiply six by nine.
    THE_ANSWER = 42

Avoid stating obvious things in the docs. It is best to write no doc at all than restating things that
are evident from the code::

    def get_thing(self):                            # Bad, don't do this.
        """
        Gets the thing or returns None if the thing is gone.
        """
        return self._maybe_thing

    def get_thing(self) -> typing.Optional[Thing]:  # Good.
        return self._maybe_thing

When writing the built-in help docs for the CLI tool,
ensure that the line width does not exceed 80 characters unless it's absolutely unavoidable (e.g., long URIs),
otherwise the text may fail to render properly on some terminals.


Testing
-------

In order to setup the local environment, execute the setup commands listed in the CI configuration files.

The script ``test.sh`` can be used to run the unit tests, static code analysis, documentation generation,
and so on, locally or on a CI server.
At the time of writing, the script takes some 10 minutes to run, so it may not work well for development;
consider invoking pytest manually on a specific directory, file, or function instead (command-line option ``-k``).
For more information refer to the PyTest documentation.

After the tests are executed, it is possible to run the `SonarQube <https://sonarqube.org>`_ scanner as follows:
``sonar-scanner -Dsonar.login=<project-key>`` (the project key is a 40-digit long hexadecimal number).
The scanner should not be run before the full general test suite since it relies on its coverage data.

When writing tests, aim to cover at least 90% of branches, excepting the DSDL generated packages (at least for now)
(the DSDL test data is synthesized at run time).

Write unit tests as functions without arguments prefixed with ``_unittest_``;
optionally, for slow test functions use the prefix ``_unittest_slow_`` (more on this below).
Generally, simple test functions should be located as close as possible to the tested code,
preferably at the end of the same Python module; exception applies to several directories listed in ``setup.cfg``,
which are unconditionally excluded from unit test discovery because they rely on DSDL autogenerated code
or optional third-party dependencies,
meaning that if you write your unit test function in there it will never be invoked.

Complex functions that require sophisticated setup and teardown process or that can't be located near the
tested code for other reasons shall be moved into the separate test package (aptly named ``tests``).
Test functions that are located inside the library are shipped together with the library,
which makes having complex testing logic inside the main codebase undesirable.

Tests that are implemented inside the main codebase shall not use any external components that are not
listed among the core runtime library dependencies; for example, the library ``pytest`` cannot be imported
because it will break the library outside of test-enabled environments.
You can do that only in the separate test package since it's never shipped and hence does not need to work
outside of test-enabled environments.

Certain tests require real-time execution.
If they appear to be failing with timeout errors and such, consider re-running them on a faster system.
It is recommended to run the test suite with at least 2 GB of free RAM and an SSD.

Auto-tests may spawn new processes, e.g., to test the CLI tool. In order to keep their code coverage measured,
we have put the coverage setup code into a special module ``sitecustomize.py``, which is auto-imported
every time a new Python interpreter is started (as long as the module's path is in ``PYTHONPATH``, of course).
Hence, every invocation of Python made during testing is coverage-tracked, which is great.
This is why we don't invoke ``coverage`` manually when running tests.
After the tests are executed, we end up with some dozen or more of ``.coverage*`` files scattered across the
source directories.
The scattered coverage files are then located automatically and combined into one file,
which is then analyzed by report generators and other tools like SonarQube.

Some of the components of the library and of the test suite require DSDL packages to be generated.
Those must be dealt with carefully as it needs to be ensured that the code that requires generated
packages to be available is not executed until they are generated.

When adding new transports, make sure to extend the test suite so that the presentation layer is tested against them.


Releasing
---------

PyUAVCAN is versioned by following `Semantic Versioning <https://semver.org>`_.

The release process is as follows:

#. Ensure you are on the default branch.
#. Bump the version number in ``pyuavcan/VERSION``. Commit the change.
#. Add a new tag. The tag format is ``<major>.<minor>.<patch>``.
#. Push the changes and the tag to the upstream remote.
#. Run ``release.sh`` to push the package to PyPI. Ask the maintainers for access credentials.


Tools
-----

We recommend the `JetBrains PyCharm <https://www.jetbrains.com/pycharm/>`_ IDE for development.
The recommended OS is GNU/Linux; if you are on a different system, you are on your own.

The library test suite stores generated DSDL packages into a directory named ``.test_dsdl_generated``
under the project root directory.
Make sure to mark it as a source directory to enable code completion and type analysis in the IDE
(for PyCharm: right click -> Mark Directory As -> Sources Root).

Configure the IDE to remove trailing whitespace on save in the entire file.
