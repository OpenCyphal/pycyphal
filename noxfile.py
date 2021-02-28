# Copyright (c) 2020 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>
# type: ignore

import os
import sys
import time
import shutil
import subprocess
from functools import partial
import configparser
from pathlib import Path
import nox


ROOT_DIR = Path(__file__).resolve().parent
DEPS_DIR = ROOT_DIR / ".test_deps"
assert DEPS_DIR.is_dir(), "Invalid configuration"
os.environ["PATH"] += os.pathsep + str(DEPS_DIR)

CONFIG = configparser.ConfigParser()
CONFIG.read("setup.cfg")
EXTRAS_REQUIRE = dict(CONFIG["options.extras_require"])
assert EXTRAS_REQUIRE, "Config could not be read correctly"

PYTHONS = ["3.7", "3.8", "3.9"]
"""The newest supported Python shall be listed last."""

nox.options.error_on_external_run = True


@nox.session(python=False)
def clean(session):
    wildcards = [
        "dist",
        "build",
        "html*",
        ".coverage*",
        ".*cache",
        ".*compiled",
        ".*generated",
        "*.egg-info",
        "*.log",
        "*.tmp",
        ".nox",
    ]
    for w in wildcards:
        for f in Path.cwd().glob(w):
            session.log(f"Removing: {f}")
            shutil.rmtree(f, ignore_errors=True)


@nox.session(python=PYTHONS, reuse_venv=True)
def test(session):
    session.log("Using the newest supported Python: %s", is_latest_python(session))
    session.install("-e", f".[{','.join(EXTRAS_REQUIRE.keys())}]")
    session.install(
        "pytest         ~= 4.6",  # Update when https://github.com/UAVCAN/nunavut/issues/144 is fixed
        "pytest-asyncio == 0.10",  # Update when https://github.com/UAVCAN/nunavut/issues/144 is fixed
        "coverage       ~= 5.3",
    )

    # The test suite generates a lot of temporary files, so we change the working directory.
    # We have to symlink the original setup.cfg as well if we run tools from the new directory.
    tmp_dir = Path(session.create_tmp()).resolve()
    session.cd(tmp_dir)
    fn = "setup.cfg"
    if not (tmp_dir / fn).exists():
        (tmp_dir / fn).symlink_to(ROOT_DIR / fn)

    if sys.platform.startswith("linux"):
        # Enable packet capture for the Python executable. This is necessary for testing the UDP capture capability.
        # It can't be done from within the test suite because it has to be done before the interpreter is started.
        session.run("sudo", "setcap", "cap_net_raw+eip", str(Path(session.bin, "python").resolve()), external=True)

    # Launch the TCP broker for testing the UAVCAN/serial transport.
    broker_process = subprocess.Popen(["ncat", "--broker", "--listen", "-p", "50905"], env=session.env)
    time.sleep(1.0)  # Ensure that it has started.
    if broker_process.poll() is not None:
        raise RuntimeError("Could not start the TCP broker")

    # Run the test suite (takes about 10-30 minutes per virtualenv).
    try:
        compiled_dir = Path.cwd().resolve() / ".compiled"
        src_dirs = [
            ROOT_DIR / "pyuavcan",
            ROOT_DIR / "tests",
        ]
        postponed = ROOT_DIR / "pyuavcan" / "application"
        env = {
            "PYTHONASYNCIODEBUG": "1",
            "PYTHONPATH": str(compiled_dir),
        }
        pytest = partial(session.run, "coverage", "run", "-m", "pytest", *session.posargs, env=env)
        # Application-layer tests are run separately after the main test suite because they require DSDL for
        # "uavcan" to be transpiled first. That namespace is transpiled as a side-effect of running the main suite.
        pytest("--ignore", str(postponed), *map(str, src_dirs))
        pytest(str(postponed))
    finally:
        broker_process.terminate()

    # Coverage analysis and report.
    fail_under = 0 if session.posargs else 90
    session.run("coverage", "combine")
    session.run("coverage", "report", f"--fail-under={fail_under}")
    if session.interactive:
        session.run("coverage", "html")
        report_file = Path.cwd().resolve() / "htmlcov" / "index.html"
        session.log(f"COVERAGE REPORT: file://{report_file}")

    # Running lints in the main test session because:
    #   1. MyPy and PyLint require access to the code generated by the test suite.
    #   2. At least MyPy has to be run separately per Python version we support.
    # If the interpreter is not CPython, this may need to be conditionally disabled.
    session.install(
        "mypy   == 0.812",
        "pylint == 2.6.0",
    )
    session.run("mypy", "--strict", *map(str, src_dirs), str(compiled_dir))
    session.run("pylint", *map(str, src_dirs), env={"PYTHONPATH": str(compiled_dir)})

    # Publish coverage statistics. This also has to be run from the test session to access the coverage files.
    if sys.platform.startswith("linux") and is_latest_python(session) and session.env.get("COVERALLS_REPO_TOKEN"):
        session.install("coveralls")
        session.run("coveralls")
    else:
        session.log("Coveralls skipped")

    # Submit analysis to SonarCloud. This also has to be run from the test session to access the coverage files.
    sonarcloud_token = session.env.get("SONARCLOUD_TOKEN")
    if sys.platform.startswith("linux") and is_latest_python(session) and sonarcloud_token:
        session.run("coverage", "xml", "-i", "-o", str(ROOT_DIR / ".coverage.xml"))

        session.run("unzip", str(list(DEPS_DIR.glob("sonar-scanner*.zip"))[0]), silent=True, external=True)
        (sonar_scanner_bin,) = list(Path().cwd().resolve().glob("sonar-scanner*/bin"))
        os.environ["PATH"] = os.pathsep.join([str(sonar_scanner_bin), os.environ["PATH"]])

        session.cd(ROOT_DIR)
        session.run("sonar-scanner", f"-Dsonar.login={sonarcloud_token}", external=True)
    else:
        session.log("SonarQube scan skipped")


@nox.session()
def demo(session):
    """
    Test the demo app orchestration example.
    This is a separate session because it is dependent on Yakut.
    """
    if sys.platform.startswith("win"):
        session.log("This session cannot be run on Windows")
        return 0

    session.install("-e", f".[{','.join(EXTRAS_REQUIRE.keys())}]")
    session.install("git+https://github.com/UAVCAN/yakut@orc")  # TODO: use stable version from PyPI when deployed.

    demo_dir = ROOT_DIR / "demo"
    tmp_dir = Path(session.create_tmp()).resolve()
    session.cd(tmp_dir)

    for s in demo_dir.iterdir():
        if s.name.startswith("."):
            continue
        session.log("Copy: %s", s)
        if s.is_dir():
            shutil.copytree(s, tmp_dir / s.name)
        else:
            shutil.copy(s, tmp_dir)

    session.env["STOP_AFTER"] = "10"
    session.run("yakut", "orc", "launch.orc.yaml", success_codes=[111])


@nox.session(python=PYTHONS)
def pristine(session):
    """
    Install the library into a pristine environment and ensure that it is importable.
    This is needed to catch errors caused by accidental reliance on test dependencies in the main codebase.
    """
    exe = partial(session.run, "python", "-c", silent=True)
    session.cd(session.create_tmp())  # Change the directory to reveal spurious dependencies from the project root.

    session.install(f"{ROOT_DIR}")  # Testing bare installation first.
    exe("import pyuavcan")
    exe("import pyuavcan.transport.can")
    exe("import pyuavcan.transport.udp")
    exe("import pyuavcan.transport.loopback")

    session.install(f"{ROOT_DIR}[transport_serial]")
    exe("import pyuavcan.transport.serial")


@nox.session(reuse_venv=True)
def check_style(session):
    session.install("black == 20.8b1")
    session.run("black", "--check", ".")


@nox.session(reuse_venv=True)
def docs(session):
    try:
        session.run("dot", "-V", silent=True, external=True)
    except Exception:
        session.error("Please install graphviz. It may be available from your package manager as 'graphviz'.")
        raise

    session.install("-r", "docs/requirements.txt")
    out_dir = Path(session.create_tmp()).resolve()
    session.cd("docs")
    sphinx_args = ["-b", "html", "-W", "--keep-going", f"-j{os.cpu_count() or 1}", ".", str(out_dir)]
    session.run("sphinx-build", *sphinx_args)
    session.log(f"DOCUMENTATION BUILD OUTPUT: file://{out_dir}/index.html")


def is_latest_python(session) -> bool:
    return PYTHONS[-1] in session.run("python", "-V", silent=True)
