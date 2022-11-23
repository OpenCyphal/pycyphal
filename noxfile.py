# Copyright (c) 2020 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>
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

PYTHONS = ["3.7", "3.8", "3.9", "3.10"]
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


MYPY_VERSION = "0.961"


@nox.session(reuse_venv=True)
def mypy(session):
    sys.path += [str(ROOT_DIR)]
    session.env["PYTHONASYNCIODEBUG"] = "1"
    os.environ["PYTHONASYNCIODEBUG"] = "1"
    tmp_dir = Path(session.create_tmp()).resolve()
    session.cd(tmp_dir)
    if not (ROOT_DIR / "demo" / "public_regulated_data_types").exists():
        session.run("git", "submodule", "update", "--init", "--recursive", external=True)
    from tests.dsdl.conftest import compile_no_cache

    compile_no_cache()
    compiled_dir = Path.cwd().resolve() / ".compiled"
    dirs = [
        ROOT_DIR / "pycyphal",
        ROOT_DIR / "tests",
        compiled_dir
    ]
    env = {
        "PYTHONASYNCIODEBUG": "1",
        "PYTHONPATH": str(compiled_dir),
    }
    session.install("mypy == " + MYPY_VERSION)
    session.cd(ROOT_DIR)
    separator = ":"
    if sys.platform == "win32":
        separator = ";"
    if session.env.get("PYTHONPATH"):
        session.env["PYTHONPATH"] = session.env["PYTHONPATH"] + separator + str(compiled_dir)
    else:
        session.env["PYTHONPATH"] = str(compiled_dir)
    relaxed_static_analysis = "3.7" in session.run("python", "-V", silent=True)  # Old Pythons require relaxed checks.
    if not relaxed_static_analysis:
        session.run(
            "mypy",
            "--config-file",
            str(ROOT_DIR / "setup.cfg"),
            "--strict",
            *map(str, dirs)
        , env=env)


@nox.session(reuse_venv=True)
def test(session):
    session.log("Using the newest supported Python: %s", is_latest_python(session))
    session.install("-e", f".[{','.join(EXTRAS_REQUIRE.keys())}]")
    session.install(
        "pytest         ~= 7.1",
        "pytest-asyncio == 0.18",
        "coverage       ~= 6.3",
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

    # Launch the TCP broker for testing the Cyphal/serial transport.
    broker_process = subprocess.Popen(["ncat", "--broker", "--listen", "-p", "50905"], env=session.env)
    time.sleep(1.0)  # Ensure that it has started.
    if broker_process.poll() is not None:
        raise RuntimeError("Could not start the TCP broker")

    # Run the test suite (takes about 10-30 minutes per virtualenv).
    try:
        compiled_dir = Path.cwd().resolve() / ".compiled"
        src_dirs = [
            ROOT_DIR / "pycyphal",
            ROOT_DIR / "tests",
        ]
        postponed = ROOT_DIR / "pycyphal" / "application"
        env = {
            "PYTHONASYNCIODEBUG": "1",
            "PYTHONPATH": str(compiled_dir),
        }
        pytest = partial(session.run, "coverage", "run", "-m", "pytest", [], env=env)
        # Application-layer tests are run separately after the main test suite because they require DSDL for
        # "uavcan" to be transpiled first. That namespace is transpiled as a side-effect of running the main suite.
        pytest("--ignore", str(postponed), *map(str, src_dirs))
        pytest(str(postponed))
    finally:
        broker_process.terminate()

    # Coverage analysis and report.
    fail_under = 0 if session.posargs else 80
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
        "mypy   == " + MYPY_VERSION,
        "pylint == 2.14.*",
    )
    relaxed_static_analysis = "3.7" in session.run("python", "-V", silent=True)  # Old Pythons require relaxed checks.
    if not relaxed_static_analysis:
        session.run("mypy", "--strict", *map(str, src_dirs), str(compiled_dir))
    session.run("pylint", *map(str, src_dirs), env={"PYTHONPATH": str(compiled_dir)})


@nox.session()
def demo(session):
    """
    Test the demo app orchestration example.
    This is a separate session because it is dependent on Yakut.
    """
    if sys.platform.startswith("win") or "3.7" in session.run("python", "-V", silent=True):  # Drop 3.7 check when EOLed
        session.log("This session cannot be run on in this environment")
        return 0

    session.install("-e", f".[{','.join(EXTRAS_REQUIRE.keys())}]")
    session.install("yakut ~= 0.11")

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


def is_latest_python(session) -> bool:
    return PYTHONS[-1] in session.run("python", "-V", silent=True)
