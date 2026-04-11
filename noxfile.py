from __future__ import annotations

import shutil
from pathlib import Path
import nox

nox.options.sessions = ["test", "mypy", "lint", "format"]

PYTHONS = ["3.11", "3.12", "3.13"]


@nox.session(python=False, default=False)
def clean(session):
    pats = ["dist", "build", "html*", ".coverage*", ".*cache", "src/*.egg-info", "*.log", "*.tmp", ".nox"]
    for w in pats:
        for f in Path.cwd().glob(w):
            session.log(f"Removing: {f}")
            if f.is_dir():
                shutil.rmtree(f, ignore_errors=True)
            else:
                f.unlink(missing_ok=True)
    for f in Path.cwd().rglob("__pycache__"):
        session.log(f"Removing: {f}")
        shutil.rmtree(f, ignore_errors=True)


@nox.session(python=PYTHONS)
def test(session: nox.Session) -> None:
    session.install("-e", ".[udp,pythoncan]", "pytest", "pytest-asyncio", "pytest-timeout", "coverage")
    session.run("coverage", "run", "-m", "pytest", "--timeout=60", "tests/", *session.posargs)
    session.run("coverage", "report")
    session.run("coverage", "html")


@nox.session(python=PYTHONS[0])
def mypy(session: nox.Session) -> None:
    session.install(".[udp,pythoncan]", "mypy", "pytest", "pytest-asyncio")
    session.run("mypy", "src/pycyphal2", "tests")


@nox.session(python=PYTHONS[0])
def lint(session: nox.Session) -> None:
    session.install("ruff")
    session.run("ruff", "check", "src", "tests", "examples")


@nox.session(python=PYTHONS[0])
def format(session: nox.Session) -> None:
    session.install("black")
    session.run("black", "--check", "--diff", "src", "tests", "examples")


@nox.session(python=PYTHONS[0], reuse_venv=True)
def docs(session: nox.Session) -> None:
    session.install("-e", ".[udp]", "pdoc")
    session.run("python", "docs/build.py")
    session.log("Docs written to html_docs/")


@nox.session(python=PYTHONS[0], default=False)
def examples(session: nox.Session) -> None:
    import json as _json
    import subprocess
    import sys
    import time

    session.install(".[udp]")
    topic = "demo/time"
    python = str(Path(session.bin) / "python")

    def run_case(label: str, extra_args: list[str]) -> None:
        session.log(f"--- examples smoke: {label} ---")
        sub_proc = subprocess.Popen(
            [python, "examples/subscribe.py", topic, "--timeout", "10", *extra_args],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        time.sleep(1)  # let the subscriber set up

        session.run(python, "examples/publish_time.py", topic, "--count", "3", *extra_args, external=True)
        time.sleep(1)  # let the last message propagate

        sub_proc.terminate()
        stdout, _ = sub_proc.communicate(timeout=5)
        lines = [ln for ln in stdout.decode().splitlines() if ln.strip()]
        session.log(f"Subscriber captured {len(lines)} line(s)")
        assert len(lines) >= 1, f"Expected at least 1 JSONL line, got {len(lines)}"
        for ln in lines:
            obj = _json.loads(ln)
            assert "ts" in obj
            assert "remote_id" in obj
            assert "topic" in obj
            assert "message_b64" in obj

    run_case("udp", [])
    if sys.platform == "linux" and Path("/sys/class/net/vcan0").exists():
        run_case("socketcan:vcan0", ["--transport", "socketcan:vcan0"])
    else:
        session.log("Skipping socketcan:vcan0 case (vcan0 not available)")
