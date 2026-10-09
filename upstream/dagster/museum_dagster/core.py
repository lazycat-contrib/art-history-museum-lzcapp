"""What the museum's Dagster code shares: the repo's paths and limits, and the command runner.

Every step is a plain command run on this machine, the same as by hand (archive/README.md):
`archive/.venv/Scripts/python -m archive.<module> <command>`. Dagster runs in dagster/.venv and only starts
those commands, through the scraper_framework.dagster kit shared with scrape-cars: a time limit, everything a
command started stopped with it, a TEMP folder of its own.
"""
from __future__ import annotations

import os
import tempfile
from datetime import timedelta
from pathlib import Path

from scraper_framework.dagster import env as kit_env, procs
from scraper_framework.dagster.procs import StepError  # noqa: F401 - the repo's name for it

REPO = Path(__file__).resolve().parents[2]
HOME = Path(os.environ.get("DAGSTER_HOME") or REPO / "dagster" / "home")
DOTENV = REPO / ".env.local"
ARCHIVE_PYTHON = REPO / "archive" / ".venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
# Outside the repo: C:\MyDrive is a Google Drive mirror.
COMMAND_TMP = Path(os.environ.get("MUSEUM_COMMAND_TMP") or Path(tempfile.gettempdir()) / "museum-dagster")
STEP_EXIT_ERRORS = HOME / "logs" / "step_exit_errors.log"

# A full WikiArt crawl is about a day at 3 requests a second, the media mirror half a day. Both resume, so a
# run stopped by its limit is simply launched again.
TIMEOUT = {"short": timedelta(hours=1), "crawl": timedelta(hours=48), "media": timedelta(hours=48)}

procs.tolerate_step_exit_winerror6(STEP_EXIT_ERRORS)


def env_file() -> dict[str, str]:
    return kit_env.env_file(DOTENV)


def archive_python() -> str:
    """archive/.venv (run_dagster.cmd setup installs it), unless PIPELINE_PYTHON names another one."""
    return kit_env.pipeline_python(env_file(), default=str(ARCHIVE_PYTHON))


def pipeline_env() -> dict[str, str]:
    return kit_env.pipeline_env(env_file())


def archive_argv(module: str, *args) -> list[str]:
    return [archive_python(), "-m", f"archive.{module}", *[str(a) for a in args]]


def run_command(argv: list, *, label: str, timeout: timedelta, log=None) -> tuple[int, list[str]]:
    return procs.run_command(argv, label=label, timeout=timeout, env=pipeline_env(), cwd=REPO,
                             tmp_root=COMMAND_TMP, log=log)
