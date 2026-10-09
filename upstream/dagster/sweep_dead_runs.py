"""Before Dagster starts: clear what it left behind (called by run_dagster.ps1 start; scraper_framework.dagster.sweep).

Runs left STARTING/STARTED/CANCELING by a stop mid-run are marked failed and their pool slots freed, old step
logs are removed, and so is the commands' temp folder. The crawls resume, so a failed run is launched again.
"""
from scraper_framework.dagster import sweep as kit

from museum_dagster.core import COMMAND_TMP

if __name__ == "__main__":
    kit.main(COMMAND_TMP, advice=" Launch it again: the crawls resume where they stopped.")
