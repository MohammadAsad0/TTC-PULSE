"""Central path constants for the TTC Pulse project."""

from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PACKAGE_ROOT.parent
PROJECT_ROOT = SRC_ROOT.parent

AIRFLOW_ROOT = PROJECT_ROOT / "airflow"
APP_ROOT = PROJECT_ROOT / "app"
DATA_ROOT = PROJECT_ROOT / "data"
RAW_ROOT = PROJECT_ROOT / "raw"
OUTPUTS_ROOT = PROJECT_ROOT / "outputs"
LOGS_ROOT = PROJECT_ROOT / "logs"

__all__ = [
    "AIRFLOW_ROOT",
    "APP_ROOT",
    "DATA_ROOT",
    "LOGS_ROOT",
    "OUTPUTS_ROOT",
    "PACKAGE_ROOT",
    "PROJECT_ROOT",
    "RAW_ROOT",
    "SRC_ROOT",
]
