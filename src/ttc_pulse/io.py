from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pandas as pd


def list_csv_files(folder: Path, predicate: Callable[[Path], bool] | None = None) -> list[Path]:
    if not folder.exists():
        return []
    files = [path for path in folder.iterdir() if path.is_file() and path.suffix.lower() == ".csv"]
    if predicate is not None:
        files = [path for path in files if predicate(path)]
    return sorted(files)


def load_csv_files(folder: Path, predicate: Callable[[Path], bool] | None = None) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in list_csv_files(folder, predicate=predicate):
        frame = pd.read_csv(path)
        frame["source_file"] = path.name
        frames.append(frame)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)
