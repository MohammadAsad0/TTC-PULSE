from __future__ import annotations

from pathlib import Path
import sys

import streamlit as st


def _bootstrap_src_path() -> None:
    here = Path(__file__).resolve()
    for parent in here.parents:
        src_dir = parent / "src"
        if (src_dir / "ttc_pulse").exists():
            if str(src_dir) not in sys.path:
                sys.path.insert(0, str(src_dir))
            return


_bootstrap_src_path()

from ttc_pulse.dashboard.formatting import fmt_int, status_label
from ttc_pulse.dashboard.loaders import GOLD_TABLE_FILES, get_gold_table_status_frame, resolve_duckdb_path

st.set_page_config(page_title="TTC Pulse", layout="wide")


@st.cache_data(ttl=60)
def _load_table_status() -> tuple[Path, "pd.DataFrame"]:
    import pandas as pd

    db_path = resolve_duckdb_path()
    frame = get_gold_table_status_frame(table_names=GOLD_TABLE_FILES.keys(), db_path=db_path)
    if frame.empty:
        frame = pd.DataFrame(columns=["table_name", "status", "source", "row_count", "message"])
    return db_path, frame


st.subheader("Dashboard Data Status")

db_file, status_frame = _load_table_status()
ready_count = int((status_frame["status"] == "ok").sum()) if "status" in status_frame else 0
empty_count = int((status_frame["status"] == "empty").sum()) if "status" in status_frame else 0
missing_count = int((status_frame["status"] == "missing").sum()) if "status" in status_frame else 0

top_left, top_mid, top_right = st.columns(3)
top_left.metric("Gold Tables Ready", fmt_int(ready_count))
top_mid.metric("Gold Tables Empty", fmt_int(empty_count))
top_right.metric("Gold Tables Missing", fmt_int(missing_count))

st.write(f"DuckDB path: `{db_file.as_posix()}`")
if missing_count > 0:
    st.warning("Some Gold marts are missing. Pages will automatically use parquet-backed reads when available.")

display_frame = status_frame.copy()
if not display_frame.empty and "status" in display_frame.columns:
    display_frame["status"] = display_frame["status"].map(status_label)
st.dataframe(display_frame, use_container_width=True, hide_index=True)

st.caption("Use the sidebar to navigate analysis pages.")
