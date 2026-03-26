from __future__ import annotations

from pathlib import Path
import sys

PAGE_DIR = Path(__file__).resolve().parent.parent
if str(PAGE_DIR) not in sys.path:
    sys.path.insert(0, str(PAGE_DIR))

import streamlit as st

from _shared import render_placeholder_page

st.set_page_config(page_title="Drill-Down Explorer", layout="wide")
render_placeholder_page(
    "Drill-Down Explorer",
    "Scaffolded page. Add detailed drill-down filters and diagnostics here.",
)
