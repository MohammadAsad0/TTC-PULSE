from __future__ import annotations

from pathlib import Path
import sys

PAGE_DIR = Path(__file__).resolve().parent.parent
if str(PAGE_DIR) not in sys.path:
    sys.path.insert(0, str(PAGE_DIR))

import streamlit as st

from _shared import render_placeholder_page

st.set_page_config(page_title="Time Patterns", layout="wide")
render_placeholder_page(
    "Time Patterns",
    "Scaffolded page. Add hour/day and periodic delay pattern analysis here.",
)
