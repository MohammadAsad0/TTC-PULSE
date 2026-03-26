from __future__ import annotations

from pathlib import Path
import sys

PAGE_DIR = Path(__file__).resolve().parent.parent
if str(PAGE_DIR) not in sys.path:
    sys.path.insert(0, str(PAGE_DIR))

import streamlit as st

from _shared import get_data, render_cache_controls, render_dataset_explorer

st.set_page_config(page_title="Dataset Explorer", layout="wide")

render_cache_controls()

data = get_data()
render_dataset_explorer(data["bus"], data["subway"])
