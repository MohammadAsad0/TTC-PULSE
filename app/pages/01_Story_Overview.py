from __future__ import annotations

from pathlib import Path
import sys

PAGE_DIR = Path(__file__).resolve().parent.parent
if str(PAGE_DIR) not in sys.path:
    sys.path.insert(0, str(PAGE_DIR))

import streamlit as st

from _shared import get_data, render_story_overview

st.set_page_config(page_title="Story Overview", layout="wide")

data = get_data()
render_story_overview(data["bus"], data["subway"])
