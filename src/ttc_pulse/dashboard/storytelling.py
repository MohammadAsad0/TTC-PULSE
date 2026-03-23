"""Storytelling helpers for stakeholder-first Streamlit pages."""

from __future__ import annotations

import streamlit as st

PRESENTATION_MODE = "Presentation"
EXPLORATION_MODE = "Exploration"


def story_mode_selector(sidebar: bool = True, key: str = "ttc_story_mode") -> str:
    """Return active story mode and render a shared mode control."""
    target = st.sidebar if sidebar else st
    mode = target.radio(
        "View Mode",
        options=[PRESENTATION_MODE, EXPLORATION_MODE],
        index=0,
        key=key,
        help="Presentation trims controls and tables; Exploration keeps full analytical depth.",
    )
    return mode


def is_presentation_mode(mode: str) -> bool:
    return str(mode).strip().lower() == PRESENTATION_MODE.lower()


def page_story_header(audience_question: str, takeaway: str) -> None:
    st.markdown(f"**Audience Question:** {audience_question}")
    st.success(f"**Takeaway:** {takeaway}")


def next_question_hint(text: str) -> None:
    st.info(f"Next Question: {text}")


__all__ = [
    "EXPLORATION_MODE",
    "PRESENTATION_MODE",
    "is_presentation_mode",
    "next_question_hint",
    "page_story_header",
    "story_mode_selector",
]
