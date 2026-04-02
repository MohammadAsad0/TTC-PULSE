"""Storytelling helpers for stakeholder-first Streamlit pages."""

from __future__ import annotations

import hashlib

import streamlit as st

from ttc_pulse.utils.project_setup import resolve_project_paths, utc_now_iso

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


def _data_artifact_signature() -> str:
    paths = resolve_project_paths()
    tracked_files = [paths.db_path.resolve()]
    for relative_dir in ("silver", "gold"):
        base_dir = (paths.project_root / relative_dir).resolve()
        if not base_dir.exists():
            continue
        tracked_files.extend(sorted(base_dir.glob("*.parquet")))

    digest = hashlib.sha1()
    for file_path in tracked_files:
        if not file_path.exists():
            continue
        stat = file_path.stat()
        digest.update(file_path.as_posix().encode("utf-8"))
        digest.update(str(stat.st_size).encode("utf-8"))
        digest.update(str(stat.st_mtime_ns).encode("utf-8"))
    return digest.hexdigest()


def sync_dashboard_data_cache() -> None:
    """
    Invalidate Streamlit caches when dataset artifacts change after a reload.

    This ensures all pages reflect newly loaded data without waiting for TTL expiry.
    """
    # Manual invalidation path: set when dataset reload completes.
    reload_nonce_key = "ttc_dataset_reload_nonce"
    applied_nonce_key = "ttc_dataset_reload_applied_nonce"
    reload_nonce = st.session_state.get(reload_nonce_key)
    if reload_nonce and st.session_state.get(applied_nonce_key) != reload_nonce:
        st.cache_data.clear()
        st.cache_resource.clear()
        st.session_state[applied_nonce_key] = reload_nonce

    signature_key = "ttc_data_artifact_signature"
    current = _data_artifact_signature()
    previous = st.session_state.get(signature_key)
    if previous is None:
        st.session_state[signature_key] = current
        return
    if previous != current:
        st.cache_data.clear()
        st.cache_resource.clear()
        st.session_state[signature_key] = current


def mark_dataset_reloaded() -> str:
    """Broadcast a dataset refresh signal across all dashboard pages."""
    nonce = utc_now_iso()
    st.session_state["ttc_dataset_reload_nonce"] = nonce
    return nonce


__all__ = [
    "EXPLORATION_MODE",
    "PRESENTATION_MODE",
    "is_presentation_mode",
    "mark_dataset_reloaded",
    "next_question_hint",
    "page_story_header",
    "sync_dashboard_data_cache",
    "story_mode_selector",
]
