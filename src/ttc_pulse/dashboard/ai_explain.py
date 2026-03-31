"""Reusable AI chart explanation helpers for Streamlit dashboard pages."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Any

import pandas as pd
import streamlit as st

from ttc_pulse.utils.project_setup import resolve_project_paths

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None


def _load_env_defaults() -> None:
    if load_dotenv is None:
        return
    paths = resolve_project_paths()
    candidates = [
        paths.project_root / ".env",
        paths.project_root / ".env" / ".env",
    ]
    for env_path in candidates:
        if env_path.exists() and env_path.is_file():
            load_dotenv(dotenv_path=env_path, override=False)
            return


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_filters(filters: dict[str, Any]) -> dict[str, str]:
    return {str(key): _stringify(value) for key, value in sorted(filters.items(), key=lambda x: x[0])}


def _compact_frame_for_prompt(frame: pd.DataFrame, max_rows: int = 120, max_columns: int = 20) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    clipped = frame.copy()
    if len(clipped.columns) > max_columns:
        clipped = clipped[clipped.columns[:max_columns]].copy()
    if len(clipped.index) > max_rows:
        clipped = clipped.head(max_rows).copy()
    return clipped


def _json_safe_record(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return _stringify(value)
    return value


def _frame_sample_records(frame: pd.DataFrame, max_rows: int = 40) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    sample = _compact_frame_for_prompt(frame, max_rows=max_rows)
    records: list[dict[str, Any]] = []
    for _, row in sample.iterrows():
        records.append({str(col): _json_safe_record(row[col]) for col in sample.columns})
    return records


def _frame_numeric_summary(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {"row_count": 0, "column_count": int(len(frame.columns))}
    numeric = frame.select_dtypes(include=["number"]).copy()
    summary: dict[str, Any] = {
        "row_count": int(len(frame.index)),
        "column_count": int(len(frame.columns)),
        "columns": [str(col) for col in frame.columns[:30]],
    }
    if numeric.empty:
        return summary

    desc: dict[str, dict[str, float | None]] = {}
    for col in numeric.columns[:12]:
        series = pd.to_numeric(numeric[col], errors="coerce")
        if series.notna().sum() == 0:
            continue
        desc[str(col)] = {
            "min": float(series.min()),
            "max": float(series.max()),
            "mean": float(series.mean()),
        }
    summary["numeric_stats"] = desc
    return summary


def _metric_term_definitions() -> dict[str, str]:
    return {
        "frequency": "Count of disruption events. Higher means disruptions happen more often.",
        "severity_p90": "90th percentile delay severity. Higher means worse severe-delay cases.",
        "regularity_p90": "90th percentile service gap/irregularity. Higher means less reliable spacing/headways.",
        "cause_mix_score": "Cause diversity index (0 to 1). Higher means disruptions come from a wider mix of causes.",
        "composite_score": "Weighted risk score combining frequency, severity, regularity, and cause mix. Higher means higher overall reliability risk.",
        "rank_position": "Relative ordering among peers. Lower rank number is worse when ranking by risk (rank 1 is top hotspot).",
        "event_count": "Aggregated count of events in the selected window. Higher means more total disruption load.",
    }


def _extract_priority_entities(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    entity_col_candidates = [
        "entity_label",
        "entity_id",
        "route_id_gtfs",
        "route_id",
        "station_canonical",
        "station_name",
        "spatial_unit_id",
    ]
    entity_col = next((col for col in entity_col_candidates if col in frame.columns), None)
    if entity_col is None:
        return []

    score_col = "frequency" if "frequency" in frame.columns else None
    if score_col is None:
        score_col = "event_count" if "event_count" in frame.columns else None
    if score_col is None:
        score_col = "composite_score" if "composite_score" in frame.columns else None

    view = frame[[entity_col] + ([score_col] if score_col else [])].copy()
    view[entity_col] = view[entity_col].astype(str).str.strip()
    view = view[view[entity_col] != ""]
    if view.empty:
        return []

    if score_col:
        view[score_col] = pd.to_numeric(view[score_col], errors="coerce").fillna(0.0)
        grouped = (
            view.groupby(entity_col, as_index=False)[score_col]
            .sum()
            .sort_values(score_col, ascending=False)
            .head(10)
        )
        return grouped.to_dict(orient="records")

    grouped = view[[entity_col]].drop_duplicates().head(10)
    return grouped.to_dict(orient="records")


def _build_payload(
    page_name: str,
    chart_id: str,
    chart_title: str,
    filters: dict[str, Any],
    frame: pd.DataFrame,
    notes: str | None = None,
) -> dict[str, Any]:
    normalized_filters = _normalize_filters(filters)
    compact = _compact_frame_for_prompt(frame)
    payload = {
        "page_name": page_name,
        "chart_id": chart_id,
        "chart_title": chart_title,
        "filters": normalized_filters,
        "metric_term_definitions": _metric_term_definitions(),
        "summary": _frame_numeric_summary(compact),
        "priority_entities": _extract_priority_entities(compact),
        "sample_rows": _frame_sample_records(compact, max_rows=40),
        "notes": _stringify(notes),
    }
    return payload


def _payload_hash(payload: dict[str, Any], model_name: str) -> str:
    blob = json.dumps({"model": model_name, "payload": payload}, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ExplainResult:
    status: str
    text: str
    hash_key: str
    model_name: str


def _get_openai_defaults() -> tuple[str, str]:
    _load_env_defaults()
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    model_name = os.getenv("OPENAI_MODEL", "").strip()
    return api_key, model_name


def _create_explanation(model_name: str, api_key: str, payload: dict[str, Any]) -> str:
    if OpenAI is None:
        raise RuntimeError("OpenAI SDK is unavailable. Install `openai` in the environment.")
    client = OpenAI(api_key=api_key)
    system_prompt = (
        "You are an expert TTC reliability analyst. "
        "Given chart context and sampled chart rows, produce a short analysis with exactly three markdown sections: "
        "'### What this chart shows', '### Potential issues', and '### Practical fixes'. "
        "Use concise bullets and focus on dataset insights, not chart-design commentary. "
        "In 'What this chart shows', explain selected metric/term meanings and what high/low values imply in this TTC context. "
        "In 'Potential issues', identify data-indicated operational issues for the displayed routes/stations/time windows/cause groups. "
        "Do not discuss visual/chart formatting issues. "
        "In 'Practical fixes', propose route/station/service actions for the displayed entities and patterns, not chart fixes. "
        "Reference concrete values only when present in payload. "
        "If data is sparse, explicitly say so without inventing values."
    )
    user_prompt = json.dumps(payload, indent=2, ensure_ascii=False)
    response = client.responses.create(
        model=model_name,
        input=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        max_output_tokens=700,
    )
    text = getattr(response, "output_text", None)
    if isinstance(text, str) and text.strip():
        return text.strip()
    return "### What this chart shows\n- No explanation text returned.\n\n### Potential issues\n- Unable to parse model output.\n\n### Practical fixes\n- Retry the explanation request."


def render_ai_explain_block(
    *,
    page_name: str,
    chart_id: str,
    chart_title: str,
    filters: dict[str, Any],
    frame: pd.DataFrame,
    notes: str | None = None,
    disabled: bool = False,
) -> None:
    payload = _build_payload(
        page_name=page_name,
        chart_id=chart_id,
        chart_title=chart_title,
        filters=filters,
        frame=frame,
        notes=notes,
    )
    api_key, model_name = _get_openai_defaults()
    model = model_name or "OPENAI_MODEL not set"
    current_hash = _payload_hash(payload, model)

    state_key = f"ai_explain_result::{page_name}::{chart_id}"
    cache_key = "ai_explain_cache"
    st.session_state.setdefault(cache_key, {})

    result = st.session_state.get(state_key)
    if isinstance(result, dict) and result.get("hash_key") != current_hash:
        result = None

    header_col, button_col = st.columns([5, 1], vertical_alignment="center")
    with header_col:
        st.caption(f"AI Explain: {chart_title}")
    with button_col:
        clicked = st.button(
            "AI Explain",
            key=f"ai_explain_btn::{page_name}::{chart_id}",
            use_container_width=True,
            disabled=disabled or frame.empty,
        )

    if clicked:
        if disabled or frame.empty:
            st.session_state[state_key] = {
                "status": "warning",
                "text": "No chart data is available for explanation in the current filter state.",
                "hash_key": current_hash,
                "model_name": model,
            }
        elif not api_key:
            st.session_state[state_key] = {
                "status": "warning",
                "text": "OPENAI_API_KEY is missing in .env; AI explanation is unavailable.",
                "hash_key": current_hash,
                "model_name": model,
            }
        elif not model_name:
            st.session_state[state_key] = {
                "status": "warning",
                "text": "OPENAI_MODEL is missing in .env; AI explanation is unavailable.",
                "hash_key": current_hash,
                "model_name": model,
            }
        else:
            cached = st.session_state[cache_key].get(current_hash)
            if cached is not None:
                st.session_state[state_key] = cached
            else:
                with st.spinner("Generating AI explanation..."):
                    try:
                        text = _create_explanation(model_name=model_name, api_key=api_key, payload=payload)
                        final = {
                            "status": "ok",
                            "text": text,
                            "hash_key": current_hash,
                            "model_name": model_name,
                        }
                    except Exception as exc:  # pragma: no cover
                        final = {
                            "status": "error",
                            "text": f"AI explanation failed: {type(exc).__name__}: {exc}",
                            "hash_key": current_hash,
                            "model_name": model_name,
                        }
                    st.session_state[state_key] = final
                    st.session_state[cache_key][current_hash] = final

    final_result = st.session_state.get(state_key)
    if not isinstance(final_result, dict):
        return

    status = _stringify(final_result.get("status")).lower()
    text = _stringify(final_result.get("text"))
    active_model = _stringify(final_result.get("model_name"))
    st.caption(f"Model: `{active_model}`")
    if status == "ok":
        st.markdown(text)
    elif status == "warning":
        st.warning(text)
    else:
        st.error(text)


__all__ = ["render_ai_explain_block"]
