from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from pathlib import Path
import sys

import altair as alt
import duckdb
import pandas as pd
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

from ttc_pulse.dashboard.loaders import query_table
from ttc_pulse.utils.project_setup import resolve_project_paths

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None


def _load_env() -> None:
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


def _frame_to_compact_csv(frame: pd.DataFrame, max_rows: int = 12) -> str:
    if frame.empty:
        return "(no rows)"
    clipped = frame.head(max_rows).copy()
    return clipped.to_csv(index=False)


def _extract_route_tokens(question: str) -> list[str]:
    q = question.lower()
    tokens: list[str] = []

    patterns = [
        r"(?:route|bus|line)\s*([a-z0-9]{1,6})",
        r"\b([0-9]{1,4}[a-z]?)\s*(?:route|bus|line)\b",
    ]
    for pattern in patterns:
        for token in re.findall(pattern, q, flags=re.IGNORECASE):
            normalized = str(token).strip().upper()
            if normalized and normalized not in tokens:
                tokens.append(normalized)

    return tokens[:5]


@st.cache_data(ttl=120)
def _build_question_specific_context(question: str) -> str:
    route_tokens = _extract_route_tokens(question)
    if not route_tokens:
        return "No route-specific lookup requested."

    parts: list[str] = []

    for route_token in route_tokens:
        route_gold = query_table(
            table_name="gold_route_time_metrics",
            query_template="""
            SELECT
                mode,
                route_id_gtfs,
                MIN(service_date) AS min_service_date,
                MAX(service_date) AS max_service_date,
                SUM(frequency)::BIGINT AS frequency,
                quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS severity_p90,
                AVG(composite_score) FILTER (WHERE composite_score IS NOT NULL) AS composite_score
            FROM {source}
            WHERE route_id_gtfs IS NOT NULL
                AND UPPER(route_id_gtfs) = UPPER(?)
            GROUP BY 1, 2
            ORDER BY mode
            """,
            params=[route_token],
        )

        if route_gold.status == "ok" and not route_gold.frame.empty:
            parts.append(f"Route-specific Gold metrics for {route_token}:")
            parts.append(_frame_to_compact_csv(route_gold.frame, max_rows=50))

            route_yearly = query_table(
                table_name="gold_route_time_metrics",
                query_template="""
                SELECT
                    mode,
                    CAST(EXTRACT(YEAR FROM service_date) AS INTEGER) AS year,
                    SUM(frequency)::BIGINT AS frequency,
                    quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS severity_p90
                FROM {source}
                WHERE route_id_gtfs IS NOT NULL
                    AND UPPER(route_id_gtfs) = UPPER(?)
                    AND service_date IS NOT NULL
                GROUP BY 1, 2
                ORDER BY mode, year
                """,
                params=[route_token],
            )
            if route_yearly.status == "ok" and not route_yearly.frame.empty:
                parts.append(f"Route {route_token} yearly trend:")
                parts.append(_frame_to_compact_csv(route_yearly.frame, max_rows=100))
            continue

        # Fallback to silver bus events to prove raw-like presence even when GTFS linkage is missing.
        silver_path = resolve_project_paths().project_root / "silver" / "silver_bus_events.parquet"
        if silver_path.exists():
            connection = duckdb.connect(":memory:")
            try:
                safe_path = silver_path.as_posix().replace("'", "''")
                silver_frame = connection.execute(
                    f"""
                    SELECT
                        COALESCE(NULLIF(route_short_name_norm, ''), NULLIF(route_label_raw, ''), 'UNKNOWN') AS route_key,
                        COUNT(*)::BIGINT AS rows_found,
                        MIN(service_date) AS min_service_date,
                        MAX(service_date) AS max_service_date
                    FROM read_parquet('{safe_path}')
                    WHERE UPPER(COALESCE(route_short_name_norm, '')) = UPPER(?)
                        OR UPPER(COALESCE(route_label_raw, '')) = UPPER(?)
                        OR UPPER(COALESCE(route_label_raw, '')) LIKE UPPER(?)
                    GROUP BY 1
                    ORDER BY rows_found DESC
                    LIMIT 20
                    """,
                    [route_token, route_token, f"%{route_token}%"],
                ).df()
            except Exception:
                silver_frame = pd.DataFrame()
            finally:
                connection.close()

            if not silver_frame.empty:
                parts.append(f"Route token {route_token} found in silver bus events (raw-derived):")
                parts.append(_frame_to_compact_csv(silver_frame, max_rows=20))
            else:
                parts.append(f"No records matched route token {route_token} in Gold route metrics or silver bus events.")
        else:
            parts.append(f"No records matched route token {route_token} in Gold route metrics, and silver bus events parquet is missing.")

    return "\n".join(parts) if parts else "No route-specific lookup results were found."


@st.cache_data(ttl=300)
def _build_dataset_context() -> str:
    context_chunks: list[str] = []

    coverage = query_table(
        table_name="gold_delay_events_core",
        query_template="""
        SELECT
            MIN(service_date) AS min_service_date,
            MAX(service_date) AS max_service_date,
            SUM(event_count)::BIGINT AS total_events,
            COUNT(*)::BIGINT AS aggregate_rows
        FROM {source}
        """,
    )
    if coverage.status in {"ok", "empty"} and not coverage.frame.empty:
        context_chunks.append("Dataset coverage and size:")
        context_chunks.append(_frame_to_compact_csv(coverage.frame, max_rows=1))

    by_mode = query_table(
        table_name="gold_delay_events_core",
        query_template="""
        SELECT
            mode,
            SUM(event_count)::BIGINT AS total_events,
            quantile_cont(min_delay_p90, 0.9) FILTER (WHERE min_delay_p90 IS NOT NULL) AS delay_p90_of_p90
        FROM {source}
        GROUP BY 1
        ORDER BY total_events DESC
        """,
    )
    if by_mode.status in {"ok", "empty"} and not by_mode.frame.empty:
        context_chunks.append("Events by mode:")
        context_chunks.append(_frame_to_compact_csv(by_mode.frame))

    yearly_trend = query_table(
        table_name="gold_delay_events_core",
        query_template="""
        SELECT
            mode,
            CAST(EXTRACT(YEAR FROM service_date) AS INTEGER) AS year,
            SUM(event_count)::BIGINT AS events
        FROM {source}
        WHERE service_date IS NOT NULL
        GROUP BY 1, 2
        ORDER BY mode, year
        """,
    )
    if yearly_trend.status in {"ok", "empty"} and not yearly_trend.frame.empty:
        context_chunks.append("Yearly trend by mode:")
        context_chunks.append(_frame_to_compact_csv(yearly_trend.frame, max_rows=40))

    top_bus_routes = query_table(
        table_name="gold_route_time_metrics",
        query_template="""
        SELECT
            route_id_gtfs,
            SUM(frequency)::BIGINT AS frequency,
            quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS severity_p90,
            AVG(composite_score) FILTER (WHERE composite_score IS NOT NULL) AS composite_score
        FROM {source}
        WHERE mode = 'bus'
            AND route_id_gtfs IS NOT NULL
        GROUP BY 1
        ORDER BY frequency DESC
        LIMIT 15
        """,
    )
    if top_bus_routes.status in {"ok", "empty"} and not top_bus_routes.frame.empty:
        context_chunks.append("Top bus routes by frequency:")
        context_chunks.append(_frame_to_compact_csv(top_bus_routes.frame))

    top_subway_lines = query_table(
        table_name="gold_route_time_metrics",
        query_template="""
        SELECT
            route_id_gtfs,
            SUM(frequency)::BIGINT AS frequency,
            quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS severity_p90,
            AVG(composite_score) FILTER (WHERE composite_score IS NOT NULL) AS composite_score
        FROM {source}
        WHERE mode = 'subway'
            AND route_id_gtfs IS NOT NULL
        GROUP BY 1
        ORDER BY frequency DESC
        LIMIT 15
        """,
    )
    if top_subway_lines.status in {"ok", "empty"} and not top_subway_lines.frame.empty:
        context_chunks.append("Top subway lines by frequency:")
        context_chunks.append(_frame_to_compact_csv(top_subway_lines.frame))

    top_stations = query_table(
        table_name="gold_station_time_metrics",
        query_template="""
        SELECT
            station_canonical,
            SUM(frequency)::BIGINT AS frequency,
            quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS severity_p90,
            AVG(composite_score) FILTER (WHERE composite_score IS NOT NULL) AS composite_score
        FROM {source}
        WHERE station_canonical IS NOT NULL
        GROUP BY 1
        ORDER BY frequency DESC
        LIMIT 15
        """,
    )
    if top_stations.status in {"ok", "empty"} and not top_stations.frame.empty:
        context_chunks.append("Top subway stations by frequency:")
        context_chunks.append(_frame_to_compact_csv(top_stations.frame))

    if not context_chunks:
        return (
            "No Gold dataset context is available in DuckDB/parquet right now. "
            "Answer with general TTC guidance and ask the user to load/build dataset artifacts."
        )

    return "\n".join(context_chunks)


def _extract_response_text(response: object) -> str:
    output_text = getattr(response, "output_text", None)
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()

    output = getattr(response, "output", None)
    if not isinstance(output, list):
        return "I could not parse the model response."

    chunks: list[str] = []
    for item in output:
        contents = getattr(item, "content", None)
        if not isinstance(contents, list):
            continue
        for content in contents:
            text = getattr(content, "text", None)
            if isinstance(text, str) and text.strip():
                chunks.append(text.strip())
    if chunks:
        return "\n\n".join(chunks)
    return "I could not parse the model response."


def _chat_with_openai(model_name: str, api_key: str, question: str, data_context: str) -> str:
    if OpenAI is None:
        return "OpenAI SDK is not installed. Add `openai` to requirements and reinstall dependencies."

    client = OpenAI(api_key=api_key)

    system_prompt = (
        "You are TTC Pulse AI analyst. "
        "Use the provided dataset context as the primary evidence. "
        "When asked for patterns, quantify with numbers from context when available. "
        "When asked for quick fixes, provide practical, data-driven actions. "
        "When asked for future predictions (e.g., 2026/2027 delays), clearly label assumptions and uncertainty. "
        "Do not invent metrics that are not in context. "
        "If context is missing for a claim, say what is missing and give the closest safe answer."
    )

    user_prompt = (
        "Dataset context:\n"
        f"{data_context}\n\n"
        "User question:\n"
        f"{question}"
    )

    response = client.responses.create(
        model=model_name,
        input=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        max_output_tokens=900,
    )
    return _extract_response_text(response)


def _detect_viz_intent(question: str) -> str | None:
    q = question.lower()
    viz_keywords = ["chart", "plot", "graph", "visualize", "visualization", "figure"]
    if not any(token in q for token in viz_keywords):
        return None

    trend_keywords = ["trend", "over time", "monthly", "month", "yearly", "year", "pattern"]
    if any(token in q for token in trend_keywords):
        return "monthly_trend"
    if "bus" in q or "route" in q:
        return "top_bus_routes"
    if "subway" in q or "station" in q:
        return "top_subway_stations"
    return "monthly_trend"


def _build_chat_export_markdown(messages: list[dict[str, object]]) -> str:
    exported_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    lines = [
        "# TTC Pulse AI Chat Export",
        "",
        f"Exported at (UTC): {exported_at}",
        "",
    ]
    for message in messages:
        role_raw = str(message.get("role", "assistant")).strip().lower()
        role_title = "User" if role_raw == "user" else "Assistant"
        content = str(message.get("content", "")).strip()
        lines.append(f"## {role_title}")
        lines.append("")
        lines.append(content)
        lines.append("")
    return "\n".join(lines).strip() + "\n"



def _build_chat_export_html(messages: list[dict[str, object]]) -> str:
    exported_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    parts = [
        "<html><head><meta charset='utf-8'><title>TTC Pulse AI Chat Export</title>",
        "<style>body{font-family:Arial,sans-serif;margin:24px;line-height:1.5;}h1{margin-bottom:4px;}",
        ".meta{color:#555;margin-bottom:20px;} .msg{margin:16px 0;padding:12px;border-radius:8px;}",
        ".user{background:#f2f7ff;border:1px solid #d3e4ff;} .assistant{background:#f8f8f8;border:1px solid #e2e2e2;}",
        "</style></head><body>",
        "<h1>TTC Pulse AI Chat Export</h1>",
        f"<div class='meta'>Exported at (UTC): {exported_at}</div>",
    ]

    for message in messages:
        role_raw = str(message.get("role", "assistant")).strip().lower()
        role_title = "User" if role_raw == "user" else "Assistant"
        css_class = "user" if role_raw == "user" else "assistant"
        content = str(message.get("content", "")).strip()
        safe = (
            content.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace("\n", "<br>")
        )
        parts.append(f"<div class='msg {css_class}'><strong>{role_title}</strong><br>{safe}</div>")

    parts.append("</body></html>")
    return "".join(parts)
def _render_visualization(intent: str) -> None:
    if intent == "monthly_trend":
        result = query_table(
            table_name="gold_delay_events_core",
            query_template="""
            SELECT
                DATE_TRUNC('month', service_date) AS month_start,
                mode,
                SUM(event_count)::BIGINT AS events
            FROM {source}
            WHERE service_date IS NOT NULL
            GROUP BY 1, 2
            ORDER BY month_start, mode
            """,
        )
        if result.status == "ok" and not result.frame.empty:
            frame = result.frame.copy()
            chart = (
                alt.Chart(frame)
                .mark_line(point=True)
                .encode(
                    x=alt.X("month_start:T", title="Month"),
                    y=alt.Y("events:Q", title="Events"),
                    color=alt.Color("mode:N", title="Mode"),
                    tooltip=["month_start:T", "mode:N", "events:Q"],
                )
                .properties(title="Monthly Delay Events Trend", height=360)
                .interactive()
            )
            st.altair_chart(chart, use_container_width=True)
        return

    if intent == "top_bus_routes":
        result = query_table(
            table_name="gold_route_time_metrics",
            query_template="""
            SELECT
                route_id_gtfs,
                SUM(frequency)::BIGINT AS frequency
            FROM {source}
            WHERE mode = 'bus'
                AND route_id_gtfs IS NOT NULL
            GROUP BY 1
            ORDER BY frequency DESC
            LIMIT 20
            """,
        )
        if result.status == "ok" and not result.frame.empty:
            frame = result.frame.copy()
            chart = (
                alt.Chart(frame)
                .mark_bar()
                .encode(
                    x=alt.X("frequency:Q", title="Frequency"),
                    y=alt.Y("route_id_gtfs:N", title="Route", sort="-x"),
                    tooltip=["route_id_gtfs:N", "frequency:Q"],
                )
                .properties(title="Top Bus Routes by Frequency", height=420)
            )
            st.altair_chart(chart, use_container_width=True)
        return

    if intent == "top_subway_stations":
        result = query_table(
            table_name="gold_station_time_metrics",
            query_template="""
            SELECT
                station_canonical,
                SUM(frequency)::BIGINT AS frequency
            FROM {source}
            WHERE station_canonical IS NOT NULL
            GROUP BY 1
            ORDER BY frequency DESC
            LIMIT 20
            """,
        )
        if result.status == "ok" and not result.frame.empty:
            frame = result.frame.copy()
            chart = (
                alt.Chart(frame)
                .mark_bar()
                .encode(
                    x=alt.X("frequency:Q", title="Frequency"),
                    y=alt.Y("station_canonical:N", title="Station", sort="-x"),
                    tooltip=["station_canonical:N", "frequency:Q"],
                )
                .properties(title="Top Subway Stations by Frequency", height=420)
            )
            st.altair_chart(chart, use_container_width=True)


_load_env()

st.title("AI-chat bot")
st.caption(
    "Ask TTC Pulse questions about delay patterns, potential fixes, and forward-looking risk trends based on the loaded dataset."
)

if "ai_chat_messages" not in st.session_state:
    st.session_state["ai_chat_messages"] = [
        {
            "role": "assistant",
            "content": "Ask a TTC question (patterns, causes, route risks, or prediction scenarios for 2026/2027).",
            "viz_intent": None,
        }
    ]

model_default = os.getenv("OPENAI_MODEL", "gpt-5.4-mini")
api_key = os.getenv("OPENAI_API_KEY", "").strip()

controls_left, controls_right = st.columns([5, 1], vertical_alignment="bottom")
with controls_left:
    model_name = st.text_input("OpenAI model", value=model_default, help="Example: gpt-5.4-mini or gpt-5.4")
with controls_right:
    if st.button("Clear chat", use_container_width=True):
        st.session_state["ai_chat_messages"] = [
            {
                "role": "assistant",
                "content": "Chat reset. Ask a new TTC question.",
                "viz_intent": None,
            }
        ]
        st.rerun()

if not api_key:
    st.warning("OPENAI_API_KEY is missing. Add it to a .env file at project root.")

for message in st.session_state["ai_chat_messages"]:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if message["role"] == "assistant":
            intent = message.get("viz_intent")
            if isinstance(intent, str) and intent:
                _render_visualization(intent)

question = st.chat_input("Ask a TTC Pulse question")
if question:
    st.session_state["ai_chat_messages"].append({"role": "user", "content": question})

    if not api_key:
        answer = "OPENAI_API_KEY is not configured, so I cannot call the model yet."
    elif not model_name.strip():
        answer = "Model name is empty. Enter an OpenAI model such as gpt-5.4-mini."
    else:
        with st.spinner("Analyzing TTC dataset context..."):
            data_context = _build_dataset_context()
            question_context = _build_question_specific_context(question)
            combined_context = (
                f"{data_context}\n\n"
                "Question-specific lookup context:\n"
                f"{question_context}"
            )
            try:
                answer = _chat_with_openai(model_name.strip(), api_key, question, combined_context)
            except Exception as exc:  # pragma: no cover
                answer = f"OpenAI request failed: {type(exc).__name__}: {exc}"

    st.session_state["ai_chat_messages"].append(
        {
            "role": "assistant",
            "content": answer,
            "viz_intent": _detect_viz_intent(question),
        }
    )
    st.rerun()

with st.expander("Dataset context sent to the model", expanded=False):
    st.text(_build_dataset_context())

export_md = _build_chat_export_markdown(st.session_state["ai_chat_messages"])
export_html = _build_chat_export_html(st.session_state["ai_chat_messages"])

export_col_a, export_col_b = st.columns(2)
with export_col_a:
    st.download_button(
        "Export chat (.md)",
        data=export_md,
        file_name="ttc_pulse_ai_chat_export.md",
        mime="text/markdown",
        use_container_width=True,
    )
with export_col_b:
    st.download_button(
        "Export printable (.html)",
        data=export_html,
        file_name="ttc_pulse_ai_chat_export.html",
        mime="text/html",
        use_container_width=True,
    )





