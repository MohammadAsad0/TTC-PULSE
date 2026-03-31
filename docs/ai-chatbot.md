# AI-Chatbot Page (`app/pages/08_AI_Chat_Bot.py`)

## Purpose
The AI-chatbot page adds a conversational analysis layer on top of TTC-PULSE datasets. It lets users ask free-form TTC questions (patterns, route issues, likely future risk, and operational suggestions) and receives model-generated answers grounded in the project data.

This page is designed to:
- Use already-ingested TTC data (gold/silver artifacts) as context for AI answers.
- Support route-specific questions (for example, bus route `113`) with targeted lookups.
- Keep chat history in Streamlit session state for a continuous conversation.
- Offer downloadable chat exports (`.md` and printable `.html`).

## Where It Fits in the Project
Within the Streamlit app, this page is the conversational interface for the analytics stack:
- Data ingestion/processing builds parquet + DuckDB-backed tables.
- Dashboard loaders (`ttc_pulse.dashboard.loaders.query_table`) expose query access.
- The AI-chatbot page queries those tables, builds compact context, and sends that context to OpenAI.

So this page does not replace analytics pages; it summarizes and interprets the same dataset in natural language.

## High-Level Runtime Flow
1. Load environment variables from `.env` using `_load_env()`.
2. Initialize chat state (`st.session_state["ai_chat_messages"]`) if first run.
3. Render existing chat messages (assistant + user).
4. On new question:
- Build global dataset context (`_build_dataset_context`).
- Build question-specific route context (`_build_question_specific_context`).
- Combine both contexts and call OpenAI via `_chat_with_openai`.
- Store assistant response in session state.
- Optionally render a chart if a visualization intent is detected.
5. Allow exports through bottom-of-page download buttons.

## Key Components

### 1) Environment + Imports
- `.env` keys used:
- `OPENAI_API_KEY`
- `OPENAI_MODEL`
- The model name is read from `.env` and used directly.
- If API key is missing, the page stays usable and shows a warning instead of crashing.

### 2) Context Builders

#### `_build_dataset_context()`
Creates compact, general context from gold tables, including:
- Overall coverage window and event totals.
- Events by mode (bus/subway/streetcar where available).
- Yearly trend by mode.
- Top bus routes by frequency.
- Top subway lines/stations by frequency.

This gives the model global system-level context.

#### `_build_question_specific_context(question)`
Extracts route tokens from the user question and performs focused lookups:
- Queries route-level gold metrics by `route_id_gtfs`.
- Adds yearly trend for matched route.
- Falls back to silver bus events parquet when gold route linkage is missing.

This is important for route-specific questions so responses are not generic.

### 3) OpenAI Call

#### `_chat_with_openai(model_name, api_key, question, data_context)`
- Builds a system prompt that forces data-grounded behavior.
- Sends context + question with `client.responses.create(...)`.
- Uses controlled token budget (`max_output_tokens=900`).
- Parses response text through `_extract_response_text`.

The prompt explicitly asks the model to:
- Quantify when possible.
- Label assumptions for predictions.
- Avoid inventing unsupported metrics.

### 4) Visualization Intent (Optional)

#### `_detect_viz_intent(question)` and `_render_visualization(intent)`
If user asks for graph/chart/plot, the page auto-renders one supporting chart after the AI message:
- Monthly delay trend.
- Top bus routes by frequency.
- Top subway stations by frequency.

These charts are generated from query results (Altair), not model output image generation.

### 5) Session State + UX
- `st.session_state["ai_chat_messages"]` stores all messages in this session.
- Clear chat button resets conversation to a starter assistant message.
- Chat UI uses `st.chat_input` and `st.chat_message` for scrollable, conversation-style interaction.

### 6) Export
Two export formats are generated from session messages:
- Markdown: `ttc_pulse_ai_chat_export.md`
- Printable HTML: `ttc_pulse_ai_chat_export.html`

This lets users keep a record of recommendations and analysis sessions.

## Error Handling and Safety
- Missing `openai` package: returns actionable message.
- Missing API key: warning shown, app remains functional.
- API exceptions: shown inline as non-fatal message.
- Empty/missing dataset context: chatbot falls back to safe guidance and indicates limitations.

## Caching and Performance
- `@st.cache_data(ttl=300)` on global dataset context reduces repeated expensive queries.
- `@st.cache_data(ttl=120)` on question-specific context reduces repeated lookups for similar route questions.
- Context is intentionally compact (CSV snippets with row caps) to control latency and cost.

## Expected Inputs/Dependencies
- A valid `.env` at project root containing:
- `OPENAI_API_KEY=...`
- `OPENAI_MODEL=...`
- Built dataset artifacts/tables that `query_table(...)` can access.
- Optional silver bus parquet for fallback route presence checks.

## Practical Usage in TTC-PULSE
Use this page when you need:
- Fast narrative explanations of trends already visible in dashboard pages.
- Route-level troubleshooting prompts (for example, "Why is route 113 high risk?").
- Early directional prediction prompts (for 2026/2027) with clearly labeled assumptions.
- Exportable AI summaries for reporting or stakeholder communication.
