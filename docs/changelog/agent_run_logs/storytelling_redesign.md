# TTC Pulse v4 Storytelling Redesign Run Log

Date: 2026-03-23  
Owner: Orchestrator Agent (Codex)  
Scope: Storytelling-first, stakeholder-facing redesign of Streamlit dashboard.

---

## Agent Setup

The run is organized into these functional agents:

1. Orchestrator Agent: workflow owner, conflict resolver, architecture freeze authority.
2. Storytelling Research Agent: distilled storytelling principles from local project docs + external references.
3. Dashboard Audit Agent: audited current pages/charts/tables for keep/merge/demote/remove.
4. Story Architecture Agent: generated current story map, redesigned story map, storyboard, reduced page architecture.
5. Streamlit UX / Page Redesign Agent: implementation agent for new page structure and mode-based UX.
6. Analytics Safeguard Agent: guarded route-first/station-first logic, GTFS bridge role, live alert validation role, and composite pillar explainability.
7. Documentation Agent: updated dashboard docs + changelog.
8. QA / Regression Agent: validates story flow, functionality, and replacements for removed pages.

---

## Deliverable 1: Storytelling Design Memo for TTC Pulse v4

### Principle 1
- Principle: Start with the audience question, not the dataset.
- Why it matters here: Stakeholders/professors need to understand reliability patterns in <10 minutes.
- Streamlit pattern: every page opens with two lines: `Audience question` + `Takeaway`.
- Source: `{Local Materials}` (`docs/proposal_extracted.md`, `docs/proposal_alignment_notes.md`)

### Principle 2
- Principle: One page, one core message.
- Why it matters here: V2 spreads “where/when/why/how severe” across many disconnected panels.
- Streamlit pattern: one primary chart per page, supporting charts only if they strengthen the same message.
- Source: `{Local Materials}` (`docs/proposal_extracted.md`), `{SWD}` (five-lesson workshop framing)

### Principle 3
- Principle: Explanatory first, exploratory second.
- Why it matters here: Current app reads as analysis workspace; mission is a persuasive stakeholder narrative.
- Streamlit pattern: `Presentation` mode defaults to concise narrative; `Exploration` mode reveals deeper controls/tables.
- Source: `{SWD}` (focus on explanatory communication, not broad interactivity), `{Local Materials}` (`docs/proposal_alignment_notes.md`)

### Principle 4
- Principle: Eliminate clutter aggressively.
- Why it matters here: Too many controls/tables reduce signal and hide key findings.
- Streamlit pattern: keep one default filter set per page; move secondary controls/tables into expanders and exploration mode.
- Source: `{SWD}` (eliminate clutter, focus attention), `{HBS}` (clarity to drive action)

### Principle 5
- Principle: Always pair the “headline metric” with context pillars.
- Why it matters here: Composite alone can be misread; project requires explainability by frequency/severity/regularity/cause.
- Streamlit pattern: whenever composite is shown, tooltips/KPIs include all four component pillars.
- Source: `{Local Materials}` (`docs/proposal_extracted.md` composite policy)

### Principle 6
- Principle: Build explicit transitions between pages.
- Why it matters here: Proposal flow is linked (where -> when -> why -> validate); current transitions are weak.
- Streamlit pattern: each page ends with “Next question” that points to the next page in narrative order.
- Source: `{Local Materials}` (`docs/proposal_extracted.md` methodology flow), `{HBS}` (setting/conflict/resolution sequence)

### Principle 7
- Principle: Turn charts into decisions.
- Why it matters here: Audience should leave with operational implications, not only observed patterns.
- Streamlit pattern: add “What this implies” callout under primary chart (e.g., target windows/routes/stations).
- Source: `{HBS}` (insights + recommended action), `{Reddit Inspiration}` (go beyond KPI display to context and action)

### Principle 8
- Principle: Use narrative annotations to direct attention.
- Why it matters here: High-density reliability plots benefit from guided reading.
- Streamlit pattern: concise annotations on peaks/hotspots and top-cause segments.
- Source: `{SWD}` (focus attention), `{External Transit/Other}` (design-thinking narrative framing from Medium article)

### Principle 9
- Principle: Keep QA/methodology visible but demoted.
- Why it matters here: Evaluators may inspect validity, but primary audience journey should not start with QA internals.
- Streamlit pattern: move linkage/data-status details to a dedicated QA section/expander page.
- Source: `{Local Materials}` (`docs/proposal_alignment_notes.md`, `docs/dashboard/ux_decisions.md`)

### Principle 10
- Principle: Validate historical story with live evidence.
- Why it matters here: TTC Pulse claim includes “today vs historical hotspot alignment.”
- Streamlit pattern: explicit live-alert alignment page with match status + recent captures + caveat text.
- Source: `{Local Materials}` (`docs/proposal_extracted.md`, `reports/proposal_feature_suggestions.md`)

### Do / Don’t Checklist

Do:
- Lead every page with one stakeholder question and one takeaway.
- Keep primary visual prominent and readable first.
- Preserve route-first bus logic and station-first subway logic.
- Show composite with component pillars.
- Provide clear handoff to next page in story.

Don’t:
- Add pages that only restate existing evidence.
- Stack multiple unrelated charts on one page.
- Put QA/data contract content in the opening narrative path.
- Rely on large raw tables as primary communication.
- Treat live-alert capture as forecast.

### External Reference Access Note
- SWD workshops page was accessible and used.
- Medium article was accessible and used.
- Reddit access was partial; one thread (`r/BusinessIntelligence`) was reliably parseable, others were treated as light inspiration only.
- HBS article was available via cached search extract in tool results and used only for high-level principles.

---

## Deliverable 2: Storytelling Audit of Current Dashboard (V2)

### Page-level audit

| Current page | Question answered | Story alignment | Action | Reason |
|---|---|---|---|---|
| Linkage QA | Are linkage/match tiers healthy? | Low for main stakeholder story | Demote | Important for methodology, not main narrative start |
| Reliability Overview | What is overall trend by mode? | High | Keep (merge style upgrade) | Natural narrative opener |
| Bus Route Ranking | Which bus routes are worst? | High | Merge | Overlaps with subway ranking and spatial hotspot page |
| Subway Station Ranking | Which stations are worst? | High | Merge | Same pattern as bus ranking |
| Weekday Hour Heatmap | When disruptions recur in week/hour? | High | Merge | Should live with time-trend page |
| Monthly Trends | Long-horizon trend by entity | Medium-High | Merge | Better as supporting chart under “When” story |
| Cause Category Mix | Why disruptions happen? | High | Keep (tighten) | Core “why” page in proposal story |
| Live Alert Validation | Do live alerts align with pattern layer? | High | Keep (tighten + contextualize) | Required validation layer |
| Spatial Hotspot Map | Where disruptions cluster? | High | Merge | Should pair with rankings for “where it breaks” |
| Bus Drill-Down | Route-level deep explanation | High (exploration) | Merge into unified drilldown page | Keep depth while reducing page count |
| Subway Drill-Down | Station-level deep explanation | High (exploration) | Merge into unified drilldown page | Keep depth while reducing page count |

### Major chart/table audit highlights

- Keep:
  - Monthly reliability line trends, hotspot map, top-offender bars, weekday-hour heatmap, cause mix bars, live match status bars.
- Merge:
  - Bus + subway rankings into one mode-switched ranking section.
  - Heatmap + monthly trends into one “When it breaks” page.
  - Bus + subway drilldowns into one exploration page with tabs.
- Demote:
  - Linkage QA charts and wide status tables into QA/methodology section.
- Remove from main narrative:
  - Large raw tables that duplicate chart insight without adding interpretation.

### Transition blockers identified

- Weak journey from overview to “where/when/why.”
- Ranking pages disconnected from spatial hotspot context.
- Live validation not framed as “historical claim check.”
- Drilldown pages are powerful but isolated from narrative sequence.

---

## Deliverable 3: Current Story Map

`Shell/Status -> Overview -> Bus Rank -> Subway Rank -> Heatmap -> Monthly -> Cause Mix -> Live Validation -> Spatial -> Bus Drilldown -> Subway Drilldown`

Issue: sequence is feature-driven rather than argument-driven.

---

## Deliverable 4: Redesigned Story Map

`Landing Narrative -> 1) Overview (What is happening?) -> 2) Recurring Hotspots (Where does it break?) -> 3) Time Patterns (When does it break?) -> 4) Cause Signatures (Why does it break?) -> 5) Drilldown Explorer (How bad is it for a specific route/station?) -> 6) Live Alignment + QA (Does today match history, and is pipeline trustworthy?)`

---

## Deliverable 5: Reduced Page Architecture (Frozen)

Frozen by Orchestrator before implementation:

1. Story Overview
2. Recurring Hotspots (Rankings + Spatial)
3. Time Patterns
4. Cause Signatures
5. Drilldown Explorer (Bus + Subway tabs)
6. Live Alert Alignment + QA

Global UX contract:
- `Presentation` mode: stakeholder-first, low clutter, one takeaway/page.
- `Exploration` mode: deeper filters, extra charts/tables, technical details.

---

## Deliverable 6: Storyboard / Narrative Flow Table

| Order | Final page | Audience question | One-sentence takeaway | Primary chart | Supporting chart(s) | Why this page exists |
|---|---|---|---|---|---|---|
| 0 | Story Overview | What is TTC Pulse trying to prove? | Reliability is a recurring spatiotemporal pattern, not one KPI. | Multi-mode monthly reliability trend | KPI strip + concise journey cards | This page exists because it frames the full argument in <2 minutes. |
| 1 | Recurring Hotspots | Where are recurring reliability failures concentrated? | A small set of routes/stations repeatedly dominate risk scores. | Top-offender ranking bar chart | Spatial hotspot map (when available) | This page exists because it localizes risk to actionable entities. |
| 2 | Time Patterns | When are disruptions most likely to recur? | Recurrence concentrates in specific weekday-hour windows and seasonal periods. | Weekday-hour heatmap | Monthly trend line by selected metric | This page exists because timing is central to planning interventions. |
| 3 | Cause Signatures | Why do these disruptions happen? | Incident categories are uneven, and their mix shifts by period. | Top-N cause category bar | Monthly stacked cause composition | This page exists because cause mix links pattern to intervention type. |
| 4 | Drilldown Explorer | How severe/regular is one target route/station? | Route-first and station-first drilldowns expose detailed reliability profile. | Selected entity yearly trend | Monthly + weekday/time slices | This page exists because stakeholders need defensible deep dives before acting. |
| 5 | Live Alert Alignment + QA | Do live alerts align with historical hotspots, and can we trust the pipeline? | Live validation offers a real-time consistency check; QA is visible but secondary. | Match-status distribution | Recent alert table + QA/linkage expanders | This page exists because it closes the claim with present-time validation and trust controls. |

---

## Deliverable 7: Page Inventory Table (Current -> Final)

| Current page | Final page | Action | Reason |
|---|---|---|---|
| Linkage QA | Live Alert Alignment + QA | Merge/Demote | Keep QA accessible without interrupting story |
| Reliability Overview | Story Overview | Keep (redesign) | Strong narrative opener |
| Bus Route Ranking | Recurring Hotspots | Merge | Remove bus/subway duplication |
| Subway Station Ranking | Recurring Hotspots | Merge | Remove duplication |
| Spatial Hotspot Map | Recurring Hotspots | Merge | Keep “where” evidence with rankings |
| Weekday Hour Heatmap | Time Patterns | Merge | Core “when” evidence |
| Monthly Trends | Time Patterns | Merge | Supporting long-horizon context |
| Cause Category Mix | Cause Signatures | Keep (tighten) | Core “why” evidence |
| Bus Drill-Down | Drilldown Explorer | Merge | Maintain route-first deep dive in one page |
| Subway Drill-Down | Drilldown Explorer | Merge | Maintain station-first deep dive in one page |
| Live Alert Validation | Live Alert Alignment + QA | Keep (redesign) | Required historical-vs-live validation |

---

## Architecture Freeze Statement

The reduced page architecture above is frozen as the implementation contract for v4.  
No page-level coding should proceed outside this structure unless this freeze is explicitly revised.

---

## Deliverable 8: Implementation Changes (Streamlit Pages + Shared Components)

### Implemented page structure (active)
- `app/streamlit_app.py` updated to TTC Pulse v4 framing and 10-minute journey order.
- New active page set:
  - `app/pages/01_Story_Overview.py`
  - `app/pages/02_Recurring_Hotspots.py`
  - `app/pages/03_Time_Patterns.py`
  - `app/pages/04_Cause_Signatures.py`
  - `app/pages/05_Drill_Down_Explorer.py`
  - `app/pages/06_Live_Alert_Alignment.py`
  - `app/pages/07_QA_Methodology.py`
- Previous V3 page set moved to:
  - `app/pages_archive/v3_pre_v4/`

### Shared storytelling component
- Added shared UX helper:
  - `src/ttc_pulse/dashboard/storytelling.py`
- Provides:
  - page-level presentation/exploration mode selector,
  - standard `Audience Question` + `Takeaway` framing,
  - `Next Question` transition hints.

### Mode behavior contract (implemented)
- Presentation mode:
  - reduced controls,
  - fewer tables,
  - one key message per page.
- Exploration mode:
  - deeper filters and expanded evidence panels.

### Merge/removal outcomes in code
- Bus and subway ranking narratives merged into Recurring Hotspots.
- Weekday-hour and monthly trend narrative merged into Time Patterns.
- Bus + subway drill pathways merged into Drill-Down Explorer while preserving route-first/station-first logic.
- QA and methodology content demoted into final appendix page.

---

## Deliverable 9: Documentation Updates (Story Structure + UX Decisions)

Updated:
- `docs/dashboard/panel_descriptions.md`
- `docs/dashboard/ux_decisions.md`
- `docs/architecture/overview.md`
- `docs/changelog/CHANGELOG.md`
- `docs/changelog/agent_run_logs/storytelling_redesign.md` (this run log)

Additional scheduler/architecture alignment documentation (same run context):
- `docs/architecture/data_flow.md`
- `docs/decisions/design_decisions.md`
- `docs/pipelines/scheduler_ops.md`
- `docs/runbook.md`
- `docs/README.md`

---

## Deliverable 10: QA / Regression Summary

### Checks executed
1. Story architecture alignment
- Verified active sidebar page inventory matches frozen 7-page architecture.
- Verified V3 pages are archived under `app/pages_archive/v3_pre_v4/`.

2. UX contract checks
- Verified all active pages use shared storytelling framing and mode selector.
- Verified presentation/exploration mode controls are present page-wide.

3. Build/syntax checks
- `py_compile` passed for:
  - `app/streamlit_app.py`
  - all v4 page modules
  - `src/ttc_pulse/dashboard/storytelling.py`

4. Analytical integrity spot checks
- Route-first bus and station-first subway drill logic preserved in merged explorer workflow.
- Composite-context policy remains documented and enforced at UX contract level.

### QA result
- Status: Pass (code-level/syntax + structure + UX contract checks).

### Residual risk / caveat
- Manual visual walkthrough in a browser session is still recommended for final presentation polish and timing rehearsal, even though structural and syntax checks passed.
