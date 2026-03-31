# Dashboard Revision Regression

Run date: 2026-03-18
Owner: Worker A
Scope: regression artifacts only

## Checks Executed
- `python3 -m py_compile` on `app/streamlit_app.py`, all files under `app/pages/*.py`, and `src/ttc_pulse/dashboard/*.py`
- grep-based contract check against active shell/pages for removed proposal-misaligned strings
- archive-state check for `Linkage QA`

## Results
- `py_compile`: pass
- Removed-string check on active UI files: pass
- Archived page state: pass

## Details
- The active shell and active `app/pages/*.py` files no longer contain the removed stakeholder-facing strings:
  - `Gold table`
  - `Gold tables`
  - `Linkage QA`
  - `Step 4 runtime shell`
  - `Historical service-date coverage window`
  - `snapshot_ts records the alert capture timestamp`
- `app/pages_archive/01_Linkage_QA.py` exists by design as an archive copy.
- `app/pages/01_Linkage_QA.py` is absent from active navigation.
- The grep check still matched `Gold table` in `src/ttc_pulse/dashboard/loaders.py`, but only inside internal helper docstrings/comments. That does not affect the active UI surface.

## Caveats
- This is a regression check, not an end-to-end browser test.
- The archived Linkage QA page intentionally preserves old text for rollback/traceability.

## Final Status
- V2 dashboard realignment is clean on syntax and active UI text contracts.
- No blocking regression was found in the active shell/pages scope.

## Post-Merge Recheck (Orchestrator)
- Re-ran `py_compile` after final page merges (`07_Cause_Category_Mix`, `08_Live_Alert_Validation`, `09_Spatial_Hotspot_Map` and related pages): pass.
- Re-ran active UI grep contract check for removed strings: pass.
