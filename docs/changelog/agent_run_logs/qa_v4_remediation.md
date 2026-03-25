# QA v4 Remediation Run Log (2026-03-23)

## Scope
- Convert QA security findings into course-appropriate remediation guidance.
- Remove full-auth implementation requirement from current sprint scope.
- Explicitly document security posture by deployment mode.

## Triage Outcome
- Critical finding (from automated QA): missing authentication.
- Project decision: full in-app authentication is deferred for course-scope MVP.
- Mitigation for current scope: strict deployment-mode policy and controlled exposure rules.

## Implemented Documentation Changes
- Added deployment security modes in `docs/qa_and_review/known_caveats.md`:
  - local development mode
  - shared/demo mode
  - public deployment mode
- Added `DD-018` in `docs/decisions/design_decisions.md` to formalize auth deferral and deployment-context controls.
- Updated `docs/changelog/CHANGELOG.md` to record the security posture decision.
- Added `reports/qa_remediation_report.md` with triage memo and remediation status.

## Auth Code Removal Check
- Repo scan found no active in-app auth gate in `app/` or `src/` runtime paths.
- No auth runtime files were present to delete in the current v4 code path.

## Deferred Items
- Full in-app authentication / RBAC remains deferred until non-course/public deployment requirements are approved.
