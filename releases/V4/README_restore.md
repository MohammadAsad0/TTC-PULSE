# TTC Pulse V4 Restore Guide

This folder is a local snapshot marker for the TTC Pulse V4 state.

## Restore (git)
- The V4 snapshot is recorded in git branch `v4`.
- To restore the code state:
  - `git checkout v4`
  - or create a rollback branch: `git checkout -b rollback/v4 v4`

## Snapshot commit
- `7c2e40b` - `chore: refresh live alert archive`

## Notes
- This release marker tracks the current V4 workspace state and the live-alert archive refresh commit.
- If you need a full file-tree snapshot like V1/V2, we can materialize one next.
