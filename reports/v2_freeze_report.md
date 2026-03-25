# V2 Freeze Report

## Freeze Summary
- Freeze name: `V2`
- Freeze intent: immutable rollback point before the UI/analytics realignment.
- Repository state was captured as a git tag and a filesystem snapshot.

## Snapshot Artifacts
- `releases/V2/`
- `releases/V2/manifest.sha256`
- `releases/V2/README_restore.md`

## Snapshot Contents
- Code snapshot of the current TTC Pulse app, src, docs, and configs tree.
- Analytical artifacts snapshot for raw, bronze, silver, dimensions, bridge, reviews, gold, outputs, and reports.
- Restore guide for filesystem and git rollback.

## Validation Notes
- The release snapshot is treated as read-only.
- The restore guide documents both file-copy recovery and git rollback via tag `v2`.
- The checksum manifest can be used to verify the snapshot integrity before restore.

## Rollback Guidance
1. Use `git checkout v2` for source rollback.
2. Copy folders from `releases/V2/` back into the project root for artifact rollback.
3. Verify integrity with `shasum -a 256 -c manifest.sha256` from inside `releases/V2/`.
