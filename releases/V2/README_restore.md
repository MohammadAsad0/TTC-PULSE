# TTC Pulse V2 Restore Guide

This folder is an immutable snapshot of TTC Pulse before the V2 UI/analytics realignment changes.

## Restore (filesystem)
1. From project root, back up current working folders if needed.
2. Copy folders from `releases/V2/` back into project root.

## Restore (git)
- Use tag `v2` to return repository source state:
  - `git checkout v2`
  - or create rollback branch: `git checkout -b rollback/v2 v2`

## Integrity
- Verify checksums:
  - `cd releases/V2`
  - `shasum -a 256 -c manifest.sha256`
