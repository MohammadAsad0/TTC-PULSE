# Wave 0 Release Freeze Report

## Snapshot Summary

- Frozen artifact set: `187` files copied into `ttc_pulse/releases/V1/`
- Generated documentation files: `2`
- Total files in the final `releases/V1` tree: `189`

## Artifacts Captured

- `app/`
- `src/`
- `docs/`
- `configs/`
- `requirements.txt`
- `data/ttc_pulse.duckdb`
- `gold/`
- `dimensions/`
- `bridge/`
- `reviews/`
- `silver/`

## Manifest

- Integrity manifest: `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/releases/V1/manifest.sha256`

## Restore Steps

1. Verify the frozen tree with `shasum -a 256 -c manifest.sha256` from `ttc_pulse/releases/V1`.
2. Restore the snapshot into the working `ttc_pulse` directory with the `rsync` and `cp` commands in `ttc_pulse/releases/V1/README_restore.md`.
3. Re-run verification after restore if you need to confirm the copied files match the frozen snapshot.
