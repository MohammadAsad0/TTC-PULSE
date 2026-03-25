# V1 Restore Instructions

This directory is the frozen Wave 0 V1 snapshot.

## Verify integrity

```sh
cd /Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/releases/V1
shasum -a 256 -c manifest.sha256
```

## Restore the snapshot

Restore the frozen tree back into the working `ttc_pulse` directory with:

```sh
cd /Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse
rsync -a releases/V1/app/ app/
rsync -a releases/V1/src/ src/
rsync -a releases/V1/docs/ docs/
rsync -a releases/V1/configs/ configs/
rsync -a releases/V1/data/ data/
rsync -a releases/V1/gold/ gold/
rsync -a releases/V1/dimensions/ dimensions/
rsync -a releases/V1/bridge/ bridge/
rsync -a releases/V1/reviews/ reviews/
rsync -a releases/V1/silver/ silver/
cp -a releases/V1/requirements.txt requirements.txt
cp -a releases/V1/data/ttc_pulse.duckdb data/ttc_pulse.duckdb
```

The `rsync` steps restore the frozen directories. The `cp` steps restore the root requirements file and the DuckDB snapshot explicitly.
