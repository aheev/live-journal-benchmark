# LiveJournal Benchmark (Ladybug)

This repository benchmarks two storage backends for the same LiveJournal graph workload:

- `icebug-disk`
- `parquet` (Parquet-backed storage)

The benchmark executes a fixed set of Cypher queries and reports latency statistics per query/backend.

## What This Benchmark Measures

- Query latency (average/min/max) for each query in `queries.py`
- Relative speedup between backends (printed as `parquet / icebug`)
- Result sanity for the first timed run (captured as a string in CSV)

The query set includes:

- Global counts (nodes/edges)
- Degree lookups for high/medium/low degree nodes
- Top-k degree query
- Distinct source count
- Full edge scans

## Repository Layout

- `benchmark.py`: main runner and report generation
- `queries.py`: benchmark query definitions
- `icebug_disk/schema.cypher`: schema using `icebug-disk` storage
- `parquet_db/schema.cypher`: schema using parquet storage
- `.gitignore`: local ignores (`__pycache__`, `*.parquet`, `*.csv`)

## Prerequisites

1. Python 3.10+ (or any recent Python 3 version with typing support used here).
2. A built Ladybug Python API module (`ladybug`) available at the path hardcoded in `benchmark.py`:

	`/media/aheev/secondary/open-source/ladybug/ladybug/tools/python_api/build`

3. LiveJournal data available where the schema files point:

	- `icebug_disk/schema.cypher` uses:
	  `/media/aheev/secondary/open-source/ladybug/benchmarks/live-journal-benchmark/icebug_disk`
	- `parquet_db/schema.cypher` uses:
	  `/media/aheev/secondary/open-source/ladybug/benchmarks/live-journal-benchmark/parquet_db/lj`

If you move this repo, update paths in:

- `benchmark.py` (`sys.path.insert(...)`)
- both `schema.cypher` files (`WITH (storage = '...')`)

## Usage

Run both backends with defaults:

```bash
python3 benchmark.py
```

Options:

- `--runs N`: timed runs per query (default `5`)
- `--warmup N`: warmup runs per query (default `1`)
- `--backends icebug,parquet`: comma-separated backends to run
- `--output results.csv`: CSV filename (written in repo root)

Examples:

```bash
# Faster smoke run
python3 benchmark.py --runs 2 --warmup 1

# Only one backend
python3 benchmark.py --backends icebug

# Custom output file
python3 benchmark.py --output run_2026_05_09.csv
```

## Output

The runner produces:

1. Console output per query/backend:
	- first observed result
	- `avg`, `min`, `max` in milliseconds
2. A CSV file with columns:
	- `backend`
	- `query_id`
	- `description`
	- `avg_ms`
	- `min_ms`
	- `max_ms`
	- `result`
3. A summary table and speedup section:
	- Speedup is shown as `parquet / icebug`
	- Value `> 1` means `icebug` is faster

## Notes and Caveats

- The benchmark creates a temporary DB path per backend run and removes it afterward.
- Query execution currently materializes all rows in Python before timing ends.
- The query file notes an engine issue for multi-row streaming; queries here return a single aggregate row for fairness.
- If a query fails during timed runs, that run is recorded as `NaN` and excluded from avg/min/max calculations.

## Repro Tips

- Keep the same dataset snapshots for both backends.
- Run on an otherwise idle machine for more stable latency numbers.
- Use multiple runs and compare medians/averages across repeated benchmark invocations.
