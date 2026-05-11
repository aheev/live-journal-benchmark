#!/usr/bin/env python3
"""
Benchmark: icebug-disk vs parquet vs native-tables storage for the LiveJournal graph.

Usage:
    python3 benchmark.py [--runs N] [--warmup N] [--backends icebug,parquet,native]
                         [--queries SPEC]

SPEC examples:
    --queries q01,q03           specific query IDs (or short prefixes)
    --queries q01-q05           range q01 through q05 (inclusive)
    --queries 1-5,q10           numeric range plus extra query
    --queries q03_outdeg_high   exact query id

For the native backend, run generate_native_db.py first to create the persistent DB.
"""

import argparse
import csv
import os
import re
import shutil
import sys
import tempfile
import time

BASE = os.path.dirname(os.path.abspath(__file__))

try:
    import ladybug as lbug
except ImportError:
    _api_path = os.environ.get("LADYBUG_PYTHON_API")
    if not _api_path:
        print(
            "ERROR: Could not import the 'ladybug' module.\n"
            "Install it, or set LADYBUG_PYTHON_API to the Ladybug Python API build directory:\n\n"
            "  export LADYBUG_PYTHON_API=/path/to/ladybug/tools/python_api/build\n"
        )
        sys.exit(1)
    sys.path.insert(0, _api_path)
    try:
        import ladybug as lbug
    except ImportError:
        print(f"ERROR: Could not import 'ladybug' from LADYBUG_PYTHON_API={_api_path!r}")
        sys.exit(1)

from queries import QUERIES

ICEBUG_SCHEMA = os.path.join(BASE, "icebug_disk", "schema.cypher")
PARQUET_SCHEMA = os.path.join(BASE, "parquet_db", "schema.cypher")

# Storage paths are computed relative to the repo root so the benchmark works
# regardless of where it is cloned.
ICEBUG_STORAGE = os.path.join(BASE, "icebug_disk")
PARQUET_STORAGE = os.path.join(BASE, "parquet_db", "lj")
NATIVE_DB = os.path.join(BASE, "native_db", "lj_native_db")
ALGO_EXT = os.path.join(BASE, "libalgo.lbug_extension")

_VALID_BACKENDS = {"icebug", "parquet", "native"}
_BACKEND_SCHEMA = {"icebug": ICEBUG_SCHEMA, "parquet": PARQUET_SCHEMA}
_BACKEND_STORAGE = {"icebug": ICEBUG_STORAGE, "parquet": PARQUET_STORAGE}
MAX_DB_SIZE = 8 * 1024**3  # 20 GB — default 1 GB is too small for this dataset


def setup_db(schema_path: str, db_path: str, storage_path: str) -> lbug.Connection:
    db = lbug.Database(db_path, max_db_size=MAX_DB_SIZE)
    conn = lbug.Connection(db)
    with open(schema_path) as f:
        schema = f.read().replace("__STORAGE_PATH__", storage_path)
    for stmt in schema.strip().split(";"):
        s = stmt.strip()
        if not s:
            continue
        try:
            conn.execute(s)
        except Exception as e:
            # Non-fatal: extension installation may fail if already installed
            # or if the extension server version doesn't match. Log and continue.
            print(f"  SETUP WARNING: {e!s:.120}")
    return conn


def setup_native_conn() -> lbug.Connection:
    """Open the pre-generated persistent native-tables DB (no hash index)."""
    if not os.path.exists(NATIVE_DB):
        print(
            f"ERROR: Native DB not found at {NATIVE_DB!r}.\n"
            "Run generate_native_db.py first to create it."
        )
        sys.exit(1)
    conn = lbug.Connection(lbug.Database(NATIVE_DB, max_db_size=MAX_DB_SIZE))
    # Reload algo extension (extensions are not persisted across connections).
    if os.path.exists(ALGO_EXT):
        conn.execute(f"LOAD EXTENSION '{ALGO_EXT}'")
    else:
        try:
            conn.execute("INSTALL algo")
        except Exception:
            pass
        conn.execute("LOAD EXTENSION algo")
    # Re-create projected graph for this connection (projected graphs are connection-scoped).
    try:
        conn.execute("CALL project_graph('lj', ['user'], ['follows'])")
    except Exception:
        pass
    return conn


def _query_num(qid: str) -> int | None:
    """Return the numeric prefix of a query id like 'q03_foo' -> 3."""
    m = re.match(r"q(\d+)", qid)
    return int(m.group(1)) if m else None


def filter_queries(spec: str, all_queries: list[dict]) -> list[dict]:
    """Filter QUERIES by a comma-separated spec of IDs, short prefixes, or numeric ranges.

    Supported formats (combinable with commas):
      q01                 short prefix — matches 'q01_count_nodes'
      q01_count_nodes     exact query id
      q01-q05             range by query prefix (inclusive)
      1-5                 range by number (inclusive)

    Examples:
      --queries q01,q03
      --queries q01-q05
      --queries 1-5,q10
      --queries q03_outdeg_high,q07-q10
    """
    all_ids = [q["id"] for q in all_queries]
    selected: list[dict] = []
    seen: set[str] = set()

    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue

        # Range: "q01-q05" or "1-5" or "q1-5" etc.
        range_match = re.fullmatch(r"q?(\d+)-q?(\d+)", part)
        if range_match:
            lo, hi = int(range_match.group(1)), int(range_match.group(2))
            for q in all_queries:
                n = _query_num(q["id"])
                if n is not None and lo <= n <= hi and q["id"] not in seen:
                    selected.append(q)
                    seen.add(q["id"])
            continue

        # Exact match first, then short-prefix match (e.g. "q03" -> "q03_outdeg_high")
        exact = [q for q in all_queries if q["id"] == part]
        prefix = [q for q in all_queries if q["id"].startswith(part + "_")] if not exact else []
        matched = exact or prefix

        if not matched:
            raise ValueError(
                f"--queries: no query matches {part!r}.\n"
                f"Available query IDs:\n  " + "\n  ".join(all_ids)
            )
        for q in matched:
            if q["id"] not in seen:
                selected.append(q)
                seen.add(q["id"])

    return selected


def run_query(conn: lbug.Connection, cypher: str) -> tuple[float, object]:
    t0 = time.perf_counter()
    result = conn.execute(cypher)
    rows = []
    while result.has_next():
        rows.append(result.get_next())
    elapsed = time.perf_counter() - t0
    return elapsed, rows


def benchmark(conn: lbug.Connection, backend: str, warmup: int, runs: int, queries: list[dict]) -> list[dict]:
    records = []
    for q in queries:
        print(f"  [{backend}] {q['id']}: {q['description']}")

        # Warmup
        for _ in range(warmup):
            try:
                run_query(conn, q["cypher"])
            except Exception as e:
                print(f"    WARMUP ERROR: {e}")
                break

        # Timed runs
        times = []
        result_val = None
        for i in range(runs):
            try:
                elapsed, rows = run_query(conn, q["cypher"])
                times.append(elapsed)
                if i == 0:
                    result_val = rows[0] if rows else None
            except Exception as e:
                print(f"    RUN ERROR: {e}")
                times.append(float("nan"))

        if times:
            avg = sum(t for t in times if t == t) / sum(1 for t in times if t == t)
            mn = min(t for t in times if t == t)
            mx = max(t for t in times if t == t)
        else:
            avg = mn = mx = float("nan")

        print(f"    result={result_val}  avg={avg*1000:.1f}ms  min={mn*1000:.1f}ms  max={mx*1000:.1f}ms")
        records.append(
            {
                "backend": backend,
                "query_id": q["id"],
                "description": q["description"],
                "avg_ms": round(avg * 1000, 2),
                "min_ms": round(mn * 1000, 2),
                "max_ms": round(mx * 1000, 2),
                "result": str(result_val),
            }
        )
    return records


def print_table(records: list[dict]) -> None:
    from itertools import groupby

    by_query: dict[str, dict] = {}
    for r in records:
        qid = r["query_id"]
        if qid not in by_query:
            by_query[qid] = {"description": r["description"]}
        by_query[qid][r["backend"] + "_avg_ms"] = r["avg_ms"]
        by_query[qid][r["backend"] + "_min_ms"] = r["min_ms"]

    backends = list(dict.fromkeys(r["backend"] for r in records))

    header = f"{'Query':<30} {'Description':<45}"
    for b in backends:
        header += f"  {b:>12} avg(ms)"
    print("\n" + "=" * len(header))
    print(header)
    print("=" * len(header))

    for qid, vals in by_query.items():
        row = f"{qid:<30} {vals['description'][:44]:<45}"
        for b in backends:
            avg = vals.get(b + "_avg_ms", float("nan"))
            row += f"  {avg:>18.1f}"
        print(row)

    if len(backends) == 2:
        b0, b1 = backends[0], backends[1]
        print(f"\nSpeedup ({b1} / {b0}, >1 means {b0} is faster):")
        for qid, vals in by_query.items():
            a = vals.get(f"{b0}_avg_ms", float("nan"))
            p = vals.get(f"{b1}_avg_ms", float("nan"))
            if a and p and a == a and p == p and a > 0:
                ratio = p / a
                print(f"  {qid:<30}  {ratio:.2f}x")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=5, help="Timed runs per query (default: 5)")
    parser.add_argument("--warmup", type=int, default=1, help="Warmup runs per query (default: 1)")
    parser.add_argument(
        "--backends",
        default="icebug,parquet",
        help="Comma-separated backends to run (default: icebug,parquet; also supports: native)",
    )
    parser.add_argument(
        "--queries",
        default=None,
        metavar="SPEC",
        help=(
            "Queries to run. Comma-separated IDs, short prefixes, or numeric ranges (inclusive). "
            "Examples: --queries q01,q03  --queries q01-q05  --queries 1-5,q10  "
            "(default: all queries)"
        ),
    )
    parser.add_argument("--output", default="results.csv", help="CSV output file")
    args = parser.parse_args()

    backends = [b.strip() for b in args.backends.split(",")]
    unknown = [b for b in backends if b not in _VALID_BACKENDS]
    if unknown:
        parser.error(f"Unknown backend(s): {', '.join(unknown)!r}. Valid options: {', '.join(sorted(_VALID_BACKENDS))}")

    try:
        queries = filter_queries(args.queries, QUERIES) if args.queries else QUERIES
    except ValueError as e:
        parser.error(str(e))

    if not queries:
        parser.error("--queries spec matched no queries.")

    all_records: list[dict] = []

    for backend in backends:
        print(f"\n{'='*60}")
        print(f"Setting up backend: {backend}")
        if backend == "native":
            conn = setup_native_conn()
            print(f"Running {args.warmup} warmup + {args.runs} timed runs per query...")
            records = benchmark(conn, backend, args.warmup, args.runs, queries)
            all_records.extend(records)
        else:
            db_path = tempfile.mktemp(prefix=f"lj_bench_{backend}_", suffix=".db")
            try:
                conn = setup_db(_BACKEND_SCHEMA[backend], db_path, _BACKEND_STORAGE[backend])
                print(f"Running {args.warmup} warmup + {args.runs} timed runs per query...")
                records = benchmark(conn, backend, args.warmup, args.runs, queries)
                all_records.extend(records)
            finally:
                shutil.rmtree(db_path, ignore_errors=True)

    # Write CSV
    out = os.path.join(BASE, args.output)
    with open(out, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["backend", "query_id", "description", "avg_ms", "min_ms", "max_ms", "result"]
        )
        writer.writeheader()
        writer.writerows(all_records)
    print(f"\nResults written to: {out}")

    print_table(all_records)


if __name__ == "__main__":
    main()
