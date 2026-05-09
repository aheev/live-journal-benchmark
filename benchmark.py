#!/usr/bin/env python3
"""
Benchmark: icebug-disk vs parquet storage for the LiveJournal graph.

Usage:
    python3 benchmark.py [--runs N] [--warmup N] [--backends icebug,parquet]
"""

import argparse
import csv
import os
import shutil
import sys
import tempfile
import time

sys.path.insert(0, "/media/aheev/secondary/open-source/ladybug/ladybug/tools/python_api/build")
import ladybug as lbug

from queries import QUERIES

BASE = os.path.dirname(os.path.abspath(__file__))
ICEBUG_SCHEMA = os.path.join(BASE, "icebug_disk", "schema.cypher")
PARQUET_SCHEMA = os.path.join(BASE, "parquet_db", "schema.cypher")


def setup_db(schema_path: str, db_path: str) -> lbug.Connection:
    db = lbug.Database(db_path)
    conn = lbug.Connection(db)
    with open(schema_path) as f:
        schema = f.read()
    for stmt in schema.strip().split(";"):
        s = stmt.strip()
        if s:
            conn.execute(s)
    return conn


def run_query(conn: lbug.Connection, cypher: str) -> tuple[float, object]:
    t0 = time.perf_counter()
    result = conn.execute(cypher)
    rows = []
    while result.has_next():
        rows.append(result.get_next())
    elapsed = time.perf_counter() - t0
    return elapsed, rows


def benchmark(conn: lbug.Connection, backend: str, warmup: int, runs: int) -> list[dict]:
    records = []
    for q in QUERIES:
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
        print("\nSpeedup (parquet / icebug, >1 means icebug is faster):")
        for qid, vals in by_query.items():
            a = vals.get("icebug_avg_ms", float("nan"))
            p = vals.get("parquet_avg_ms", float("nan"))
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
        help="Comma-separated backends to run (default: icebug,parquet)",
    )
    parser.add_argument("--output", default="results.csv", help="CSV output file")
    args = parser.parse_args()

    backends = [b.strip() for b in args.backends.split(",")]

    all_records: list[dict] = []

    for backend in backends:
        db_path = tempfile.mktemp(prefix=f"lj_bench_{backend}_", suffix=".db")
        try:
            print(f"\n{'='*60}")
            print(f"Setting up backend: {backend}")
            schema = ICEBUG_SCHEMA if backend == "icebug" else PARQUET_SCHEMA
            conn = setup_db(schema, db_path)
            print(f"Running {args.warmup} warmup + {args.runs} timed runs per query...")
            records = benchmark(conn, backend, args.warmup, args.runs)
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
