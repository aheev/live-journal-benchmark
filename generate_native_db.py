#!/usr/bin/env python3
"""
One-time script to generate the persistent native-tables Ladybug database
for the LiveJournal benchmark.

Expects CSV files produced by generate_csv.py:
  nodes.csv  — one column: node id (no header)
  edges.csv  — two columns: src,dst (no header)

Usage:
    python3 generate_native_db.py [--db native_db/lj_native_db]
                                  [--nodes-csv native_db/nodes.csv]
                                  [--edges-csv native_db/edges.csv]

Set LBUG_C_API_LIB_PATH if the installed ladybug package has a broken .so symlink:
    export LBUG_C_API_LIB_PATH=/path/to/liblbug.so
"""

import argparse
import os
import sys
import time

BASE = os.path.dirname(os.path.abspath(__file__))

try:
    import ladybug as lbug
except ImportError:
    _api_path = os.environ.get("LADYBUG_PYTHON_API")
    if not _api_path:
        print(
            "ERROR: Could not import the 'ladybug' module.\n"
            "Install it, or set LADYBUG_PYTHON_API to the Ladybug Python API build directory.\n"
        )
        sys.exit(1)
    sys.path.insert(0, _api_path)
    try:
        import ladybug as lbug
    except ImportError:
        print(f"ERROR: Could not import 'ladybug' from LADYBUG_PYTHON_API={_api_path!r}")
        sys.exit(1)

DEFAULT_DB = os.path.join(BASE, "native_db", "lj_native_db")
DEFAULT_NODES_CSV = os.path.join(BASE, "native_db", "nodes.csv")
DEFAULT_EDGES_CSV = os.path.join(BASE, "native_db", "edges.csv")
ALGO_EXT = os.path.join(BASE, "libalgo.lbug_extension")

NODE_BATCH = 50_000      # nodes per UNWIND CREATE batch
MAX_DB_SIZE = 8 * 1024**3  # 8 GB — default 1 GB is too small for this dataset


def load_nodes(conn: lbug.Connection, nodes_csv: str) -> int:
    """Insert nodes via batched UNWIND+CREATE (no hash index required).
    Returns the number of nodes loaded."""
    node_ids = []
    with open(nodes_csv) as f:
        for line in f:
            line = line.strip()
            if line:
                node_ids.append(int(line))

    total = len(node_ids)
    t0 = time.perf_counter()
    inserted = 0
    for start in range(0, total, NODE_BATCH):
        batch = node_ids[start : start + NODE_BATCH]
        id_list = "[" + ",".join(str(i) for i in batch) + "]"
        conn.execute(f"UNWIND {id_list} AS id CREATE (:user {{id: id}})")
        inserted += len(batch)
        elapsed = time.perf_counter() - t0
        rate = inserted / elapsed if elapsed > 0 else 0
        eta = (total - inserted) / rate if rate > 0 else 0
        print(
            f"\r  {inserted:,}/{total:,} nodes  ({rate:,.0f}/s  ETA {eta:.0f}s)  ",
            end="",
            flush=True,
        )
    print(f"\r  {total:,} nodes loaded in {time.perf_counter() - t0:.1f}s" + " " * 20)
    return total


def generate(db_path: str, nodes_csv: str, edges_csv: str) -> None:
    if os.path.exists(db_path):
        print(f"ERROR: DB already exists at {db_path!r}.")
        print("Delete or rename it before re-generating.")
        sys.exit(1)

    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # ── Create DB and schema (hash index disabled for fair comparison) ────────
    print(f"Creating native-tables DB at {db_path!r} ...")
    db = lbug.Database(db_path, max_db_size=MAX_DB_SIZE)
    conn = lbug.Connection(db)

    # conn.execute("CALL enable_default_hash_index=false")
    conn.execute("CREATE NODE TABLE user(id INT32, PRIMARY KEY(id))")
    conn.execute("CREATE REL TABLE follows(FROM user TO user)")

    # ── Insert nodes via UNWIND batches (COPY FROM requires hash index) ───────
    print(f"Loading nodes from {nodes_csv} ...")
    num_nodes = load_nodes(conn, nodes_csv)

    # ── Bulk-load edges via COPY FROM (rel tables work without hash index) ────
    print(f"Loading edges from {edges_csv} ...")
    t0 = time.perf_counter()
    conn.execute(f"COPY follows FROM '{edges_csv}' (header=false)")
    print(f"  Done in {time.perf_counter() - t0:.1f}s")

    # ── Algo extension + projected graph ──────────────────────────────────────
    print("Loading algo extension and creating projected graph ...")
    if os.path.exists(ALGO_EXT):
        try:
            conn.execute(f"LOAD EXTENSION '{ALGO_EXT}'")
        except Exception:
            # Fall back to registry if local binary is incompatible
            conn.execute("INSTALL algo")
            conn.execute("LOAD EXTENSION algo")
    else:
        conn.execute("INSTALL algo")
        conn.execute("LOAD EXTENSION algo")
    conn.execute("CALL project_graph('lj', ['user'], ['follows'])")
    print("  Projected graph 'lj' created.")

    print(f"\nDone. Persistent DB written to: {db_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Load CSV files into a native-tables Ladybug DB.")
    parser.add_argument("--db", default=DEFAULT_DB, help=f"Output DB path (default: {DEFAULT_DB})")
    parser.add_argument("--nodes-csv", default=DEFAULT_NODES_CSV, help=f"Nodes CSV file (default: {DEFAULT_NODES_CSV})")
    parser.add_argument("--edges-csv", default=DEFAULT_EDGES_CSV, help=f"Edges CSV file (default: {DEFAULT_EDGES_CSV})")
    args = parser.parse_args()

    for label, path in [("nodes CSV", args.nodes_csv), ("edges CSV", args.edges_csv)]:
        if not os.path.exists(path):
            print(f"ERROR: {label} not found: {path!r}")
            print("Run generate_csv.py first.")
            sys.exit(1)

    generate(args.db, args.nodes_csv, args.edges_csv)


if __name__ == "__main__":
    main()

