#!/usr/bin/env python3
"""
Parse com-LiveJournal.mtx and write two CSV files consumed by generate_native_db.py:

  native_db/nodes.csv  — one column: node id (no header)
  native_db/edges.csv  — two columns: src,dst  (symmetrized, self-loops dropped, no header)

Usage:
    python3 generate_csv.py [--mtx com-LiveJournal.mtx] [--out-dir native_db]
"""

import argparse
import os
import sys
import time

BASE = os.path.dirname(os.path.abspath(__file__))

DEFAULT_MTX = os.path.join(BASE, "com-LiveJournal.mtx")
DEFAULT_OUT = os.path.join(BASE, "native_db")


def generate_csv(mtx_path: str, nodes_csv: str, edges_csv: str) -> None:
    print(f"Parsing {mtx_path} ...")
    t0 = time.perf_counter()

    node_ids: set[int] = set()
    edge_count = 0

    with open(mtx_path) as f_in, open(edges_csv, "w") as f_edges:
        for line in f_in:
            if line.startswith("%"):
                continue
            parts = line.split()
            if len(parts) == 3 and parts[0] == parts[1]:
                # Dimension line: rows cols nnz — skip
                continue
            u, v = int(parts[0]), int(parts[1])
            node_ids.add(u)
            node_ids.add(v)
            if u != v:
                f_edges.write(f"{u},{v}\n")
                f_edges.write(f"{v},{u}\n")
                edge_count += 2

    with open(nodes_csv, "w") as f_nodes:
        for nid in sorted(node_ids):
            f_nodes.write(f"{nid}\n")

    elapsed = time.perf_counter() - t0
    print(f"  {len(node_ids):,} nodes  →  {nodes_csv}")
    print(f"  {edge_count:,} directed edges  →  {edges_csv}")
    print(f"  Done in {elapsed:.1f}s")


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert LiveJournal MTX to CSV files for generate_native_db.py.")
    parser.add_argument("--mtx", default=DEFAULT_MTX, help=f"Input MTX file (default: {DEFAULT_MTX})")
    parser.add_argument("--out-dir", default=DEFAULT_OUT, help=f"Output directory for CSV files (default: {DEFAULT_OUT})")
    args = parser.parse_args()

    if not os.path.exists(args.mtx):
        print(f"ERROR: MTX file not found: {args.mtx!r}")
        sys.exit(1)

    os.makedirs(args.out_dir, exist_ok=True)
    nodes_csv = os.path.join(args.out_dir, "nodes.csv")
    edges_csv = os.path.join(args.out_dir, "edges.csv")

    generate_csv(args.mtx, nodes_csv, edges_csv)


if __name__ == "__main__":
    main()
