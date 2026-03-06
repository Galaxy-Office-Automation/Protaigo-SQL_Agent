#!/usr/bin/env python3
"""
Run & Verify Optimized Query
=============================
Takes an already-optimized query, runs it against the database,
checks it finishes under 10 seconds, and shows the output.

Usage:
  python tests/run_query.py --file optimized_query.sql
  python tests/run_query.py --sql "SELECT ..."
  python tests/run_query.py                        # interactive paste mode
"""

import sys
import time
import argparse
import psycopg2
from datetime import datetime

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "app_db",
    "user": "app_user",
    "password": "StrongPassword123!"
}


def run_query(query, timeout_sec):
    conn = psycopg2.connect(**DB_CONFIG, connect_timeout=5)
    cur = conn.cursor()
    cur.execute(f"SET statement_timeout = '{timeout_sec * 1000}'")
    start = time.time()
    cur.execute(query)
    rows = cur.fetchall()
    elapsed = time.time() - start
    col_names = [d[0] for d in cur.description] if cur.description else []
    cur.close()
    conn.close()
    return elapsed, rows, col_names


def print_table(rows, col_names, max_rows=20):
    if not rows or not col_names:
        print("\n  (0 rows returned)")
        return

    show_cols = min(len(col_names), 8)
    headers = col_names[:show_cols]
    display = rows[:max_rows]

    def fmt(v):
        s = str(v) if v is not None else "NULL"
        return s[:22] + "…" if len(s) > 22 else s

    widths = [max(len(h), max(len(fmt(r[i])) if i < len(r) else 0 for r in display))
              for i, h in enumerate(headers)]
    widths = [min(w, 22) for w in widths]

    hdr = " | ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    sep = "-+-".join("-" * w for w in widths)
    print(f"\n  {hdr}")
    print(f"  {sep}")
    for row in display:
        vals = " | ".join(fmt(row[i]).ljust(widths[i]) if i < len(row) else " " * widths[i]
                          for i in range(show_cols))
        print(f"  {vals}")

    if len(rows) > max_rows:
        print(f"  ... {len(rows) - max_rows} more rows")
    if len(col_names) > show_cols:
        print(f"  (+ {len(col_names) - show_cols} more columns)")


def main():
    parser = argparse.ArgumentParser(description="Run and verify an optimized SQL query")
    parser.add_argument("--file", "-f", help="Path to a .sql file")
    parser.add_argument("--sql", "-s", help="Inline SQL string")
    parser.add_argument("--timeout", "-t", type=int, default=10,
                        help="Max seconds allowed (default: 10)")
    parser.add_argument("--save", action="store_true",
                        help="Save report to tests/run_query_report.txt")
    args = parser.parse_args()

    # ── Get query ───────────────────────────────────────
    if args.file:
        with open(args.file) as f:
            query = f.read()
        source = args.file
    elif args.sql:
        query = args.sql
        source = "inline"
    else:
        print("=" * 55)
        print("  Paste your optimized SQL query below.")
        print("  Type END on a new line when done.")
        print("=" * 55)
        lines = []
        try:
            for line in sys.stdin:
                if line.strip().upper() == "END":
                    break
                lines.append(line)
        except EOFError:
            pass
        query = "".join(lines)
        source = "stdin"

    if not query.strip():
        print("Error: no query provided.")
        sys.exit(1)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    report = []

    def log(msg=""):
        print(msg)
        report.append(msg)

    log("=" * 55)
    log("  OPTIMIZED QUERY — EXECUTION TEST")
    log("=" * 55)
    log(f"  Date      : {now}")
    log(f"  Source    : {source}")
    log(f"  Threshold : under {args.timeout}s, no errors")
    log("=" * 55)

    # ── Execute ─────────────────────────────────────────
    try:
        elapsed, rows, col_names = run_query(query, args.timeout + 5)
        error = None
    except Exception as e:
        elapsed, rows, col_names = None, [], []
        error = str(e)

    # ── Output ──────────────────────────────────────────
    log("")
    if error:
        log(f"  ✗ ERROR: {error}")
    else:
        log(f"  Execution time : {elapsed:.3f}s")
        log(f"  Rows returned  : {len(rows)}")
        print_table(rows, col_names)

    # ── Verdict ─────────────────────────────────────────
    log("")
    log("-" * 55)
    if error:
        log("  ✗ No errors    : FAIL")
        log("  ✗ Under 10s    : FAIL")
        passed = False
    else:
        no_err = True
        fast = elapsed < args.timeout
        log(f"  {'✓' if no_err else '✗'} No errors    : PASS")
        log(f"  {'✓' if fast else '✗'} Under {args.timeout}s    : {'PASS' if fast else 'FAIL'}"
            f"  ({elapsed:.3f}s)")
        passed = no_err and fast
    log("-" * 55)
    log("")
    if passed:
        log("  ✅ PASSED")
    else:
        log("  ❌ FAILED")
    log("")

    if args.save:
        import os
        path = os.path.join(os.path.dirname(__file__), "run_query_report.txt")
        with open(path, "w") as f:
            f.write("\n".join(report))
        log(f"  Report saved to: {path}")

    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
