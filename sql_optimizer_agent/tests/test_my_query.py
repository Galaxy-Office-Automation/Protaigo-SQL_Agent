#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════
  SQL Optimizer Agent — Interactive Query Tester
═══════════════════════════════════════════════════════════════

Usage:
  # 1. Pass a .sql file
  python tests/test_my_query.py  --file  my_query.sql

  # 2. Pass inline SQL
  python tests/test_my_query.py  --sql  "SELECT * FROM pgbench_accounts LIMIT 10"

  # 3. Interactive mode (paste query, end with Ctrl+D or a line containing just 'END')
  python tests/test_my_query.py

Options:
  --timeout N     Max seconds for execution (default: 10)
  --skip-original Skip running the original query (useful if it's known to be very slow)
  --show-diff     Show a side-by-side diff of original vs optimized
  --save          Save report to tests/my_query_report.txt
"""

import sys
import os
import re
import time
import argparse
import textwrap
import psycopg2
from datetime import datetime

# ── Setup path ──────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from agent.orchestrator import AgentOrchestrator
from validator.syntax_validator import SyntaxValidator

# ── DB config (matches .env) ───────────────────────────────
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "app_db",
    "user": "app_user",
    "password": "StrongPassword123!"
}


# ═══════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════

def execute_query(query: str, timeout_sec: int = 10):
    """Run a query and return (elapsed_seconds, row_count, error_string|None)."""
    try:
        conn = psycopg2.connect(**DB_CONFIG, connect_timeout=5)
        cur = conn.cursor()
        cur.execute(f"SET statement_timeout = '{timeout_sec * 1000}'")
        t0 = time.time()
        cur.execute(query)
        rows = cur.fetchall()
        elapsed = time.time() - t0
        col_names = [desc[0] for desc in cur.description] if cur.description else []
        cur.close()
        conn.close()
        return elapsed, len(rows), rows[:5], col_names, None
    except Exception as e:
        return None, 0, [], [], str(e)


def print_section(title: str, char="─"):
    width = 68
    print(f"\n  {char * width}")
    print(f"  {title}")
    print(f"  {char * width}")


def print_check(name: str, passed: bool, detail: str = ""):
    icon = "✓ PASS" if passed else "✗ FAIL"
    line = f"  {name:<35} {icon}"
    if detail:
        line += f"  ({detail})"
    print(line)


def show_diff(original: str, optimized: str):
    """Show what changed between original and optimized queries."""
    orig_lines = original.strip().splitlines()
    opt_lines = optimized.strip().splitlines()

    print("\n  ┌─ CHANGES APPLIED ─────────────────────────────────────────┐")
    max_lines = max(len(orig_lines), len(opt_lines))
    changes = 0
    for i in range(max_lines):
        ol = orig_lines[i].rstrip() if i < len(orig_lines) else ""
        nl = opt_lines[i].rstrip() if i < len(opt_lines) else ""
        if ol != nl:
            changes += 1
            if ol:
                print(f"  │ - {ol[:62]}")
            if nl:
                print(f"  │ + {nl[:62]}")
            print(f"  │")
    if changes == 0:
        print("  │  (no differences)")
    print(f"  └─ {changes} line(s) changed ─────────────────────────────────┘")


def preview_rows(rows, col_names, max_cols=6):
    """Print a small preview of result rows."""
    if not rows or not col_names:
        return
    cols = col_names[:max_cols]
    print(f"\n  Preview (first {len(rows)} rows, {len(cols)} cols):")

    # Truncate values
    def fmt(v):
        s = str(v)
        return s[:20] + "…" if len(s) > 20 else s

    header = " | ".join(f"{c[:15]:>15}" for c in cols)
    print(f"  {header}")
    print(f"  {'-' * len(header)}")
    for row in rows:
        vals = " | ".join(f"{fmt(row[i]) if i < len(row) else '':>15}" for i in range(len(cols)))
        print(f"  {vals}")


# ═══════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Test any SQL query through the SQL Optimizer Agent"
    )
    parser.add_argument("--file", "-f", help="Path to a .sql file")
    parser.add_argument("--sql", "-s", help="Inline SQL query string")
    parser.add_argument("--timeout", "-t", type=int, default=10,
                        help="Max execution time in seconds (default: 10)")
    parser.add_argument("--skip-original", action="store_true",
                        help="Skip executing the original query")
    parser.add_argument("--show-diff", action="store_true",
                        help="Show diff between original and optimized")
    parser.add_argument("--save", action="store_true",
                        help="Save report to tests/my_query_report.txt")
    args = parser.parse_args()

    # ── Get the query ───────────────────────────────────────
    if args.file:
        with open(args.file, 'r') as f:
            query = f.read()
        source = args.file
    elif args.sql:
        query = args.sql
        source = "inline"
    else:
        print("═" * 68)
        print("  Paste your SQL query below.")
        print("  End with Ctrl+D (Linux/Mac) or type END on a new line.")
        print("═" * 68)
        lines = []
        try:
            for line in sys.stdin:
                if line.strip().upper() == 'END':
                    break
                lines.append(line)
        except EOFError:
            pass
        query = ''.join(lines)
        source = "stdin"

    if not query.strip():
        print("Error: No query provided.")
        sys.exit(1)

    report = []
    def log(msg=""):
        print(msg)
        report.append(msg)

    # ── Header ──────────────────────────────────────────────
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log("═" * 68)
    log("  SQL OPTIMIZER AGENT — QUERY TEST REPORT")
    log("═" * 68)
    log(f"  Date       : {now}")
    log(f"  Source     : {source}")
    log(f"  Timeout    : {args.timeout}s")
    log(f"  Query size : {len(query)} chars, {len(query.splitlines())} lines")
    log("═" * 68)

    # ── Step 1: Run original query (optional) ───────────────
    orig_time = None
    orig_rows = 0
    if not args.skip_original:
        print_section("STEP 1: Execute Original Query")
        log("")
        orig_time, orig_rows, orig_preview, orig_cols, orig_err = execute_query(
            query, timeout_sec=args.timeout + 5
        )
        if orig_err:
            log(f"  Original query error: {orig_err}")
            log(f"  (This is expected for very slow queries — use --skip-original)")
        else:
            log(f"  Execution time : {orig_time:.3f}s")
            log(f"  Rows returned  : {orig_rows}")
            preview_rows(orig_preview, orig_cols)
    else:
        log("\n  [Skipped original query execution]")

    # ── Step 2: Run optimizer ───────────────────────────────
    print_section("STEP 2: Optimize Query (Rule-Based Agent)")
    log("")
    orchestrator = AgentOrchestrator(use_llm=False, use_explain=False)

    try:
        result = orchestrator.optimize(query)
    except Exception as e:
        log(f"  ✗ Agent crashed: {e}")
        orchestrator.close()
        sys.exit(1)

    optimized = result.optimized_query
    changed = optimized.strip() != query.strip()

    log(f"  Bottlenecks detected  : {len(result.bottlenecks)}")
    log(f"  Suggestions generated : {len(result.suggestions)}")
    log(f"  Query modified        : {'Yes' if changed else 'No (returned original)'}")
    log(f"  Expected improvement  : {result.expected_improvement}")

    if result.bottlenecks:
        log("\n  Bottlenecks:")
        for b in result.bottlenecks:
            log(f"    [{b.severity:6}] {b.bottleneck_type}: {b.description}")

    if result.suggestions:
        log("\n  Suggestions applied:")
        for s in result.suggestions:
            log(f"    • {s.strategy_id}: {s.explanation[:70]}")

    if args.show_diff and changed:
        show_diff(query, optimized)

    # ── Step 3: Validate optimized query ────────────────────
    print_section("STEP 3: Validate Optimized Query")
    log("")
    validator = SyntaxValidator()
    validation = validator.validate(optimized)
    print_check("Syntax validation (static)", validation.is_valid)
    if not validation.is_valid:
        for err in validation.errors:
            log(f"    Error: {err}")

    # ── Step 4: Execute optimized query ─────────────────────
    print_section("STEP 4: Execute Optimized Query")
    log("")
    opt_time, opt_rows, opt_preview, opt_cols, opt_err = execute_query(
        optimized, timeout_sec=args.timeout + 5
    )

    checks = {}
    if opt_err:
        log(f"  ✗ Execution error: {opt_err}")
        checks["execution_no_error"] = False
        checks["under_threshold"] = False
    else:
        checks["execution_no_error"] = True
        checks["under_threshold"] = opt_time is not None and opt_time < args.timeout
        log(f"  Execution time : {opt_time:.3f}s")
        log(f"  Rows returned  : {opt_rows}")
        preview_rows(opt_preview, opt_cols)

    # ── Step 5: Summary ─────────────────────────────────────
    print_section("SUMMARY", "═")
    log("")
    print_check("Optimization produced", changed)
    print_check("Syntax valid", validation.is_valid)
    print_check("Execution error-free", checks.get("execution_no_error", False))
    print_check(f"Execution under {args.timeout}s", checks.get("under_threshold", False),
                f"{opt_time:.3f}s" if opt_time else "N/A")

    if orig_time and opt_time:
        speedup = orig_time / opt_time if opt_time > 0 else float('inf')
        log(f"\n  Speedup: {speedup:.1f}x  ({orig_time:.3f}s → {opt_time:.3f}s)")

    all_pass = changed and validation.is_valid and checks.get("execution_no_error") and checks.get("under_threshold")
    log("")
    if all_pass:
        log("  ✅ OVERALL: PASSED — Query is optimized, valid, and fast.")
    else:
        log("  ❌ OVERALL: FAILED — See details above.")
    log("")
    log("═" * 68)

    # ── Show optimized SQL ──────────────────────────────────
    print("\n  ┌─ OPTIMIZED SQL ────────────────────────────────────────────┐")
    for line in optimized.strip().splitlines():
        print(f"  │ {line}")
    print("  └──────────────────────────────────────────────────────────────┘")

    # ── Save report ─────────────────────────────────────────
    if args.save:
        report_path = os.path.join(os.path.dirname(__file__), 'my_query_report.txt')
        with open(report_path, 'w') as f:
            f.write('\n'.join(report))
        print(f"\n  Report saved to: {report_path}")

    orchestrator.close()
    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
