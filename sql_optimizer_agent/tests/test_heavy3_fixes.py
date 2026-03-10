#!/usr/bin/env python3
"""
Test: Run the optimizer agent on the Heavy 3 query and verify
that the two bugs are fixed:
1. WHERE clause is NOT deleted/commented out
2. Recursive CTE does NOT get MATERIALIZED, CTE names preserve case
"""
import sys, types

# Mock psycopg2 to avoid DB connection hang
mock = types.ModuleType('psycopg2')
mock.Error = Exception
sys.modules['psycopg2'] = mock

sys.path.insert(0, '/home/galaxy/DB_setup/sql_optimizer_agent')

from optimizer.bottleneck import BottleneckDetector
from optimizer.strategies import OptimizationStrategies
from optimizer.rewriter import QueryRewriter

QUERY = open('/tmp/heavy3_query.sql').read()

detector = BottleneckDetector()
bottlenecks = detector.detect(QUERY)

print("=" * 60)
print("DETECTED BOTTLENECKS")
print("=" * 60)
for b in bottlenecks:
    print(f"  [{b.severity}] Line {b.line_number}: {b.bottleneck_type}")
    print(f"    Suggestion: {b.suggestion}")
    print()

strategies = OptimizationStrategies()
suggestions = strategies.generate_suggestions(QUERY, bottlenecks)

print("=" * 60)
print("OPTIMIZATION SUGGESTIONS")
print("=" * 60)
if not suggestions:
    print("  (No rule-based suggestions generated)")
for s in suggestions:
    print(f"  Line {s.line_number}: {s.strategy_id}")
    print(f"    Original:  {s.original_content[:80]}")
    print(f"    Suggested: {s.suggested_content[:80]}")
    print()

rewriter = QueryRewriter()
optimized = rewriter.create_optimized_query(QUERY, suggestions, aggressive=True)

print("=" * 60)
print("OPTIMIZED QUERY")
print("=" * 60)
print(optimized)
print()

print("=" * 60)
print("VERIFICATION CHECKS")
print("=" * 60)
ok = True

# Check 1: WHERE clause preserved
if "WHERE aid % 1000 = 0 AND aid <= 500000" not in optimized:
    print("  FAIL: WHERE clause filter was removed!")
    ok = False
else:
    print("  PASS: WHERE clause filter preserved.")

# Check 2: CTE names not uppercased
if "TRANSACTION_CHAIN" in optimized or "CHAIN_ANALYTICS" in optimized:
    print("  FAIL: CTE names were uppercased!")
    ok = False
else:
    print("  PASS: CTE names preserve original case.")

# Check 3: MATERIALIZED not applied to recursive CTE
for line in optimized.split("\n"):
    if "MATERIALIZED" in line and "RECURSIVE" in line.upper():
        print("  FAIL: MATERIALIZED applied to recursive CTE!")
        ok = False
        break
else:
    print("  PASS: No MATERIALIZED on recursive CTE.")

# Check 4: Non-recursive CTE got MATERIALIZED (bonus)
if "chain_analytics AS MATERIALIZED" in optimized.lower():
    print("  PASS: Non-recursive CTE chain_analytics correctly got MATERIALIZED.")

if ok:
    print("\n  ALL CHECKS PASSED")
else:
    print("\n  SOME CHECKS FAILED")
    sys.exit(1)
