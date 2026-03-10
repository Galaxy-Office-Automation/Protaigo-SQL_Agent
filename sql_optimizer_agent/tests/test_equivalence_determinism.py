#!/usr/bin/env python3
"""
Tests for mathematical equivalence guarantees.
Verifies that the optimizer never swaps PERCENTILE_CONT with PERCENTILE_DISC
and that the equivalence validator uses deterministic ordering.
"""

import sys
sys.path.insert(0, '/home/galaxy/DB_setup/sql_optimizer_agent')

from optimizer.rewriter import QueryRewriter
from optimizer.strategies import OptimizationStrategies
from optimizer.bottleneck import BottleneckDetector
from validator.equivalence import EquivalenceValidator
from agent.reflection_agent import ReflectionAgent


def test_rewriter_no_percentile_swap():
    """QueryRewriter must NOT have a remove_percentile_cont method."""
    rewriter = QueryRewriter()
    assert not hasattr(rewriter, 'remove_percentile_cont'), \
        "remove_percentile_cont should have been removed from QueryRewriter"
    print("✓ test_rewriter_no_percentile_swap passed")


def test_rewriter_preserves_percentile_cont():
    """create_optimized_query must preserve PERCENTILE_CONT as-is."""
    rewriter = QueryRewriter()
    query = "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) FROM t"
    result = rewriter.create_optimized_query(query, [], aggressive=True)
    assert 'PERCENTILE_CONT' in result, \
        f"PERCENTILE_CONT was removed or altered: {result}"
    assert 'PERCENTILE_DISC' not in result, \
        f"PERCENTILE_CONT was swapped to PERCENTILE_DISC: {result}"
    print("✓ test_rewriter_preserves_percentile_cont passed")


def test_no_replace_percentile_strategy():
    """REPLACE_PERCENTILE strategy must not exist."""
    strategies = OptimizationStrategies()
    assert 'REPLACE_PERCENTILE' not in strategies.strategies, \
        "REPLACE_PERCENTILE strategy should have been removed"
    print("✓ test_no_replace_percentile_strategy passed")


def test_percentile_bottleneck_is_low():
    """PERCENTILE_CONT bottleneck must be LOW severity (informational)."""
    detector = BottleneckDetector()
    rule = next((r for r in detector.detection_rules if r['name'] == 'PERCENTILE_CONT'), None)
    assert rule is not None, "PERCENTILE_CONT rule should still exist"
    assert rule['severity'] == 'LOW', \
        f"Expected LOW severity, got {rule['severity']}"
    assert 'PERCENTILE_DISC' not in rule['suggestion'], \
        f"Suggestion should not mention PERCENTILE_DISC: {rule['suggestion']}"
    print("✓ test_percentile_bottleneck_is_low passed")


def test_equivalence_add_limit_has_order_by():
    """_add_limit must inject ORDER BY for deterministic row sampling."""
    validator = EquivalenceValidator()
    wrapped = validator._add_limit("SELECT a, b FROM t", 100)
    assert 'ORDER BY' in wrapped.upper(), \
        f"ORDER BY missing in wrapped query: {wrapped}"
    assert 'LIMIT 100' in wrapped, \
        f"LIMIT clause missing in wrapped query: {wrapped}"
    print("✓ test_equivalence_add_limit_has_order_by passed")


def test_reflection_catches_percentile_disc_injection():
    """Reflection agent must flag PERCENTILE_CONT→PERCENTILE_DISC as unauthorized."""
    # Create a minimal ReflectionAgent (no LLM needed for this check)
    agent = ReflectionAgent.__new__(ReflectionAgent)
    
    original = "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) FROM t"
    optimized = "SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY val) FROM t"
    
    result = agent._has_unauthorized_data_alteration(original, optimized)
    assert result is True, \
        "Should detect PERCENTILE_CONT→PERCENTILE_DISC as unauthorized alteration"
    print("✓ test_reflection_catches_percentile_disc_injection passed")


def test_reflection_allows_valid_optimization():
    """Reflection agent must NOT flag legitimate structural changes."""
    agent = ReflectionAgent.__new__(ReflectionAgent)
    
    original = "SELECT a FROM t WHERE aid <= 80000"
    optimized = "WITH cte AS MATERIALIZED (SELECT a FROM t WHERE aid <= 80000) SELECT a FROM cte"
    
    result = agent._has_unauthorized_data_alteration(original, optimized)
    assert result is False, \
        "Should not flag CTE wrapping as unauthorized alteration"
    print("✓ test_reflection_allows_valid_optimization passed")


def main():
    print("=" * 60)
    print("Mathematical Equivalence - Tests")
    print("=" * 60)
    print()

    tests = [
        test_rewriter_no_percentile_swap,
        test_rewriter_preserves_percentile_cont,
        test_no_replace_percentile_strategy,
        test_percentile_bottleneck_is_low,
        test_equivalence_add_limit_has_order_by,
        test_reflection_catches_percentile_disc_injection,
        test_reflection_allows_valid_optimization,
    ]

    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"✗ {test.__name__} FAILED: {e}")
            failed += 1
        except Exception as e:
            print(f"✗ {test.__name__} ERROR: {e}")
            failed += 1

    print()
    print(f"Results: {passed} passed, {failed} failed out of {len(tests)} tests")
    print("=" * 60)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
