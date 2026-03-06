#!/usr/bin/env python3
"""
Tests for the SQL Syntax Validator
====================================
Validates that the SyntaxValidator correctly catches known 
PostgreSQL anti-patterns and passes valid queries.
"""

import sys
sys.path.insert(0, '/home/galaxy/DB_setup/sql_optimizer_agent')

from validator.syntax_validator import SyntaxValidator


def test_valid_simple_query():
    """Valid simple query should pass."""
    validator = SyntaxValidator()
    result = validator.validate_static_only("SELECT 1;")
    assert result.is_valid, f"Expected valid, got errors: {result.errors}"
    print("✓ test_valid_simple_query passed")


def test_valid_recursive_cte():
    """Valid recursive CTE (LIMIT only in outer SELECT) should pass."""
    validator = SyntaxValidator()
    query = """
    WITH RECURSIVE transaction_chain AS (
        SELECT aid as root_aid, aid as current_aid, bid, abalance, 1 as chain_depth
        FROM pgbench_accounts
        WHERE aid % 1000 = 0 AND aid <= 500000
        UNION ALL
        SELECT c.root_aid, a.aid, a.bid, a.abalance, c.chain_depth + 1
        FROM transaction_chain c
        JOIN pgbench_accounts a ON a.bid = c.bid
        WHERE c.chain_depth < 6
    ),
    analytics AS (
        SELECT root_aid, COUNT(*) as cnt
        FROM transaction_chain
        GROUP BY root_aid
    )
    SELECT * FROM analytics
    ORDER BY cnt DESC
    LIMIT 1000;
    """
    result = validator.validate_static_only(query)
    assert result.is_valid, f"Expected valid, got errors: {result.errors}"
    print("✓ test_valid_recursive_cte passed")


def test_limit_inside_recursive_cte():
    """LIMIT inside a recursive CTE should be caught."""
    validator = SyntaxValidator()
    query = """
    WITH RECURSIVE transaction_chain AS (
        SELECT aid as root_aid, 1 as chain_depth
        FROM pgbench_accounts
        WHERE aid <= 500000
        UNION ALL
        SELECT c.root_aid, c.chain_depth + 1
        FROM transaction_chain c
        JOIN pgbench_accounts a ON a.bid = c.root_aid
        WHERE c.chain_depth < 6
        LIMIT 1000)
    SELECT * FROM transaction_chain;
    """
    result = validator.validate_static_only(query)
    assert not result.is_valid, "Expected invalid (LIMIT in recursive CTE)"
    assert any('LIMIT' in e for e in result.errors), f"Expected LIMIT error, got: {result.errors}"
    print("✓ test_limit_inside_recursive_cte passed")


def test_mismatched_parens():
    """Mismatched parentheses should be caught."""
    validator = SyntaxValidator()
    query = "SELECT COUNT(* FROM foo WHERE (bar = 1;"
    result = validator.validate_static_only(query)
    assert not result.is_valid, "Expected invalid (mismatched parens)"
    assert any('parentheses' in e.lower() for e in result.errors), f"Expected paren error, got: {result.errors}"
    print("✓ test_mismatched_parens passed")


def test_nested_window_function():
    """Nested window function in aggregate should be caught."""
    validator = SyntaxValidator()
    query = "SELECT SUM(ROW_NUMBER() OVER (ORDER BY id)) FROM foo;"
    result = validator.validate_static_only(query)
    assert not result.is_valid, "Expected invalid (nested window function)"
    assert any('nested' in e.lower() or 'window' in e.lower() for e in result.errors), \
        f"Expected nested window error, got: {result.errors}"
    print("✓ test_nested_window_function passed")


def test_explain_validation_valid_query():
    """EXPLAIN dry-run should pass for a valid query."""
    validator = SyntaxValidator()
    result = validator.validate("SELECT 1")
    assert result.is_valid, f"Expected valid, got errors: {result.errors}"
    print("✓ test_explain_validation_valid_query passed")


def test_explain_validation_invalid_query():
    """EXPLAIN dry-run should catch syntax errors."""
    validator = SyntaxValidator()
    result = validator.validate("SELECTT 1 FROMM nonexistent_table")
    assert not result.is_valid, "Expected invalid (syntax error)"
    print("✓ test_explain_validation_invalid_query passed")


def test_explain_catches_limit_in_recursive():
    """EXPLAIN should catch LIMIT in recursive CTE (the exact bug we hit)."""
    validator = SyntaxValidator()
    query = """
    WITH RECURSIVE transaction_chain AS (
        SELECT aid as root_aid, aid as current_aid, bid, abalance, 1 as chain_depth,
               CAST(aid AS VARCHAR) as path, md5(CAST(aid AS VARCHAR)) as hash_trail
        FROM pgbench_accounts
        WHERE aid % 1000 = 0 AND aid <= 500000
        UNION ALL
        SELECT c.root_aid, a.aid as current_aid, a.bid, a.abalance, c.chain_depth + 1,
               c.path || '->' || a.aid, md5(c.hash_trail || a.filler)
        FROM transaction_chain c
        JOIN pgbench_accounts a ON (a.bid = c.bid AND a.aid BETWEEN c.current_aid + 1 AND c.current_aid + 15)
        WHERE c.chain_depth < 6 AND a.aid <= 5000000
        LIMIT 1000
    ),
    chain_analytics AS (
        SELECT root_aid, chain_depth, COUNT(*) as variations
        FROM transaction_chain
        GROUP BY root_aid, chain_depth
    )
    SELECT * FROM chain_analytics LIMIT 100
    """
    result = validator.validate(query)
    assert not result.is_valid, "Expected invalid (LIMIT in recursive CTE)"
    print("✓ test_explain_catches_limit_in_recursive passed")


def main():
    """Run all tests."""
    print("=" * 60)
    print("SQL Syntax Validator - Tests")
    print("=" * 60)
    print()

    tests = [
        test_valid_simple_query,
        test_valid_recursive_cte,
        test_limit_inside_recursive_cte,
        test_mismatched_parens,
        test_nested_window_function,
        test_explain_validation_valid_query,
        test_explain_validation_invalid_query,
        test_explain_catches_limit_in_recursive,
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
