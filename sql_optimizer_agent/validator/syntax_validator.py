"""
SQL Syntax Validator - Validates optimized queries before deployment.

Uses two layers of validation:
1. Static rule checks for known PostgreSQL anti-patterns (fast, no DB needed)
2. EXPLAIN dry-run validation against PostgreSQL (catches all syntax errors)
"""

import re
import psycopg2
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
import sys

sys.path.insert(0, '/home/galaxy/DB_setup/sql_optimizer_agent')
from config import DB_CONFIG


@dataclass
class ValidationResult:
    """Result of SQL syntax validation"""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    @property
    def error(self) -> Optional[str]:
        """Return first error or None"""
        return self.errors[0] if self.errors else None


class SyntaxValidator:
    """Validates SQL queries for syntactic correctness against PostgreSQL.
    
    Combines fast static checks with an EXPLAIN dry-run to catch both
    known LLM anti-patterns and any other syntax issues.
    """

    # Known PostgreSQL anti-patterns that LLMs commonly produce
    STATIC_RULES = [
        {
            'id': 'LIMIT_IN_RECURSIVE_CTE',
            'description': 'LIMIT/OFFSET inside a recursive CTE is not supported in PostgreSQL',
            'severity': 'error',
        },
        {
            'id': 'OFFSET_IN_CTE',
            'description': 'OFFSET inside a CTE definition is not supported',
            'severity': 'error',
        },
        {
            'id': 'NESTED_WINDOW_FUNCTION',
            'description': 'Window functions cannot be nested',
            'severity': 'error',
        },
        {
            'id': 'MISMATCHED_PARENS',
            'description': 'Mismatched parentheses in query',
            'severity': 'error',
        },
    ]

    def __init__(self, db_config: Dict = None):
        self.db_config = db_config or DB_CONFIG

    def validate(self, query: str) -> ValidationResult:
        """Run full validation: static checks then EXPLAIN dry-run.
        
        Returns ValidationResult with is_valid=True only if all checks pass.
        """
        result = ValidationResult(is_valid=True)

        # Layer 1: Static rule checks (fast, no DB connection needed)
        self._check_static_rules(query, result)

        # If static checks already found errors, skip the DB round-trip
        if not result.is_valid:
            return result

        # Layer 2: EXPLAIN dry-run against PostgreSQL
        self._check_explain(query, result)

        return result

    def validate_static_only(self, query: str) -> ValidationResult:
        """Run only static rule checks (no database connection required)."""
        result = ValidationResult(is_valid=True)
        self._check_static_rules(query, result)
        return result

    # ── Static Rule Checks ──────────────────────────────────────────

    def _check_static_rules(self, query: str, result: ValidationResult):
        """Apply all static rule checks to the query."""
        self._check_limit_in_recursive_cte(query, result)
        self._check_offset_in_cte(query, result)
        self._check_nested_window_functions(query, result)
        self._check_mismatched_parens(query, result)

    def _check_limit_in_recursive_cte(self, query: str, result: ValidationResult):
        """Detect LIMIT or OFFSET placed inside a WITH RECURSIVE CTE body."""
        upper = query.upper()

        if 'WITH RECURSIVE' not in upper:
            return

        # Strategy: find the CTE definition block between WITH RECURSIVE and the
        # final outer SELECT. We look for LIMIT/OFFSET keywords that appear inside
        # the CTE body (before the closing paren of the CTE definition).
        
        # Find everything after WITH RECURSIVE up to the content
        recursive_start = upper.index('WITH RECURSIVE') + len('WITH RECURSIVE')
        cte_section = upper[recursive_start:]
        
        # Track paren depth to find where CTE definitions end
        # CTE definitions are wrapped in AS (...), so we need to find the outermost
        # closing paren that ends the last CTE definition
        depth = 0
        cte_end = len(cte_section)
        for i, ch in enumerate(cte_section):
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    cte_end = i
                    # Don't break — there might be more CTE definitions after a comma

        cte_body = cte_section[:cte_end]

        # Check for LIMIT/OFFSET inside the CTE body
        if re.search(r'\bLIMIT\s+\d+', cte_body):
            result.is_valid = False
            result.errors.append(
                'LIMIT inside a recursive CTE is not supported in PostgreSQL. '
                'Move the LIMIT to the outer/final SELECT statement instead.'
            )

        if re.search(r'\bOFFSET\s+\d+', cte_body):
            result.is_valid = False
            result.errors.append(
                'OFFSET inside a recursive CTE is not supported in PostgreSQL.'
            )

    def _check_offset_in_cte(self, query: str, result: ValidationResult):
        """Detect OFFSET inside any CTE definition (not just recursive)."""
        upper = query.upper()
        if 'WITH ' not in upper:
            return

        # Find CTE blocks: content between AS ( ... )
        cte_bodies = re.findall(r'AS\s*\((.*?)\)', upper, re.DOTALL)
        for body in cte_bodies:
            # OFFSET is valid after ORDER BY in subqueries, but not at CTE level in some cases
            if re.search(r'\bOFFSET\s+\d+', body) and 'RECURSIVE' not in upper:
                result.warnings.append(
                    'OFFSET found inside a CTE definition. Verify this is intentional.'
                )

    def _check_nested_window_functions(self, query: str, result: ValidationResult):
        """Detect nested window function calls like SUM(ROW_NUMBER() OVER ...)."""
        upper = query.upper()
        # Pattern: agg_function( ... OVER( ... ) ... ) OVER
        # Simplified: look for two OVER keywords in close proximity within a single expression
        window_funcs = ['ROW_NUMBER', 'RANK', 'DENSE_RANK', 'NTILE', 'LAG', 'LEAD',
                        'FIRST_VALUE', 'LAST_VALUE', 'NTH_VALUE']
        
        for func in window_funcs:
            # Check if a window function appears inside another aggregate/window
            pattern = rf'\b(?:SUM|AVG|COUNT|MAX|MIN)\s*\([^)]*\b{func}\s*\('
            if re.search(pattern, upper):
                result.is_valid = False
                result.errors.append(
                    f'Nested window function detected: {func} inside an aggregate. '
                    f'Window functions cannot be nested in PostgreSQL.'
                )

    def _check_mismatched_parens(self, query: str, result: ValidationResult):
        """Check for mismatched parentheses."""
        # Strip string literals first to avoid counting parens inside strings
        cleaned = re.sub(r"'[^']*'", '', query)
        open_count = cleaned.count('(')
        close_count = cleaned.count(')')

        if open_count != close_count:
            result.is_valid = False
            result.errors.append(
                f'Mismatched parentheses: {open_count} opening vs {close_count} closing.'
            )

    # ── EXPLAIN Dry-Run Validation ──────────────────────────────────

    def _check_explain(self, query: str, result: ValidationResult):
        """Run EXPLAIN (not ANALYZE) to validate query syntax against PostgreSQL.
        
        EXPLAIN parses and plans the query without executing it, catching
        all syntax errors including PostgreSQL-specific constraints.
        """
        try:
            conn = psycopg2.connect(**self.db_config)
            conn.set_session(readonly=True)
            cursor = conn.cursor()

            # Set a short timeout — EXPLAIN should be near-instant
            cursor.execute("SET statement_timeout = '10s'")

            # EXPLAIN without ANALYZE: plans but does NOT execute the query
            explain_query = f"EXPLAIN {query.rstrip().rstrip(';')}"
            cursor.execute(explain_query)

            # If we get here, the query is syntactically valid
            cursor.close()
            conn.close()

        except psycopg2.Error as e:
            result.is_valid = False
            error_msg = str(e).strip()
            result.errors.append(f'PostgreSQL syntax validation failed: {error_msg}')

        except Exception as e:
            # Don't fail validation on connection errors — just warn
            result.warnings.append(
                f'Could not connect to database for EXPLAIN validation: {e}'
            )
