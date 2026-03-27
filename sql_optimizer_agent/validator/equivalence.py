"""
Output Equivalence Validator - Validates that optimized query produces same results.

FIX Bug 2: Replaced conn.set_session(readonly=True) — which crashes after
           autocommit=True DDL work — with SET TRANSACTION READ ONLY, which
           is safe within the same session.

FIX Bug 3: Added SET search_path = pg_temp, public so PostgreSQL resolves
           unqualified table names to our shadow temp tables first, instead
           of falling through to the full production public.* tables.
"""

import psycopg2
from typing import Dict, Any, Optional
import hashlib
import sys
sys.path.insert(0, '/home/galaxy/DB_setup/sql_optimizer_agent')
from config import DB_CONFIG


class EquivalenceValidator:
    """Validates that two queries produce identical results."""

    def __init__(self, db_config: Dict = None):
        self.db_config = db_config or DB_CONFIG

    def validate(self, original_query: str, optimized_query: str,
                 limit: int = 50) -> Dict[str, Any]:
        """
        Validate that the optimized query produces the same results as the original.

        Strategy:
        1. Parse both queries to find all real table names (excluding CTEs).
        2. Use anchored sampling to pick a small set of representative rows.
        3. Build shadow temp tables from those rows on a single connection
           with autocommit=True (required for DDL).
        4. Switch autocommit OFF on the same connection (temp tables are
           session-scoped, so we must reuse it), set search_path to pg_temp
           so queries hit our shadow tables, then execute both queries
           read-only and compare their results.
        """
        orig_limited = self._add_limit(original_query, limit)
        opt_limited  = self._add_limit(optimized_query, limit)

        # Strip explicit schema prefix up-front — temp tables are unqualified
        orig_limited = orig_limited.replace('public.', '')
        opt_limited  = opt_limited.replace('public.', '')

        validation_method_str = "Standard LIMIT Validation"
        sampled_keys  = []
        anchor_table  = None

        try:
            # ----------------------------------------------------------------
            # Phase 1: DDL — create shadow temp tables (requires autocommit=True)
            # ----------------------------------------------------------------
            conn = psycopg2.connect(**self.db_config)
            conn.autocommit = True
            cursor = conn.cursor()

            # Parse both queries to discover real table names (not CTEs)
            from analyzer.sql_parser import SQLParser
            parser    = SQLParser()
            p1        = parser.parse(original_query)
            p2        = parser.parse(optimized_query)
            cte_names = [c['name'] for c in (p1.ctes + p2.ctes)]
            used_tables = list(set(
                [t for t in (p1.tables + p2.tables) if t not in cte_names]
            ))

            if used_tables:
                from analyzer.schema_analyzer import SchemaAnalyzer
                schema_analyzer = SchemaAnalyzer(self.db_config)
                anchor_info     = schema_analyzer.find_anchor_key(used_tables)
                key_list_str    = ""

                if anchor_info:
                    anchor_table   = anchor_info['anchor_table']
                    anchor_key     = anchor_info['anchor_key']
                    related_tables = anchor_info['related_tables']
                    method         = anchor_info['method']

                    # Sample a small set of anchor keys to bound our mini-DB
                    try:
                        try:
                            cursor.execute(
                                f"SELECT {anchor_key} FROM {anchor_table} "
                                f"TABLESAMPLE SYSTEM (0.1) LIMIT 5"
                            )
                        except Exception:
                            cursor.execute(
                                f"SELECT {anchor_key} FROM {anchor_table} LIMIT 5"
                            )
                        sampled_keys = [
                            str(row[0]) for row in cursor.fetchall()
                            if row[0] is not None
                        ]
                        if sampled_keys:
                            validation_method_str = (
                                f"Dynamic Anchored Sampling "
                                f"({len(sampled_keys)} {anchor_key}s via {method})"
                            )
                            key_list_str = ",".join(f"'{k}'" for k in sampled_keys)
                    except Exception as e:
                        print(f"Sampling failed: {e}")

                if not sampled_keys:
                    validation_method_str = "Statistical TABLESAMPLE Validation (1% sampling)"

                # Build shadow temp tables from the sampled universe
                for table in used_tables:
                    cursor.execute(f"DROP TABLE IF EXISTS pg_temp.{table}")
                    if sampled_keys and (
                        table == anchor_table or table in (related_tables or {})
                    ):
                        fk_col = (
                            anchor_key
                            if table == anchor_table
                            else related_tables[table]
                        )
                        cursor.execute(
                            f"CREATE TEMP TABLE {table} AS "
                            f"SELECT * FROM public.{table} "
                            f"WHERE {fk_col} IN ({key_list_str})"
                        )
                    else:
                        cursor.execute(
                            f"CREATE TEMP TABLE {table} AS "
                            f"SELECT * FROM public.{table} LIMIT 1000"
                        )

            # ----------------------------------------------------------------
            # Phase 2: Query execution — reuse the same connection so temp
            # tables (which are session-scoped) remain visible.
            #
            # FIX Bug 2: set_session(readonly=True) raises ProgrammingError
            # when called after autocommit DDL work on the same connection.
            # Instead: turn autocommit OFF, then issue SET TRANSACTION READ ONLY
            # inside the transaction — this is fully supported by PostgreSQL.
            #
            # FIX Bug 3: SET search_path = pg_temp, public makes PostgreSQL
            # resolve unqualified table names to our shadow tables first,
            # bypassing the full production public.* tables entirely.
            # ----------------------------------------------------------------
            conn.autocommit = False
            cursor.execute("SET search_path = pg_temp, public;")
            cursor.execute("SET statement_timeout = 30000;")
            cursor.execute("SET TRANSACTION READ ONLY;")

            # Execute original query on mini-DB
            cursor.execute(orig_limited)
            orig_results = cursor.fetchall()
            orig_columns = (
                [desc[0] for desc in cursor.description]
                if cursor.description else []
            )

            # Execute optimized query on mini-DB
            cursor.execute(opt_limited)
            opt_results  = cursor.fetchall()
            opt_columns  = (
                [desc[0] for desc in cursor.description]
                if cursor.description else []
            )

            cursor.close()
            conn.close()

            result = self._compare_results(
                orig_results, opt_results,
                orig_columns, opt_columns
            )
            result['validation_method'] = validation_method_str
            result['sampled_keys']      = sampled_keys
            result['anchor_table']      = anchor_table
            return result

        except psycopg2.Error as e:
            return {
                "valid":             False,
                "error":             str(e),
                "reason":            f"Query execution failed: {e}",
                "validation_method": validation_method_str,
                "sampled_keys":      sampled_keys,
                "anchor_table":      anchor_table,
            }

    def _add_limit(self, query: str, limit: int) -> str:
        """
        Wrap query in a subquery with deterministic ordering and LIMIT.
        ORDER BY 1 ensures both original and optimized return the same
        subset regardless of differences in their execution plans.
        """
        query = query.rstrip().rstrip(';')
        return (
            f"SELECT * FROM (\n{query}\n) AS wrapped_query "
            f"ORDER BY 1 LIMIT {limit}"
        )

    def _compare_results(self, orig_results, opt_results,
                         orig_columns, opt_columns) -> Dict[str, Any]:
        """Compare two result sets for equivalence."""

        # Column names must match
        if orig_columns != opt_columns:
            return {
                "valid":              False,
                "reason":             "Column names differ",
                "original_columns":   orig_columns,
                "optimized_columns":  opt_columns,
            }

        # Row counts must match
        if len(orig_results) != len(opt_results):
            return {
                "valid":          False,
                "reason":         f"Row count differs: {len(orig_results)} vs {len(opt_results)}",
                "original_count": len(orig_results),
                "optimized_count": len(opt_results),
            }

        # Hash comparison (order-independent)
        orig_hash = self._hash_results(orig_results)
        opt_hash  = self._hash_results(opt_results)
        if orig_hash == opt_hash:
            return {
                "valid":     True,
                "reason":    "Results are identical",
                "row_count": len(orig_results),
            }

        # Sorted string comparison (catches same rows in different order)
        orig_sorted = sorted([str(r) for r in orig_results])
        opt_sorted  = sorted([str(r) for r in opt_results])
        if orig_sorted == opt_sorted:
            return {
                "valid":     True,
                "reason":    "Results are identical (different order)",
                "row_count": len(orig_results),
            }

        # Results genuinely differ — surface up to 10 differing rows
        differences = []
        for i, (o, p) in enumerate(zip(orig_sorted[:10], opt_sorted[:10])):
            if o != p:
                differences.append({
                    "row":       i,
                    "original":  o[:100],
                    "optimized": p[:100],
                })
        return {
            "valid":              False,
            "reason":             "Result values differ",
            "sample_differences": differences,
        }

    def _hash_results(self, results) -> str:
        """Create a stable, order-independent hash of a result set."""
        sorted_results = sorted([str(r) for r in results])
        content = '\n'.join(sorted_results)
        return hashlib.md5(content.encode()).hexdigest()

    def quick_validate(self, original_query: str, optimized_query: str) -> bool:
        """Quick boolean validation with a small sample."""
        result = self.validate(original_query, optimized_query, limit=100)
        return result.get("valid", False)