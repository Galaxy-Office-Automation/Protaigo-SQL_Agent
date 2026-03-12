"""
Output Equivalence Validator - Validates that optimized query produces same results
"""

import psycopg2
from typing import Dict, Any, Optional, Tuple
import hashlib
import sys
sys.path.insert(0, '/home/galaxy/DB_setup/sql_optimizer_agent')
from config import DB_CONFIG


class EquivalenceValidator:
    """Validates that two queries produce identical results"""
    
    def __init__(self, db_config: Dict = None):
        self.db_config = db_config or DB_CONFIG
    
    def validate(self, original_query: str, optimized_query: str, 
                 limit: int = 1000) -> Dict[str, Any]:
        """
        Validate that optimized query produces same results as original.
        Uses LIMIT to avoid running full queries.
        """
        # Add LIMIT if not present for comparison
        orig_limited = self._add_limit(original_query, limit)
        opt_limited = self._add_limit(optimized_query, limit)
        
        try:
            conn = psycopg2.connect(**self.db_config)
            conn.autocommit = True
            cursor = conn.cursor()
            
            # --- STRATIFIED SAMPLING LOGIC ---
            # Extract ALL tables used in the query dynamically to support any schema
            import sys
            sys.path.insert(0, '/home/galaxy/DB_setup/sql_optimizer_agent')
            from analyzer.sql_parser import SQLParser
            parser = SQLParser()
            t1 = parser.parse(original_query).tables
            t2 = parser.parse(optimized_query).tables
            used_tables = list(set(t1 + t2))
            
            validation_method_str = "Standard LIMIT Validation"
            
            if used_tables:
                pgbench_bids = []
                pgbench_tables = [t for t in used_tables if 'pgbench' in t.lower()]
                generic_tables = [t for t in used_tables if 'pgbench' not in t.lower()]
                
                if pgbench_tables:
                    # Sample 3 random branch IDs for relationally-consistent pgbench Anchored Stratified Sampling
                    cursor.execute("SELECT bid FROM pgbench_branches ORDER BY RANDOM() LIMIT 3")
                    pgbench_bids = [str(row[0]) for row in cursor.fetchall()]
                    if pgbench_bids:
                        validation_method_str = "Anchored Stratified Sampling (3 random pgbench branches)"
                
                if generic_tables and not pgbench_tables:
                    validation_method_str = "Statistical TABLESAMPLE Validation (1% sampling)"

                # Shadow the public tables with small temporary datasets.
                # Temp tables persist through transactions in the same session and bypass public.
                for table in used_tables:
                    cursor.execute(f"DROP TABLE IF EXISTS pg_temp.{table}")
                    
                    if table in pgbench_tables and pgbench_bids:
                        # Anchored subset preserving foreign-keys bounds
                        bid_list = ",".join(pgbench_bids)
                        cursor.execute(f"CREATE TEMP TABLE {table} AS SELECT * FROM public.{table} WHERE bid IN ({bid_list})")
                    else:
                        # Generic arbitrary schema tables fallback
                        try:
                            # 1% fast block-sampling
                            cursor.execute(f"CREATE TEMP TABLE {table} AS SELECT * FROM public.{table} TABLESAMPLE SYSTEM (1)")
                        except Exception:
                            # Fallback for Views or systems ignoring TABLESAMPLE
                            cursor.execute(f"CREATE TEMP TABLE {table} AS SELECT * FROM public.{table} LIMIT 1000")
            
            # Switch back to explicit manual transactions for Read-Only safety when executing LLM generated query
            conn.autocommit = False
            conn.set_session(readonly=True)
            
            # Remove any explicit schema path mapping to force usage of our shadowed Temp tables
            orig_limited = orig_limited.replace('public.', '')
            opt_limited = opt_limited.replace('public.', '')
            
            # Set timeout for safety (keep it short so heavy queries fast-fail to heuristic checks)
            # 30 seconds should be plenty for our 3-branch sample to execute
            cursor.execute("SET statement_timeout = 30000;")
            
            # Execute original query
            cursor.execute(orig_limited)
            orig_results = cursor.fetchall()
            orig_columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Execute optimized query
            cursor.execute(opt_limited)
            opt_results = cursor.fetchall()
            opt_columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            cursor.close()
            conn.close()
            
            # Compare results
            result = self._compare_results(
                orig_results, opt_results,
                orig_columns, opt_columns
            )
            result['validation_method'] = validation_method_str
            return result
            
        except psycopg2.Error as e:
            return {
                "valid": False,
                "error": str(e),
                "reason": f"Query execution failed: {e}"
            }
    
    def _add_limit(self, query: str, limit: int) -> str:
        """Add LIMIT clause securely via subquery with deterministic ordering.
        
        Uses ORDER BY on ordinal column positions so both the original and
        optimized queries always return the exact same subset of rows,
        regardless of differences in their execution plans.
        """
        query = query.rstrip().rstrip(';')
        return (
            f"SELECT * FROM (\n{query}\n) AS wrapped_query "
            f"ORDER BY 1,2,3,4,5,6,7,8,9,10 LIMIT {limit}"
        )
    
    def _compare_results(self, orig_results, opt_results,
                          orig_columns, opt_columns) -> Dict[str, Any]:
        """Compare query results"""
        
        # Check column names
        if orig_columns != opt_columns:
            return {
                "valid": False,
                "reason": "Column names differ",
                "original_columns": orig_columns,
                "optimized_columns": opt_columns
            }
        
        # Check row count
        if len(orig_results) != len(opt_results):
            return {
                "valid": False,
                "reason": f"Row count differs: {len(orig_results)} vs {len(opt_results)}",
                "original_count": len(orig_results),
                "optimized_count": len(opt_results)
            }
        
        # Check result content (order-independent)
        orig_hash = self._hash_results(orig_results)
        opt_hash = self._hash_results(opt_results)
        
        if orig_hash == opt_hash:
            return {
                "valid": True,
                "reason": "Results are identical",
                "row_count": len(orig_results)
            }
        
        # Results differ - try sorted comparison
        orig_sorted = sorted([str(r) for r in orig_results])
        opt_sorted = sorted([str(r) for r in opt_results])
        
        if orig_sorted == opt_sorted:
            return {
                "valid": True,
                "reason": "Results are identical (different order)",
                "row_count": len(orig_results)
            }
        
        # Find differences
        differences = []
        for i, (o, p) in enumerate(zip(orig_sorted[:10], opt_sorted[:10])):
            if o != p:
                differences.append({"row": i, "original": o[:100], "optimized": p[:100]})
        
        return {
            "valid": False,
            "reason": "Result values differ",
            "sample_differences": differences
        }
    
    def _hash_results(self, results) -> str:
        """Create hash of results for comparison"""
        sorted_results = sorted([str(r) for r in results])
        content = '\n'.join(sorted_results)
        return hashlib.md5(content.encode()).hexdigest()
    
    def quick_validate(self, original_query: str, optimized_query: str) -> bool:
        """Quick validation with small sample"""
        result = self.validate(original_query, optimized_query, limit=100)
        return result.get("valid", False)
