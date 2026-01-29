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
            conn.set_session(readonly=True)
            cursor = conn.cursor()
            
            # Set timeout for safety
            cursor.execute("SET statement_timeout = '30s'")
            
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
            return self._compare_results(
                orig_results, opt_results,
                orig_columns, opt_columns
            )
            
        except psycopg2.Error as e:
            return {
                "valid": False,
                "error": str(e),
                "reason": "Query execution failed"
            }
    
    def _add_limit(self, query: str, limit: int) -> str:
        """Add LIMIT clause if not present"""
        query_upper = query.upper()
        if 'LIMIT' not in query_upper:
            query = query.rstrip().rstrip(';')
            query = f"{query} LIMIT {limit}"
        return query
    
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
