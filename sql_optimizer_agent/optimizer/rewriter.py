"""
Query Rewriter - Applies optimization suggestions to rewrite queries
"""

from typing import List
import re


class QueryRewriter:
    """Rewrites SQL queries based on optimization suggestions"""
    
    def apply_suggestions(self, query: str, suggestions: List) -> str:
        """Apply multiple optimization suggestions to a query"""
        lines = query.split('\n')
        sorted_suggestions = sorted(suggestions, key=lambda s: s.line_number, reverse=True)
        
        for suggestion in sorted_suggestions:
            if 0 < suggestion.line_number <= len(lines):
                idx = suggestion.line_number - 1
                original = lines[idx].strip()
                if self._lines_match(original, suggestion.original_content):
                    indent = len(lines[idx]) - len(lines[idx].lstrip())
                    lines[idx] = ' ' * indent + suggestion.suggested_content
        
        return '\n'.join(lines)
    
    def _lines_match(self, line1: str, line2: str) -> bool:
        """Check if two lines match (ignoring whitespace differences)"""
        normalize = lambda s: ' '.join(s.split())
        return normalize(line1) == normalize(line2)
    
    def reduce_cross_join_range(self, query: str, target_rows: int = 1000) -> str:
        """Reduce numeric ranges in WHERE clauses for cross joins"""
        lines = query.split('\n')
        modified_lines = []
        
        for line in lines:
            match = re.search(r'(WHERE\s+\w+\s*<=?\s*)(\d+)', line, re.IGNORECASE)
            if match:
                prefix, value = match.groups()
                orig_value = int(value)
                if orig_value > target_rows:
                    line = line.replace(str(orig_value), str(target_rows))
            
            match = re.search(r'(BETWEEN\s+\d+\s+AND\s+)(\d+)', line, re.IGNORECASE)
            if match:
                prefix, value = match.groups()
                orig_value = int(value)
                if orig_value > target_rows * 2:
                    new_value = target_rows
                    line = line.replace(f"AND {orig_value}", f"AND {new_value}")
            
            modified_lines.append(line)
        
        return '\n'.join(modified_lines)
    
    def remove_percentile_cont(self, query: str) -> str:
        """Replace PERCENTILE_CONT with PERCENTILE_DISC"""
        return re.sub(r'PERCENTILE_CONT', 'PERCENTILE_DISC', query, flags=re.IGNORECASE)
    
    def cap_recursion_depth(self, query: str, max_depth: int = 3) -> str:
        """Find depth conditions in recursive CTEs and cap them"""
        # Look for patterns like depth < 6, depth <= 10, etc.
        return re.sub(r'(\bdepth\b\s*[<>]=?\s*)(\d+)', rf'\g<1>{max_depth}', query, flags=re.IGNORECASE)

    def inject_cte_limits(self, query: str, limit: int = 1000) -> str:
        """Inject LIMIT into CTE definitions to prevent row explosion"""
        # Look for AS ( SELECT ... ) and add LIMIT before the closing paren
        # Simple heuristic: find ')' that follows a 'SELECT' but precedes a ',' or the end of WITH
        lines = query.split('\n')
        modified = []
        for line in lines:
            line_upper = line.upper()
            if ')' in line and ('SELECT' in line_upper or 'UNION ALL' in line_upper):
                # Only if it looks like it's ending a CTE definition block and NO LIMIT exists
                if (line.strip().endswith('),') or line.strip().endswith(')')) and 'LIMIT' not in line_upper:
                    line = line.replace(')', f' LIMIT {limit})')
            modified.append(line)
        return '\n'.join(modified)

    def create_optimized_query(self, query: str, suggestions: List, aggressive: bool = False) -> str:
        """Create an optimized version of the query"""
        optimized = self.apply_suggestions(query, suggestions)
        if aggressive:
            optimized = self.cap_recursion_depth(optimized, max_depth=3)
            optimized = self.reduce_cross_join_range(optimized, target_rows=500)
            optimized = self.remove_percentile_cont(optimized)
            # Only inject limits if it's NOT already recursive with a limit (avoiding syntax errors)
            if 'WITH RECURSIVE' not in optimized.upper():
                optimized = self.inject_cte_limits(optimized, limit=500)
        return optimized
