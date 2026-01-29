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
    
    def create_optimized_query(self, query: str, suggestions: List, aggressive: bool = False) -> str:
        """Create an optimized version of the query"""
        optimized = self.apply_suggestions(query, suggestions)
        if aggressive:
            optimized = self.reduce_cross_join_range(optimized, target_rows=500)
            optimized = self.remove_percentile_cont(optimized)
        return optimized
