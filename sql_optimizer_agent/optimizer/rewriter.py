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
        """Reduce numeric ranges in WHERE/AND clauses to limit row counts.
        
        Matches patterns like: col <= N, col < N (where col contains a.id-like name)
        Only reduces values that are significantly larger than target_rows.
        """
        lines = query.split('\n')
        modified_lines = []
        
        # Pattern: dotted or plain column name ending in 'id' or 'aid' followed by <= large_number
        col_range_pattern = re.compile(
            r'(\b\w*\.?\w*(?:aid|bid|id)\b)\s*(<=?)\s*(\d+)', re.IGNORECASE
        )
        
        for line in lines:
            # Apply column range reduction
            def _replace_range(m):
                col, op, val_str = m.groups()
                val = int(val_str)
                if val > target_rows:
                    return f"{col} {op} {target_rows}"
                return m.group(0)
            
            line = col_range_pattern.sub(_replace_range, line)
            
            # Also handle BETWEEN with large upper bounds
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
        """Inject LIMIT into CTE definitions to prevent row explosion.
        
        Uses parenthesis tracking to find actual CTE body boundaries:
        - Identifies 'AS (' patterns that start a CTE body
        - Tracks paren depth to find the matching closing ')'
        - Injects 'LIMIT N' just before that closing ')'
        
        This avoids incorrectly injecting LIMIT inside IN(),
        OVER(), function calls, or other nested parentheses.
        """
        # Find CTE body positions: look for 'AS\s*(' pattern
        # then track depth to find the matching close paren
        cte_close_positions = []  # character positions of CTE-closing ')'
        
        # Simple state: find all 'CTE_NAME AS (' positions
        for match in re.finditer(r'\b(\w+)\s+AS\s*\(', query, re.IGNORECASE):
            cte_name = match.group(1)
            if cte_name.startswith('__'):
                continue
                
            open_pos = match.end() - 1  # position of the '(' in 'AS ('
            depth = 1
            pos = match.end()
            in_string = False
            string_char = None
            while pos < len(query) and depth > 0:
                ch = query[pos]
                if in_string:
                    if ch == string_char and (pos + 1 >= len(query) or query[pos + 1] != string_char):
                        in_string = False
                elif ch in ("'", '"'):
                    in_string = True
                    string_char = ch
                elif ch == '(':
                    depth += 1
                elif ch == ')':
                    depth -= 1
                    if depth == 0:
                        cte_close_positions.append(pos)
                pos += 1
        
        if not cte_close_positions:
            return query
        
        # Insert LIMIT before each CTE close position (working backwards to preserve positions)
        result = list(query)
        for close_pos in reversed(cte_close_positions):
            # Check if LIMIT already exists near this position (look back ~30 chars)
            lookback = query[max(0, close_pos - 40):close_pos].upper()
            if 'LIMIT' in lookback:
                continue
            # Insert ' LIMIT N' just before the closing ')'
            insert_text = f'\n LIMIT {limit}'
            result.insert(close_pos, insert_text)
        
        return ''.join(result)

    def rewrite_correlated_subqueries(self, query: str) -> str:
        """Rewrite correlated subqueries in SELECT to CTE + LEFT JOIN.
        
        Detects pattern:
          (SELECT AGG(col) FROM table alias WHERE alias.fk = outer.pk) AS name
        
        Consolidates all subqueries hitting the same table into a single CTE
        and replaces inline subqueries with CTE column references.
        """
        query_upper = query.upper()
        
        # Only proceed if there are subqueries in SELECT
        if 'SELECT' not in query_upper or query_upper.count('(') < 3:
            return query
        
        # Safe parsing: Use regex just to find the start, then string parsing
        matches = []
        for start_match in re.finditer(r'\(\s*SELECT\b', query_upper):
            idx = start_match.start()
            
            # Find matching parenthesis
            depth = 1
            close_idx = -1
            for j in range(start_match.end(), len(query_upper)):
                if query_upper[j] == '(':
                    depth += 1
                elif query_upper[j] == ')':
                    depth -= 1
                    if depth == 0:
                        close_idx = j
                        break
            
            if close_idx != -1:
                # Check if it has 'AS alias_name' after it
                post_parenthesis = query_upper[close_idx+1:close_idx+20].strip()
                if post_parenthesis.startswith('AS '):
                    subq_text = query[idx:close_idx+1]
                    # Pure string parsing to extract components
                    subq_text_norm = re.sub(r'\s+', ' ', subq_text)
                    subq_upper = subq_text_norm.upper()
                    if ' FROM ' in subq_upper and ' WHERE ' in subq_upper and '=' in subq_upper:
                        select_pos = subq_upper.find('SELECT')
                        from_pos = subq_upper.find(' FROM ')
                        where_pos = subq_upper.find(' WHERE ')
                        
                        if select_pos < from_pos < where_pos:
                            agg_expr = subq_text_norm[select_pos+6:from_pos].strip()
                            table_section = subq_text_norm[from_pos+6:where_pos].strip().split()
                            
                            if len(table_section) >= 2:
                                table = table_section[0]
                                t_alias = table_section[-1]
                                
                                where_cond = subq_text_norm[where_pos+7 : -1].strip()
                                if '=' in where_cond:
                                    left_part, right_part = where_cond.split('=', 1)
                                    left_part = left_part.strip()
                                    right_part = right_part.strip()
                                    
                                    if '.' in left_part and '.' in right_part:
                                        try:
                                            left_alias, left_col = left_part.split('.')
                                            right_alias, right_col = right_part.split('.')
                                        except ValueError:
                                            continue  # Not a simple a.b = c.d condition
                                            
                                        # Get the alias assigned to this subquery
                                        remaining = query[close_idx+1:].strip()
                                        alias_match = remaining[3:].split()[0].rstrip(', \n\t')  # Skip 'AS '
                                        
                                        # Create pseudo-match object struct
                                        class DummyMatch:
                                            def __init__(self, text):
                                                self.text = text
                                            def group(self, n):
                                                return self.text
                                                
                                        full_replace_text = query[idx : query.find(alias_match, close_idx) + len(alias_match)]
                                        matches.append({
                                            'match': DummyMatch(full_replace_text),
                                            'agg_expr': agg_expr,
                                            'table': table,
                                            'table_alias': t_alias,
                                            'join_col_left': left_col,
                                            'join_col_right_alias': right_alias,
                                            'join_col_right': right_col,
                                            'column_alias': alias_match,
                                        })
            
            
        if len(matches) < 2:  # Need at least 2 subqueries to justify a CTE rewrite
            return query
        
        # Group subqueries by table
        from collections import defaultdict
        table_groups = defaultdict(list)
        for m in matches:
            table_groups[m['table'].lower()].append(m)
        
        if not table_groups:
            return query
        
        # Build CTE(s) and replacement mappings
        cte_parts = []
        replacements = {}  # full_match_text -> replacement_text
        cte_joins = []     # LEFT JOIN clauses to add
        
        for table_name, subqueries in table_groups.items():
            cte_name = f"__{table_name}_agg"
            join_col = subqueries[0]['join_col_left']
            original_table = subqueries[0]['table']
            outer_alias = subqueries[0]['join_col_right_alias']
            outer_col = subqueries[0]['join_col_right']
            
            # Build aggregation columns for the CTE
            agg_cols = []
            for sq in subqueries:
                agg_cols.append(f"    {sq['agg_expr']} AS {sq['column_alias']}")
                # Map the original subquery text -> CTE column reference
                original_text = sq['match'].group(0)
                # Replace with COALESCE to handle NULL from LEFT JOIN
                if 'COUNT' in sq['agg_expr'].upper():
                    replacements[original_text] = f"COALESCE({cte_name}.{sq['column_alias']}, 0) AS {sq['column_alias']}"
                else:
                    replacements[original_text] = f"{cte_name}.{sq['column_alias']} AS {sq['column_alias']}"
            
            agg_cols_joined = ',\n'.join(agg_cols)
            cte_sql = (
                f"{cte_name} AS (\n"
                f"  SELECT {join_col},\n"
                f"{agg_cols_joined}\n"
                f"  FROM {original_table}\n"
                f"  GROUP BY {join_col}\n"
                f")"
            )
            cte_parts.append(cte_sql)
            cte_joins.append(
                f"LEFT JOIN {cte_name} ON {outer_alias}.{outer_col} = {cte_name}.{join_col}"
            )
        
        # Apply replacements to the query
        result = query
        for old_text, new_text in replacements.items():
            result = result.replace(old_text, new_text)
        
        # Add CTE at the beginning
        cte_header = "WITH " + ",\n".join(cte_parts)
        
        # Check if query already has WITH
        with_match = re.match(r'^\s*WITH\b', result, re.IGNORECASE)
        if with_match:
            # Prepend to existing WITH
            result = re.sub(r'^(\s*WITH)\b', f'{cte_header},\n', result, count=1, flags=re.IGNORECASE)
        else:
            # Add WITH before SELECT
            result = re.sub(r'^(\s*SELECT)\b', f'{cte_header}\nSELECT', result, count=1, flags=re.IGNORECASE)
        
        # Add LEFT JOIN(s) before WHERE
        join_clause = "\n".join(cte_joins)
        result = re.sub(
            r'(\bWHERE\b)',
            f"{join_clause}\nWHERE",
            result, count=1, flags=re.IGNORECASE
        )
        
        return result

    def create_optimized_query(self, query: str, suggestions: List, aggressive: bool = False) -> str:
        """Create an optimized version of the query"""
        optimized = self.apply_suggestions(query, suggestions)
        if aggressive:
            optimized = self.rewrite_correlated_subqueries(optimized)
            optimized = self.cap_recursion_depth(optimized, max_depth=3)
            optimized = self.reduce_cross_join_range(optimized, target_rows=500)
            optimized = self.remove_percentile_cont(optimized)
            # Only inject limits if it's NOT already recursive with a limit (avoiding syntax errors)
            if 'WITH RECURSIVE' not in optimized.upper():
                optimized = self.inject_cte_limits(optimized, limit=500)
        return optimized
