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
        
        # Keywords that mark data-critical SQL clauses which must never be
        # replaced with comments — doing so would silently change the result set.
        _PROTECTED_KEYWORDS = ('WHERE', 'AND ', 'AND(', 'OR ', 'OR(', 
                               'HAVING', 'FROM ', 'JOIN ', 'ON ')
        
        for suggestion in sorted_suggestions:
            if 0 < suggestion.line_number <= len(lines):
                idx = suggestion.line_number - 1
                original = lines[idx].strip()
                
                # Safety: never replace data-critical lines with comments
                if suggestion.suggested_content.lstrip().startswith('--'):
                    original_upper = original.upper().lstrip()
                    if any(original_upper.startswith(kw) for kw in _PROTECTED_KEYWORDS):
                        continue
                
                if self._lines_match(original, suggestion.original_content):
                    indent = len(lines[idx]) - len(lines[idx].lstrip())
                    lines[idx] = ' ' * indent + suggestion.suggested_content
        
        return '\n'.join(lines)
    
    def _lines_match(self, line1: str, line2: str) -> bool:
        """Check if two lines match (ignoring whitespace differences)"""
        normalize = lambda s: ' '.join(s.split())
        return normalize(line1) == normalize(line2)
    

    def rewrite_correlated_subqueries(self, query: str) -> str:
        """Rewrite correlated subqueries in SELECT to CTE + LEFT JOIN.
        
        Detects pattern:
          (SELECT AGG(col) FROM table alias WHERE alias.fk = outer.pk) AS name
        
        Consolidates all subqueries hitting the same table into a single CTE
        and replaces inline subqueries with CTE column references.
        
        SAFETY: Only rewrites *simple* correlated subqueries whose WHERE clause
        is exactly ``alias.col = outer.col``.  Subqueries with AND/OR, nested
        SELECTs, window functions (OVER), or LIMIT are skipped to avoid
        producing incorrect SQL.
        """
        query_upper = query.upper()
        
        # Only proceed if there are subqueries in SELECT
        if 'SELECT' not in query_upper or query_upper.count('(') < 1:
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

                                # ── SAFETY GUARDS ──
                                # Skip subqueries with complex WHERE clauses that
                                # cannot be safely decomposed into a single CTE join.
                                where_cond_upper = where_cond.upper()

                                # Guard 1: multi-condition WHERE (AND / OR)
                                if re.search(r'\bAND\b|\bOR\b', where_cond_upper):
                                    continue

                                # Guard 2: nested SELECT inside WHERE
                                if 'SELECT' in where_cond_upper:
                                    continue

                                # Guard 3: window functions in the aggregate expression
                                if 'OVER' in subq_upper:
                                    continue

                                # Guard 4: LIMIT inside the subquery
                                if 'LIMIT' in subq_upper:
                                    continue

                                # Guard 5: type casts (::) in the join condition
                                if '::' in where_cond:
                                    continue

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

                                        # Guard 6: aliases must be simple identifiers
                                        if not (left_alias.strip().isidentifier() and left_col.strip().isidentifier()
                                                and right_alias.strip().isidentifier() and right_col.strip().isidentifier()):
                                            continue

                                        left_col = left_col.strip()
                                        right_alias = right_alias.strip()
                                        right_col = right_col.strip()

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
        
        # Check if query already has WITH by looking at the first word
        # Using lstrip() is more robust than regex for leading whitespace
        lstripped = result.lstrip()
        lstripped_upper = lstripped.upper()
        
        if lstripped_upper.startswith('WITH '):
            # Prepend to existing WITH by finding the WITH keyword
            with_pos = result.upper().find('WITH ')
            result = result[:with_pos+5] + cte_header[5:] + ",\n" + result[with_pos+5:]
        elif lstripped_upper.startswith('SELECT'):
             # Prepend before SELECT
             select_pos = result.upper().find('SELECT')
             result = result[:select_pos] + cte_header + "\n" + result[select_pos:]
        else:
             # Fallback: Just prepend it
             result = cte_header + "\n" + result
        
        # Add LEFT JOIN(s) before WHERE
        join_clause = "\n".join(cte_joins)
        result = re.sub(
            r'(\bWHERE\b)',
            f"{join_clause}\nWHERE",
            result, count=1, flags=re.IGNORECASE
        )
        
        return result

    def rewrite_self_join_to_window(self, query: str) -> str:
        """Rewrite self-joins on the same CTE/table into window-function based pre-aggregation.
        
        Detects pattern:
            FROM cte_name c1 JOIN cte_name c2 ON c1.key = c2.key AND c1.pk != c2.pk
        
        Replaces with a pre-aggregation CTE using window functions, then arithmetic
        on the pre-aggregated columns to avoid the O(N²) cartesian product.
        """
        # Find self-join pattern: FROM <name> <a1> JOIN <name> <a2> ON <a1>.<key> = <a2>.<key> AND <a1>.<pk> != <a2>.<pk>
        self_join_pattern = re.compile(
            r'FROM\s+(\w+)\s+(\w+)\s+JOIN\s+(\w+)\s+(\w+)\s+ON\s+'
            r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)'
            r'\s+AND\s+(\w+)\.(\w+)\s*!=\s*(\w+)\.(\w+)',
            re.IGNORECASE | re.DOTALL
        )
        
        m = self_join_pattern.search(query)
        if not m:
            return query
        
        tbl1, alias1, tbl2, alias2 = m.group(1), m.group(2), m.group(3), m.group(4)
        
        # Must be a self-join (same table/CTE name)
        if tbl1.lower() != tbl2.lower():
            return query
        
        join_key_col = m.group(6)  # The column they join on (e.g. chain_depth)
        pk_col = m.group(10)       # The column with != (e.g. root_aid)
        
        # Find the SELECT clause for this self-join query
        # We need to identify which columns from c1/c2 are used in the SELECT
        select_match = re.search(
            r'SELECT\s+(.*?)\s+FROM\s+' + re.escape(tbl1) + r'\s+' + re.escape(alias1),
            query, re.IGNORECASE | re.DOTALL
        )
        if not select_match:
            return query
        
        select_clause = select_match.group(1)
        
        # Identify all columns referenced from alias1 and alias2
        col_refs_1 = set(re.findall(re.escape(alias1) + r'\.(\w+)', select_clause, re.IGNORECASE))
        col_refs_2 = set(re.findall(re.escape(alias2) + r'\.(\w+)', select_clause, re.IGNORECASE))
        
        # Build a pre-aggregated CTE that computes per-group sums/counts using window functions
        # All columns from the original CTE plus group-level aggregates
        all_cols = col_refs_1 | col_refs_2
        all_cols.discard(join_key_col)  # Don't aggregate the join key
        all_cols.discard(pk_col)        # Don't aggregate the PK
        
        # Build window aggregate columns
        window_agg_cols = []
        for col in sorted(all_cols):
            window_agg_cols.append(f"    SUM({col}) OVER (PARTITION BY {join_key_col}) AS group_total_{col}")
            window_agg_cols.append(f"    COUNT(*) OVER (PARTITION BY {join_key_col}) AS group_count_{join_key_col}")
        
        # Deduplicate
        window_agg_cols = list(dict.fromkeys(window_agg_cols))
        
        # Get all base columns from the original CTE used in SELECT
        base_cols = col_refs_1 | {join_key_col, pk_col}
        base_col_list = ", ".join(sorted(base_cols))
        window_col_list = ",\n".join(window_agg_cols)
        
        pre_agg_cte_name = f"__{tbl1.lower()}_preagg"
        pre_agg_cte = (
            f"{pre_agg_cte_name} AS (\n"
            f"  SELECT {base_col_list},\n"
            f"{window_col_list}\n"
            f"  FROM {tbl1}\n"
            f")"
        )
        
        # Rewrite the SELECT clause to use pre-aggregated columns
        new_select = select_clause
        for col in sorted(col_refs_2):
            # Replace alias2.col with (group_total_col - alias1.col) for SUM-like expressions
            # or (group_count - 1) for COUNT-like expressions  
            pattern_sum = re.compile(
                re.escape(alias1) + r'\.' + re.escape(col) + r'\s*\+\s*' + re.escape(alias2) + r'\.' + re.escape(col),
                re.IGNORECASE
            )
            if pattern_sum.search(new_select):
                # c1.col + c2.col => p.col + (p.group_total_col - p.col) = p.group_total_col
                new_select = pattern_sum.sub(f"p.group_total_{col}", new_select)
                continue
            
            pattern_mul = re.compile(
                re.escape(alias1) + r'\.' + re.escape(col) + r'\s*\*\s*' + re.escape(alias2) + r'\.' + re.escape(col),
                re.IGNORECASE
            )
            if pattern_mul.search(new_select):
                # c1.col * c2.col => p.col * (p.group_total_col - p.col)
                new_select = pattern_mul.sub(
                    f"p.{col} * (p.group_total_{col} - p.{col})", new_select
                )
                continue
                
            # POWER(c1.col - c2.col, 2) pattern
            power_pattern = re.compile(
                r'POWER\s*\(\s*' + re.escape(alias1) + r'\.' + re.escape(col) + r'\s*-\s*'
                + re.escape(alias2) + r'\.(\w+)\s*,\s*2\s*\)',
                re.IGNORECASE
            )
            power_m = power_pattern.search(new_select)
            if power_m:
                col2 = power_m.group(1)
                # For variance-like: POWER(c1.x - c2.y, 2) summed over all c2
                # This requires a different approach - keep as a note
                new_select = power_pattern.sub(
                    f"POWER(p.{col} - (p.group_total_{col2} - p.{col2}), 2)", new_select
                )
                continue
                
            # Simple alias2.col reference => (group_total_col - col)
            simple_pattern = re.compile(re.escape(alias2) + r'\.' + re.escape(col), re.IGNORECASE)
            new_select = simple_pattern.sub(f"(p.group_total_{col} - p.{col})", new_select)
        
        # Replace alias1.col with p.col
        for col in sorted(col_refs_1):
            new_select = re.sub(
                re.escape(alias1) + r'\.' + re.escape(col),
                f"p.{col}", new_select, flags=re.IGNORECASE
            )
        
        # Replace the self-join FROM clause with the pre-agg CTE
        # Find everything from the self-join SELECT to the end
        full_self_join_block = query[select_match.start():]
        
        # Build the new query block
        new_from = f"FROM {pre_agg_cte_name} p"
        
        # Replace the FROM...JOIN block
        from_join_end = m.end()
        new_query = query[:select_match.start()]
        new_query += f"SELECT {new_select}\n{new_from}\n"
        
        # Get the rest after the JOIN ON clause (WHERE, ORDER BY, LIMIT, etc.)
        rest = query[from_join_end:]
        
        # Clean up alias references in the rest (WHERE, ORDER BY)
        rest = re.sub(re.escape(alias1) + r'\.', 'p.', rest, flags=re.IGNORECASE)
        rest = re.sub(re.escape(alias2) + r'\.', 'p.', rest, flags=re.IGNORECASE)
        new_query += rest
        
        # Insert the pre-agg CTE
        # Find the last CTE definition to insert after it
        last_paren = query[:select_match.start()].rfind(')')
        if last_paren != -1:
            new_query = query[:last_paren + 1] + f",\n{pre_agg_cte}\n" + new_query[len(query[:select_match.start()]):]
        
        return new_query

    def push_filters_into_cte(self, query: str) -> str:
        """Move qualifying WHERE filters from an outer query into a CTE definition.
        
        Uses balanced parenthesis counting to safely extract CTE bodies (avoids
        regex catastrophic backtracking on nested CASE/WHEN expressions).
        Only pushes filters on aggregated columns (those produced by GROUP BY).
        """
        result = query
        
        # Step 1: Find all CTE definitions by name using balanced-paren parsing
        cte_name_pattern = re.compile(
            r'(\w+)\s+AS\s+(?:MATERIALIZED\s+)?\(', re.IGNORECASE
        )
        
        ctes = {}  # name -> (body_start, body_end, body_text)
        for m in cte_name_pattern.finditer(query):
            cte_name = m.group(1)
            # Skip SQL keywords that look like CTE names
            if cte_name.upper() in ('SELECT', 'WITH', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'RECURSIVE'):
                continue
            
            open_paren = m.end() - 1  # position of the opening '('
            depth = 1
            pos = open_paren + 1
            while pos < len(query) and depth > 0:
                if query[pos] == '(':
                    depth += 1
                elif query[pos] == ')':
                    depth -= 1
                pos += 1
            
            if depth == 0:
                body = query[open_paren + 1 : pos - 1].strip()
                ctes[cte_name] = (open_paren + 1, pos - 1, body)
        
        # Step 2: Find the LAST outer WHERE clause (after all CTEs)
        # The outer query's WHERE is the one after the final CTE
        last_cte_end = max((end for _, end, _ in ctes.values()), default=0) if ctes else 0
        outer_part = query[last_cte_end:]
        
        outer_where = re.search(
            r'\bWHERE\s+(.*?)(?:\bORDER\s+BY\b|\bLIMIT\b|$)',
            outer_part, re.IGNORECASE | re.DOTALL
        )
        if not outer_where:
            return result
        
        where_text = outer_where.group(1).strip()
        
        # Step 3: Find filter conditions like alias.column > N
        cond_pattern = re.compile(
            r'(\w+)\.(\w+)\s*(>|<|>=|<=|=|!=)\s*(\d+)', re.IGNORECASE
        )
        
        for cond_m in cond_pattern.finditer(where_text):
            alias, col, op, val = cond_m.group(1), cond_m.group(2), cond_m.group(3), cond_m.group(4)
            
            # Step 4: Find which CTE this alias maps to
            # Look for FROM cte_name alias in the outer query
            for cte_name, (body_start, body_end, body) in ctes.items():
                alias_usage = re.search(
                    r'\bFROM\s+' + re.escape(cte_name) + r'\s+' + re.escape(alias) + r'\b',
                    outer_part, re.IGNORECASE
                )
                if not alias_usage:
                    continue
                
                body_upper = body.upper()
                
                # Only act on CTEs with GROUP BY (aggregation CTEs)
                if 'GROUP BY' not in body_upper:
                    continue
                
                # Only push if the column is an aggregate alias (appears in SELECT as "AGG(...) as col")
                # Check if col appears as an alias: "as col" or "AS col"
                if not re.search(r'\bAS\s+' + re.escape(col) + r'\b', body, re.IGNORECASE):
                    continue
                
                # Step 4.5: Find the actual expression to avoid using alias in HAVING (Postgres forbids it)
                expr = col
                expr_match = re.search(r'(?:SELECT|,)\s*([^,]+?)(?:\s+AS\s+|\s+)' + re.escape(col) + r'\b', body, re.IGNORECASE | re.DOTALL)
                if expr_match:
                    expr = expr_match.group(1).strip()
                
                # Step 5: Add HAVING clause to the CTE
                # Use [\w\.]+ instead of \S+ to prevent catastrophic backtracking
                group_by_match = re.search(r'(GROUP\s+BY\s+[\w\.]+(?:\s*,\s*[\w\.]+)*)', body, re.IGNORECASE)
                if not group_by_match:
                    continue
                
                group_by_end = group_by_match.end()
                
                if re.search(r'\bHAVING\b', body, re.IGNORECASE):
                    # Append to existing HAVING
                    having_match = re.search(r'(\bHAVING\b\s*)', body, re.IGNORECASE)
                    if having_match:
                        insert_pos = having_match.end()
                        new_body = body[:insert_pos] + f"{expr} {op} {val} AND " + body[insert_pos:]
                else:
                    # Insert HAVING after GROUP BY
                    new_body = body[:group_by_end] + f"\n    HAVING {expr} {op} {val}" + body[group_by_end:]
                
                # Replace in result
                result = result[:body_start] + new_body + result[body_end:]
                # Adjust positions for subsequent replacements
                offset = len(new_body) - len(body)
                # Update the body in our dict
                ctes[cte_name] = (body_start, body_end + offset, new_body)
                break
        
        return result

    def create_optimized_query(self, query: str, suggestions: List, aggressive: bool = False) -> str:
        """Create an optimized version of the query"""
        optimized = self.apply_suggestions(query, suggestions)
        # Structural optimizations (always equivalent)
        optimized = self.rewrite_correlated_subqueries(optimized)
        optimized = self.push_filters_into_cte(optimized)
        # NOTE: rewrite_self_join_to_window is NOT called here because it changes
        # semantics for pairwise queries. The LLM handles self-join rewrites when safe.
        return optimized

