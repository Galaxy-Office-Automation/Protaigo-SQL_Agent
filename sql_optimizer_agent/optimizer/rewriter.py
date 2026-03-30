"""
Query Rewriter - Applies optimization suggestions to rewrite queries

BUG 3 FIX (Guard 1 — line 111):
  The old Guard 1 blocked ANY correlated subquery whose WHERE had AND/OR.
  This meant queries like:
    WHERE h.aid = a.aid AND DATE(h.mtime) >= CURRENT_DATE - INTERVAL '90 days'
  were never rewritten, even though the extra condition is a safe range filter.
  Fix: Guard 1 now only blocks AND/OR conditions where the extra term itself
  contains a correlated reference to the outer query (alias.col pattern).
  A plain date/range filter with no outer alias is allowed through.

BUG 4 FIX (WHERE injection — line 271):
  The old code used re.sub(r'WHERE', ..., count=1) which matched the FIRST
  WHERE in the string — often one inside a subquery body — and injected the
  LEFT JOIN clause there, producing invalid SQL.
  Fix: _find_outer_where() walks the string tracking parenthesis depth and
  returns only the position of a WHERE at depth 0 (outer query scope).
"""

from typing import List
import re


class QueryRewriter:
    """Rewrites SQL queries based on optimization suggestions"""

    def apply_suggestions(self, query: str, suggestions: List) -> str:
        """Apply multiple optimization suggestions to a query"""
        lines = query.split('\n')
        sorted_suggestions = sorted(suggestions, key=lambda s: s.line_number, reverse=True)

        _PROTECTED_KEYWORDS = ('WHERE', 'AND ', 'AND(', 'OR ', 'OR(',
                               'HAVING', 'FROM ', 'JOIN ', 'ON ')

        for suggestion in sorted_suggestions:
            if 0 < suggestion.line_number <= len(lines):
                idx = suggestion.line_number - 1
                original = lines[idx].strip()

                sc = suggestion.suggested_content

                # Never inject a SQL comment into the query body
                if sc.lstrip().startswith('--'):
                    continue

                # Never replace data-critical lines with empty or comment content
                original_upper = original.upper().lstrip()
                if any(original_upper.startswith(kw) for kw in _PROTECTED_KEYWORDS):
                    if not sc.strip() or sc.lstrip().startswith('--'):
                        continue

                if self._lines_match(original, suggestion.original_content):
                    indent = len(lines[idx]) - len(lines[idx].lstrip())
                    if sc.strip() == "":
                        lines[idx] = None
                    else:
                        lines[idx] = ' ' * indent + sc

        return '\n'.join(line for line in lines if line is not None)

    def _lines_match(self, line1: str, line2: str) -> bool:
        """Check if two lines match (ignoring whitespace differences)"""
        normalize = lambda s: ' '.join(s.split())
        return normalize(line1) == normalize(line2)

    def _find_outer_where(self, query: str) -> int:
        """
        BUG 4 FIX: Find the position of the OUTER WHERE clause only.

        The old code used re.sub(r'WHERE', ..., count=1) which matched the
        first WHERE encountered — usually one inside a subquery — and injected
        the LEFT JOIN there instead of at the outer query level.

        This method walks the query character by character, tracking parenthesis
        depth. A WHERE keyword at depth 0 belongs to the outer query.
        Returns the character position, or -1 if not found.
        """
        depth = 0
        i     = 0
        qu    = query.upper()
        while i < len(qu):
            ch = qu[i]
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            elif ch == 'W' and depth == 0:
                if qu[i:i+5] == 'WHERE':
                    # Confirm it is a word boundary (not part of an identifier)
                    before = qu[i-1] if i > 0 else ' '
                    after  = qu[i+5] if i+5 < len(qu) else ' '
                    if not before.isalnum() and before != '_' \
                       and not after.isalnum() and after != '_':
                        return i
            i += 1
        return -1

    def _is_correlated_condition(self, condition: str, outer_aliases: set,
                                  subquery_alias: str = "") -> bool:
        """
        Return True if 'condition' contains a reference to an outer-query alias.
        Used by the relaxed Guard 1 to decide whether an AND clause is safe.

        A condition is correlated if it references alias.column where alias is
        one of the known outer-query table aliases AND is not the subquery's
        own internal alias (subquery_alias).

        The subquery_alias exclusion is critical: a condition like
          DATE(h1.mtime) >= CURRENT_DATE - INTERVAL '90 days'
        references h1 which is the subquery's own FROM alias, not an outer
        table. Without the exclusion, h1 would be treated as correlated and
        the safe date-range filter would incorrectly block the rewrite.
        """
        refs = re.findall(r'\b(\w+)\.\w+', condition)
        sq_alias_lower = subquery_alias.lower()
        return any(
            ref.lower() in outer_aliases and ref.lower() != sq_alias_lower
            for ref in refs
        )

    def rewrite_correlated_subqueries(self, query: str) -> str:
        """Rewrite correlated subqueries in SELECT to CTE + LEFT JOIN.

        Detects pattern:
          (SELECT AGG(col) FROM table alias WHERE alias.fk = outer.pk [...]) AS name

        Consolidates all subqueries hitting the same table into a single CTE
        and replaces inline subqueries with CTE column references.

        Safety guards skip subqueries that cannot be safely rewritten.
        """
        query_upper = query.upper()

        if 'SELECT' not in query_upper or query_upper.count('(') < 1:
            return query

        # Collect outer-query table aliases so we can detect correlated AND terms
        # Pattern: FROM/JOIN table alias  or  FROM/JOIN table AS alias
        outer_aliases = set()
        for m in re.finditer(
            r'\b(?:FROM|JOIN)\s+(\w+)\s+(?:AS\s+)?(\w+)',
            query, re.IGNORECASE
        ):
            outer_aliases.add(m.group(2).lower())

        matches = []
        for start_match in re.finditer(r'\(\s*SELECT\b', query_upper):
            idx = start_match.start()

            depth     = 1
            close_idx = -1
            for j in range(start_match.end(), len(query_upper)):
                if query_upper[j] == '(':
                    depth += 1
                elif query_upper[j] == ')':
                    depth -= 1
                    if depth == 0:
                        close_idx = j
                        break

            if close_idx == -1:
                continue

            post_parenthesis = query_upper[close_idx+1:close_idx+20].strip()
            if not post_parenthesis.startswith('AS '):
                continue

            subq_text      = query[idx:close_idx+1]
            subq_text_norm = re.sub(r'\s+', ' ', subq_text)
            subq_upper     = subq_text_norm.upper()

            if not (' FROM ' in subq_upper and ' WHERE ' in subq_upper and '=' in subq_upper):
                continue

            select_pos = subq_upper.find('SELECT')
            from_pos   = subq_upper.find(' FROM ')
            where_pos  = subq_upper.find(' WHERE ')

            if not (select_pos < from_pos < where_pos):
                continue

            agg_expr      = subq_text_norm[select_pos+6:from_pos].strip()
            table_section = subq_text_norm[from_pos+6:where_pos].strip().split()

            if len(table_section) < 2:
                continue

            table   = table_section[0]
            t_alias = table_section[-1]

            # Full WHERE content after the WHERE keyword (minus closing paren)
            full_where = subq_text_norm[where_pos+7:-1].strip()
            full_where_upper = full_where.upper()

            # ── SAFETY GUARDS ──────────────────────────────────────────────

            # Guard 2: nested SELECT inside WHERE
            if 'SELECT' in full_where_upper:
                continue

            # Guard 3: window functions in the aggregate expression
            if 'OVER' in subq_upper:
                continue

            # Guard 4: LIMIT inside the subquery
            if 'LIMIT' in subq_upper:
                continue

            # Guard 5: type casts (::) in the full where clause
            if '::' in full_where:
                continue

            # Extract the correlated join condition: the FIRST condition
            # (before any AND) that looks like alias.col = outer.col
            # BUG 3 FIX: Guard 1 previously rejected ANY subquery with AND in
            # its WHERE, which blocked queries like:
            #   WHERE h.aid = a.aid AND DATE(h.mtime) >= CURRENT_DATE - INTERVAL '90 days'
            # Now we split on AND, take the first condition as the join key,
            # and only reject if one of the extra conditions is itself correlated
            # (references an outer alias) — because that would require per-row
            # evaluation that a simple CTE join cannot replicate.
            if re.search(r'\bAND\b|\bOR\b', full_where_upper):
                # Split on AND (we don't support OR rewrites)
                if re.search(r'\bOR\b', full_where_upper):
                    continue  # OR conditions are genuinely unsafe

                parts = re.split(r'\bAND\b', full_where, flags=re.IGNORECASE)
                # The correlated part must be exactly alias.col = outer.col
                corr_part = parts[0].strip()
                extra_parts = parts[1:]

                # Reject if any extra part is itself correlated to the outer query.
                # Pass t_alias so the subquery's own FROM alias is not mistaken
                # for an outer reference (e.g. DATE(h1.mtime) uses h1 internally).
                for ep in extra_parts:
                    if self._is_correlated_condition(ep, outer_aliases, subquery_alias=t_alias):
                        corr_part = None
                        break

                if corr_part is None:
                    continue

                where_cond = corr_part

                # Preserve the extra (non-correlated) filter conditions so they
                # are included in the CTE WHERE clause for correct pre-filtering
                extra_filter = " AND ".join(p.strip() for p in extra_parts)
            else:
                where_cond   = full_where
                extra_filter = ""

            where_cond_upper = where_cond.upper()

            if '=' not in where_cond:
                continue

            left_part, right_part = where_cond.split('=', 1)
            left_part  = left_part.strip()
            right_part = right_part.strip()

            if '.' not in left_part or '.' not in right_part:
                continue

            try:
                left_alias, left_col   = left_part.split('.')
                right_alias, right_col = right_part.split('.')
            except ValueError:
                continue

            if not (left_alias.strip().isidentifier() and left_col.strip().isidentifier()
                    and right_alias.strip().isidentifier() and right_col.strip().isidentifier()):
                continue

            left_col    = left_col.strip()
            right_alias = right_alias.strip()
            right_col   = right_col.strip()

            remaining   = query[close_idx+1:].strip()
            alias_match = remaining[3:].split()[0].rstrip(', \n\t')

            class DummyMatch:
                def __init__(self, text):
                    self.text = text
                def group(self, n):
                    return self.text

            full_replace_text = query[idx : query.find(alias_match, close_idx) + len(alias_match)]
            matches.append({
                'match':                DummyMatch(full_replace_text),
                'agg_expr':             agg_expr,
                'table':                table,
                'table_alias':          t_alias,
                'join_col_left':        left_col,
                'join_col_right_alias': right_alias,
                'join_col_right':       right_col,
                'column_alias':         alias_match,
                'extra_filter':         extra_filter,  # date/range conditions for CTE
            })

        if len(matches) < 2:
            return query

        from collections import defaultdict
        table_groups = defaultdict(list)
        for m in matches:
            table_groups[m['table'].lower()].append(m)

        if not table_groups:
            return query

        cte_parts    = []
        replacements = {}
        cte_joins    = []

        for table_name, subqueries in table_groups.items():
            cte_name       = f"__{table_name}_agg"
            join_col       = subqueries[0]['join_col_left']
            original_table = subqueries[0]['table']
            outer_alias    = subqueries[0]['join_col_right_alias']
            outer_col      = subqueries[0]['join_col_right']

            # Collect the extra filter from the subqueries (e.g. date range)
            # All subqueries on the same table should share the same extra filter
            extra_filter = subqueries[0].get('extra_filter', '')

            agg_cols = []
            for sq in subqueries:
                agg_cols.append(f"    {sq['agg_expr']} AS {sq['column_alias']}")
                original_text = sq['match'].group(0)
                if 'COUNT' in sq['agg_expr'].upper():
                    replacements[original_text] = f"COALESCE({cte_name}.{sq['column_alias']}, 0) AS {sq['column_alias']}"
                else:
                    replacements[original_text] = f"{cte_name}.{sq['column_alias']} AS {sq['column_alias']}"

            agg_cols_joined = ',\n'.join(agg_cols)

            # Build the CTE WHERE clause: include extra filter if present
            cte_where = f"  WHERE {extra_filter}" if extra_filter else ""

            cte_sql = (
                f"{cte_name} AS (\n"
                f"  SELECT {join_col},\n"
                f"{agg_cols_joined}\n"
                f"  FROM {original_table}\n"
                f"{cte_where}\n"
                f"  GROUP BY {join_col}\n"
                f")"
            )
            cte_parts.append(cte_sql)
            cte_joins.append(
                f"LEFT JOIN {cte_name} ON {outer_alias}.{outer_col} = {cte_name}.{join_col}"
            )

        result = query
        for old_text, new_text in replacements.items():
            result = result.replace(old_text, new_text)

        cte_header      = "WITH " + ",\n".join(cte_parts)
        lstripped       = result.lstrip()
        lstripped_upper = lstripped.upper()

        if lstripped_upper.startswith('WITH '):
            with_pos = result.upper().find('WITH ')
            result = result[:with_pos+5] + cte_header[5:] + ",\n" + result[with_pos+5:]
        elif lstripped_upper.startswith('SELECT'):
            select_pos = result.upper().find('SELECT')
            result = result[:select_pos] + cte_header + "\n" + result[select_pos:]
        else:
            result = cte_header + "\n" + result

        # BUG 4 FIX: inject LEFT JOINs before the OUTER WHERE only.
        # Old code: re.sub(r'WHERE', ..., count=1) matched first WHERE anywhere
        # — usually inside a subquery body — and broke the SQL.
        join_clause     = "\n".join(cte_joins)
        outer_where_pos = self._find_outer_where(result)

        if outer_where_pos != -1:
            result = (
                result[:outer_where_pos]
                + join_clause + "\n"
                + result[outer_where_pos:]
            )
        else:
            # No outer WHERE: append before ORDER BY, or at end of FROM block
            order_match = re.search(r'\bORDER\s+BY\b', result, re.IGNORECASE)
            limit_match = re.search(r'\bLIMIT\b',      result, re.IGNORECASE)
            anchor = order_match or limit_match
            if anchor:
                result = result[:anchor.start()] + join_clause + "\n" + result[anchor.start():]
            else:
                result = result.rstrip() + "\n" + join_clause

        return result

    def rewrite_self_join_to_window(self, query: str) -> str:
        """Rewrite self-joins on the same CTE/table into window-function based pre-aggregation."""
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

        if tbl1.lower() != tbl2.lower():
            return query

        join_key_col = m.group(6)
        pk_col       = m.group(10)

        select_match = re.search(
            r'SELECT\s+(.*?)\s+FROM\s+' + re.escape(tbl1) + r'\s+' + re.escape(alias1),
            query, re.IGNORECASE | re.DOTALL
        )
        if not select_match:
            return query

        select_clause = select_match.group(1)
        col_refs_1 = set(re.findall(re.escape(alias1) + r'\.(\w+)', select_clause, re.IGNORECASE))
        col_refs_2 = set(re.findall(re.escape(alias2) + r'\.(\w+)', select_clause, re.IGNORECASE))

        all_cols = col_refs_1 | col_refs_2
        all_cols.discard(join_key_col)
        all_cols.discard(pk_col)

        window_agg_cols = []
        for col in sorted(all_cols):
            window_agg_cols.append(f"    SUM({col}) OVER (PARTITION BY {join_key_col}) AS group_total_{col}")
            window_agg_cols.append(f"    COUNT(*) OVER (PARTITION BY {join_key_col}) AS group_count_{join_key_col}")
        window_agg_cols = list(dict.fromkeys(window_agg_cols))

        base_cols       = col_refs_1 | {join_key_col, pk_col}
        base_col_list   = ", ".join(sorted(base_cols))
        window_col_list = ",\n".join(window_agg_cols)

        pre_agg_cte_name = f"__{tbl1.lower()}_preagg"
        pre_agg_cte = (
            f"{pre_agg_cte_name} AS (\n"
            f"  SELECT {base_col_list},\n"
            f"{window_col_list}\n"
            f"  FROM {tbl1}\n"
            f")"
        )

        new_select = select_clause
        for col in sorted(col_refs_2):
            pattern_sum = re.compile(
                re.escape(alias1) + r'\.' + re.escape(col) + r'\s*\+\s*' + re.escape(alias2) + r'\.' + re.escape(col),
                re.IGNORECASE
            )
            if pattern_sum.search(new_select):
                new_select = pattern_sum.sub(f"p.group_total_{col}", new_select)
                continue

            pattern_mul = re.compile(
                re.escape(alias1) + r'\.' + re.escape(col) + r'\s*\*\s*' + re.escape(alias2) + r'\.' + re.escape(col),
                re.IGNORECASE
            )
            if pattern_mul.search(new_select):
                new_select = pattern_mul.sub(f"p.{col} * (p.group_total_{col} - p.{col})", new_select)
                continue

            power_pattern = re.compile(
                r'POWER\s*\(\s*' + re.escape(alias1) + r'\.' + re.escape(col) + r'\s*-\s*'
                + re.escape(alias2) + r'\.(\w+)\s*,\s*2\s*\)',
                re.IGNORECASE
            )
            power_m = power_pattern.search(new_select)
            if power_m:
                col2 = power_m.group(1)
                new_select = power_pattern.sub(
                    f"POWER(p.{col} - (p.group_total_{col2} - p.{col2}), 2)", new_select
                )
                continue

            simple_pattern = re.compile(re.escape(alias2) + r'\.' + re.escape(col), re.IGNORECASE)
            new_select = simple_pattern.sub(f"(p.group_total_{col} - p.{col})", new_select)

        for col in sorted(col_refs_1):
            new_select = re.sub(
                re.escape(alias1) + r'\.' + re.escape(col),
                f"p.{col}", new_select, flags=re.IGNORECASE
            )

        new_from      = f"FROM {pre_agg_cte_name} p"
        from_join_end = m.end()
        new_query     = query[:select_match.start()]
        new_query    += f"SELECT {new_select}\n{new_from}\n"

        rest = query[from_join_end:]
        rest = re.sub(re.escape(alias1) + r'\.', 'p.', rest, flags=re.IGNORECASE)
        rest = re.sub(re.escape(alias2) + r'\.', 'p.', rest, flags=re.IGNORECASE)
        new_query += rest

        last_paren = query[:select_match.start()].rfind(')')
        if last_paren != -1:
            new_query = (
                query[:last_paren + 1]
                + f",\n{pre_agg_cte}\n"
                + new_query[len(query[:select_match.start()]):]
            )

        return new_query

    def push_filters_into_cte(self, query: str) -> str:
        """Move qualifying WHERE filters from an outer query into a CTE definition."""
        result = query

        cte_name_pattern = re.compile(r'(\w+)\s+AS\s+(?:MATERIALIZED\s+)?\(', re.IGNORECASE)
        ctes = {}
        for m in cte_name_pattern.finditer(query):
            cte_name = m.group(1)
            if cte_name.upper() in ('SELECT', 'WITH', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'RECURSIVE'):
                continue
            open_paren = m.end() - 1
            depth = 1
            pos   = open_paren + 1
            while pos < len(query) and depth > 0:
                if query[pos] == '(':
                    depth += 1
                elif query[pos] == ')':
                    depth -= 1
                pos += 1
            if depth == 0:
                body = query[open_paren + 1 : pos - 1].strip()
                ctes[cte_name] = (open_paren + 1, pos - 1, body)

        last_cte_end = max((end for _, end, _ in ctes.values()), default=0) if ctes else 0
        outer_part   = query[last_cte_end:]

        outer_where = re.search(
            r'\bWHERE\s+(.*?)(?:\bORDER\s+BY\b|\bLIMIT\b|$)',
            outer_part, re.IGNORECASE | re.DOTALL
        )
        if not outer_where:
            return result

        where_text   = outer_where.group(1).strip()
        cond_pattern = re.compile(r'(\w+)\.(\w+)\s*(>|<|>=|<=|=|!=)\s*(\d+)', re.IGNORECASE)

        for cond_m in cond_pattern.finditer(where_text):
            alias, col, op, val = cond_m.group(1), cond_m.group(2), cond_m.group(3), cond_m.group(4)

            for cte_name, (body_start, body_end, body) in ctes.items():
                alias_usage = re.search(
                    r'\bFROM\s+' + re.escape(cte_name) + r'\s+' + re.escape(alias) + r'\b',
                    outer_part, re.IGNORECASE
                )
                if not alias_usage:
                    continue

                body_upper = body.upper()
                if 'GROUP BY' not in body_upper:
                    continue
                if not re.search(r'\bAS\s+' + re.escape(col) + r'\b', body, re.IGNORECASE):
                    continue

                expr = col
                expr_match = re.search(
                    r'(?:SELECT|,)\s*([^,]+?)(?:\s+AS\s+|\s+)' + re.escape(col) + r'\b',
                    body, re.IGNORECASE | re.DOTALL
                )
                if expr_match:
                    expr = expr_match.group(1).strip()

                group_by_match = re.search(
                    r'(GROUP\s+BY\s+[\w\.]+(?:\s*,\s*[\w\.]+)*)',
                    body, re.IGNORECASE
                )
                if not group_by_match:
                    continue

                group_by_end = group_by_match.end()

                if re.search(r'\bHAVING\b', body, re.IGNORECASE):
                    having_match = re.search(r'(\bHAVING\b\s*)', body, re.IGNORECASE)
                    if having_match:
                        insert_pos = having_match.end()
                        new_body = body[:insert_pos] + f"{expr} {op} {val} AND " + body[insert_pos:]
                else:
                    new_body = body[:group_by_end] + f"\n    HAVING {expr} {op} {val}" + body[group_by_end:]

                result = result[:body_start] + new_body + result[body_end:]
                offset = len(new_body) - len(body)
                ctes[cte_name] = (body_start, body_end + offset, new_body)
                break

        return result

    def create_optimized_query(self, query: str, suggestions: List, aggressive: bool = False) -> str:
        """Create an optimized version of the query"""
        optimized = self.apply_suggestions(query, suggestions)
        optimized = self.rewrite_correlated_subqueries(optimized)
        optimized = self.push_filters_into_cte(optimized)
        return optimized

    def _strip_inline_comments(self, sql: str) -> str:
        """Strip advisory inline comments (-- ...) added during optimization."""
        lines = sql.split('\n')
        clean_lines = []
        for line in lines:
            if line.strip().startswith('--'):
                continue
            comment_idx = line.find('--')
            if comment_idx != -1:
                line = line[:comment_idx].rstrip()
            if line.strip():
                clean_lines.append(line)
        return '\n'.join(clean_lines)



# """
# Query Rewriter - Applies optimization suggestions to rewrite queries
# """

# from typing import List
# import re


# class QueryRewriter:
#     """Rewrites SQL queries based on optimization suggestions"""
    
#     def apply_suggestions(self, query: str, suggestions: List) -> str:
#         """Apply multiple optimization suggestions to a query"""
#         lines = query.split('\n')
#         sorted_suggestions = sorted(suggestions, key=lambda s: s.line_number, reverse=True)
        
#         # Keywords that mark data-critical SQL clauses which must never be
#         # replaced with comments — doing so would silently change the result set.
#         _PROTECTED_KEYWORDS = ('WHERE', 'AND ', 'AND(', 'OR ', 'OR(', 
#                                'HAVING', 'FROM ', 'JOIN ', 'ON ')
        
#         for suggestion in sorted_suggestions:
#             if 0 < suggestion.line_number <= len(lines):
#                 idx = suggestion.line_number - 1
#                 original = lines[idx].strip()

#                 sc = suggestion.suggested_content

#                 # ── Global comment guard ───────────────────────────────────
#                 # NEVER inject a SQL comment (-- ...) into the query body.
#                 # Comments in suggested_content are advisory notes for the UI,
#                 # not valid SQL replacements.  Any suggestion that generates a
#                 # comment as its replacement must use line_number=0 so it is
#                 # never reached here.  This guard is a second line of defence.
#                 if sc.lstrip().startswith('--'):
#                     continue

#                 # ── Data-critical line guard ───────────────────────────────
#                 # Never replace WHERE/JOIN/FROM/ON/AND/OR/HAVING lines even
#                 # with non-comment content, unless the replacement is clearly
#                 # a valid SQL fragment (heuristic: contains a SQL keyword).
#                 original_upper = original.upper().lstrip()
#                 if any(original_upper.startswith(kw) for kw in _PROTECTED_KEYWORDS):
#                     # Allow replacements that are non-empty and look like SQL
#                     if not sc.strip() or sc.lstrip().startswith('--'):
#                         continue

#                 if self._lines_match(original, suggestion.original_content):
#                     indent = len(lines[idx]) - len(lines[idx].lstrip())
#                     if sc.strip() == "":
#                         # Empty suggested_content means DELETE the line entirely
#                         lines[idx] = None   # mark for removal
#                     else:
#                         lines[idx] = ' ' * indent + sc
        
#         return '\n'.join(line for line in lines if line is not None)
    
#     def _lines_match(self, line1: str, line2: str) -> bool:
#         """Check if two lines match (ignoring whitespace differences)"""
#         normalize = lambda s: ' '.join(s.split())
#         return normalize(line1) == normalize(line2)
    

#     def rewrite_correlated_subqueries(self, query: str) -> str:
#         """Rewrite correlated subqueries in SELECT to CTE + LEFT JOIN.
        
#         Detects pattern:
#           (SELECT AGG(col) FROM table alias WHERE alias.fk = outer.pk) AS name
        
#         Consolidates all subqueries hitting the same table into a single CTE
#         and replaces inline subqueries with CTE column references.
        
#         SAFETY: Rewrites correlated subqueries whose WHERE clause contains a
#         join condition ``alias.col = outer.col``, optionally combined with
#         extra filter conditions via AND (e.g. date ranges).  The join
#         condition drives the CTE GROUP BY, and extra filters become WHERE
#         clauses inside the CTE.

#         Subqueries with OR, nested SELECTs, window functions (OVER), or
#         LIMIT are still skipped to avoid producing incorrect SQL.
#         """
#         query_upper = query.upper()
        
#         # Only proceed if there are subqueries in SELECT
#         if 'SELECT' not in query_upper or query_upper.count('(') < 1:
#             return query
        
#         # Safe parsing: Use regex just to find the start, then string parsing
#         matches = []
#         for start_match in re.finditer(r'\(\s*SELECT\b', query_upper):
#             idx = start_match.start()
            
#             # Find matching parenthesis
#             depth = 1
#             close_idx = -1
#             for j in range(start_match.end(), len(query_upper)):
#                 if query_upper[j] == '(':
#                     depth += 1
#                 elif query_upper[j] == ')':
#                     depth -= 1
#                     if depth == 0:
#                         close_idx = j
#                         break
            
#             if close_idx != -1:
#                 # Check if it has 'AS alias_name' after it
#                 post_parenthesis = query_upper[close_idx+1:close_idx+20].strip()
#                 if post_parenthesis.startswith('AS '):
#                     subq_text = query[idx:close_idx+1]
#                     # Pure string parsing to extract components
#                     subq_text_norm = re.sub(r'\s+', ' ', subq_text)
#                     subq_upper = subq_text_norm.upper()
#                     if ' FROM ' in subq_upper and ' WHERE ' in subq_upper and '=' in subq_upper:
#                         select_pos = subq_upper.find('SELECT')
#                         from_pos = subq_upper.find(' FROM ')
#                         where_pos = subq_upper.find(' WHERE ')
                        
#                         if select_pos < from_pos < where_pos:
#                             agg_expr = subq_text_norm[select_pos+6:from_pos].strip()
#                             table_section = subq_text_norm[from_pos+6:where_pos].strip().split()
                            
#                             if len(table_section) >= 2:
#                                 table = table_section[0]
#                                 t_alias = table_section[-1]
                                
#                                 where_cond = subq_text_norm[where_pos+7 : -1].strip()

#                                 # ── SAFETY GUARDS ──
#                                 where_cond_upper = where_cond.upper()

#                                 # Guard: OR makes decomposition unsafe
#                                 if re.search(r'\bOR\b', where_cond_upper):
#                                     continue

#                                 # Guard 2: nested SELECT inside WHERE
#                                 if 'SELECT' in where_cond_upper:
#                                     continue

#                                 # Guard 3: window functions in the aggregate expression
#                                 if 'OVER' in subq_upper:
#                                     continue

#                                 # Guard 4: LIMIT inside the subquery
#                                 if 'LIMIT' in subq_upper:
#                                     continue

#                                 # Guard 5: type casts (::) in the join condition
#                                 if '::' in where_cond:
#                                     continue

#                                 # Split WHERE on AND to separate join condition
#                                 # from extra filters (e.g. date ranges)
#                                 and_parts = re.split(r'\bAND\b', where_cond, flags=re.IGNORECASE)
#                                 join_cond = None
#                                 extra_filters = []

#                                 for part in and_parts:
#                                     part = part.strip()
#                                     # A join condition has alias.col = outer.col
#                                     if '=' in part and '.' in part:
#                                         eq_parts = part.split('=', 1)
#                                         lp = eq_parts[0].strip()
#                                         rp = eq_parts[1].strip()
#                                         if '.' in lp and '.' in rp:
#                                             # Check for comparison operators
#                                             # (>= would have been split on =)
#                                             if lp.endswith('>') or lp.endswith('<') or lp.endswith('!'):
#                                                 extra_filters.append(part)
#                                                 continue
#                                             try:
#                                                 la, lc = lp.split('.')
#                                                 ra, rc = rp.split('.')
#                                                 if (la.strip().isidentifier() and lc.strip().isidentifier()
#                                                         and ra.strip().isidentifier() and rc.strip().isidentifier()):
#                                                     if join_cond is None:
#                                                         join_cond = (la.strip(), lc.strip(), ra.strip(), rc.strip())
#                                                     else:
#                                                         # Multiple join conditions — too complex
#                                                         extra_filters.append(part)
#                                                 else:
#                                                     extra_filters.append(part)
#                                             except ValueError:
#                                                 extra_filters.append(part)
#                                         else:
#                                             extra_filters.append(part)
#                                     else:
#                                         extra_filters.append(part)

#                                 if join_cond is None:
#                                     continue

#                                 left_alias_jc, left_col, right_alias_jc, right_col = join_cond

#                                 # Get the alias assigned to this subquery
#                                 remaining = query[close_idx+1:].strip()
#                                 alias_match = remaining[3:].split()[0].rstrip(', \n\t')  # Skip 'AS '
                                
#                                 # Create pseudo-match object struct
#                                 class DummyMatch:
#                                     def __init__(self, text):
#                                         self.text = text
#                                     def group(self, n):
#                                         return self.text
                                        
#                                 full_replace_text = query[idx : query.find(alias_match, close_idx) + len(alias_match)]
#                                 matches.append({
#                                     'match': DummyMatch(full_replace_text),
#                                     'agg_expr': agg_expr,
#                                     'table': table,
#                                     'table_alias': t_alias,
#                                     'join_col_left': left_col,
#                                     'join_col_right_alias': right_alias_jc,
#                                     'join_col_right': right_col,
#                                     'column_alias': alias_match,
#                                     'extra_filters': extra_filters,
#                                 })
            
            
#         if len(matches) < 2:  # Need at least 2 subqueries to justify a CTE rewrite
#             return query
        
#         # Group subqueries by table
#         from collections import defaultdict
#         table_groups = defaultdict(list)
#         for m in matches:
#             table_groups[m['table'].lower()].append(m)
        
#         if not table_groups:
#             return query
        
#         # Build CTE(s) and replacement mappings
#         cte_parts = []
#         replacements = {}  # full_match_text -> replacement_text
#         cte_joins = []     # LEFT JOIN clauses to add
        
#         for table_name, subqueries in table_groups.items():
#             cte_name = f"__{table_name}_agg"
#             join_col = subqueries[0]['join_col_left']
#             original_table = subqueries[0]['table']
#             outer_alias = subqueries[0]['join_col_right_alias']
#             outer_col = subqueries[0]['join_col_right']
            
#             # Build aggregation columns for the CTE
#             agg_cols = []
#             for sq in subqueries:
#                 agg_cols.append(f"    {sq['agg_expr']} AS {sq['column_alias']}")
#                 # Map the original subquery text -> CTE column reference
#                 original_text = sq['match'].group(0)
#                 # Replace with COALESCE to handle NULL from LEFT JOIN
#                 if 'COUNT' in sq['agg_expr'].upper():
#                     replacements[original_text] = f"COALESCE({cte_name}.{sq['column_alias']}, 0) AS {sq['column_alias']}"
#                 else:
#                     replacements[original_text] = f"{cte_name}.{sq['column_alias']} AS {sq['column_alias']}"
            
#             agg_cols_joined = ',\n'.join(agg_cols)

#             # Collect any extra WHERE filters from the subqueries
#             # (e.g. date range conditions that apply inside the CTE)
#             all_extra_filters = []
#             for sq in subqueries:
#                 for ef in sq.get('extra_filters', []):
#                     if ef not in all_extra_filters:
#                         all_extra_filters.append(ef)

#             where_clause = ''
#             if all_extra_filters:
#                 where_clause = f"  WHERE {' AND '.join(all_extra_filters)}\n"

#             cte_sql = (
#                 f"{cte_name} AS (\n"
#                 f"  SELECT {join_col},\n"
#                 f"{agg_cols_joined}\n"
#                 f"  FROM {original_table}\n"
#                 f"{where_clause}"
#                 f"  GROUP BY {join_col}\n"
#                 f")"
#             )
#             cte_parts.append(cte_sql)
#             cte_joins.append(
#                 f"LEFT JOIN {cte_name} ON {outer_alias}.{outer_col} = {cte_name}.{join_col}"
#             )
        
#         # Apply replacements to the query
#         result = query
#         for old_text, new_text in replacements.items():
#             result = result.replace(old_text, new_text)
        
#         # Add CTE at the beginning
#         cte_header = "WITH " + ",\n".join(cte_parts)
        
#         # Check if query already has WITH by looking at the first word
#         # Using lstrip() is more robust than regex for leading whitespace
#         lstripped = result.lstrip()
#         lstripped_upper = lstripped.upper()
        
#         if lstripped_upper.startswith('WITH '):
#             # Prepend to existing WITH by finding the WITH keyword
#             with_pos = result.upper().find('WITH ')
#             result = result[:with_pos+5] + cte_header[5:] + ",\n" + result[with_pos+5:]
#         elif lstripped_upper.startswith('SELECT'):
#              # Prepend before SELECT
#              select_pos = result.upper().find('SELECT')
#              result = result[:select_pos] + cte_header + "\n" + result[select_pos:]
#         else:
#              # Fallback: Just prepend it
#              result = cte_header + "\n" + result
        
#         # Add LEFT JOIN(s) before WHERE
#         join_clause = "\n".join(cte_joins)
#         result = re.sub(
#             r'(\bWHERE\b)',
#             f"{join_clause}\nWHERE",
#             result, count=1, flags=re.IGNORECASE
#         )
        
#         return result

#     def rewrite_self_join_to_window(self, query: str) -> str:
#         """Rewrite self-joins on the same CTE/table into window-function based pre-aggregation.
        
#         Detects pattern:
#             FROM cte_name c1 JOIN cte_name c2 ON c1.key = c2.key AND c1.pk != c2.pk
        
#         Replaces with a pre-aggregation CTE using window functions, then arithmetic
#         on the pre-aggregated columns to avoid the O(N²) cartesian product.
#         """
#         # Find self-join pattern: FROM <name> <a1> JOIN <name> <a2> ON <a1>.<key> = <a2>.<key> AND <a1>.<pk> != <a2>.<pk>
#         self_join_pattern = re.compile(
#             r'FROM\s+(\w+)\s+(\w+)\s+JOIN\s+(\w+)\s+(\w+)\s+ON\s+'
#             r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)'
#             r'\s+AND\s+(\w+)\.(\w+)\s*!=\s*(\w+)\.(\w+)',
#             re.IGNORECASE | re.DOTALL
#         )
        
#         m = self_join_pattern.search(query)
#         if not m:
#             return query
        
#         tbl1, alias1, tbl2, alias2 = m.group(1), m.group(2), m.group(3), m.group(4)
        
#         # Must be a self-join (same table/CTE name)
#         if tbl1.lower() != tbl2.lower():
#             return query
        
#         join_key_col = m.group(6)  # The column they join on (e.g. chain_depth)
#         pk_col = m.group(10)       # The column with != (e.g. root_aid)
        
#         # Find the SELECT clause for this self-join query
#         # We need to identify which columns from c1/c2 are used in the SELECT
#         select_match = re.search(
#             r'SELECT\s+(.*?)\s+FROM\s+' + re.escape(tbl1) + r'\s+' + re.escape(alias1),
#             query, re.IGNORECASE | re.DOTALL
#         )
#         if not select_match:
#             return query
        
#         select_clause = select_match.group(1)
        
#         # Identify all columns referenced from alias1 and alias2
#         col_refs_1 = set(re.findall(re.escape(alias1) + r'\.(\w+)', select_clause, re.IGNORECASE))
#         col_refs_2 = set(re.findall(re.escape(alias2) + r'\.(\w+)', select_clause, re.IGNORECASE))
        
#         # Build a pre-aggregated CTE that computes per-group sums/counts using window functions
#         # All columns from the original CTE plus group-level aggregates
#         all_cols = col_refs_1 | col_refs_2
#         all_cols.discard(join_key_col)  # Don't aggregate the join key
#         all_cols.discard(pk_col)        # Don't aggregate the PK
        
#         # Build window aggregate columns
#         window_agg_cols = []
#         for col in sorted(all_cols):
#             window_agg_cols.append(f"    SUM({col}) OVER (PARTITION BY {join_key_col}) AS group_total_{col}")
#             window_agg_cols.append(f"    COUNT(*) OVER (PARTITION BY {join_key_col}) AS group_count_{join_key_col}")
        
#         # Deduplicate
#         window_agg_cols = list(dict.fromkeys(window_agg_cols))
        
#         # Get all base columns from the original CTE used in SELECT
#         base_cols = col_refs_1 | {join_key_col, pk_col}
#         base_col_list = ", ".join(sorted(base_cols))
#         window_col_list = ",\n".join(window_agg_cols)
        
#         pre_agg_cte_name = f"__{tbl1.lower()}_preagg"
#         pre_agg_cte = (
#             f"{pre_agg_cte_name} AS (\n"
#             f"  SELECT {base_col_list},\n"
#             f"{window_col_list}\n"
#             f"  FROM {tbl1}\n"
#             f")"
#         )
        
#         # Rewrite the SELECT clause to use pre-aggregated columns
#         new_select = select_clause
#         for col in sorted(col_refs_2):
#             # Replace alias2.col with (group_total_col - alias1.col) for SUM-like expressions
#             # or (group_count - 1) for COUNT-like expressions  
#             pattern_sum = re.compile(
#                 re.escape(alias1) + r'\.' + re.escape(col) + r'\s*\+\s*' + re.escape(alias2) + r'\.' + re.escape(col),
#                 re.IGNORECASE
#             )
#             if pattern_sum.search(new_select):
#                 # c1.col + c2.col => p.col + (p.group_total_col - p.col) = p.group_total_col
#                 new_select = pattern_sum.sub(f"p.group_total_{col}", new_select)
#                 continue
            
#             pattern_mul = re.compile(
#                 re.escape(alias1) + r'\.' + re.escape(col) + r'\s*\*\s*' + re.escape(alias2) + r'\.' + re.escape(col),
#                 re.IGNORECASE
#             )
#             if pattern_mul.search(new_select):
#                 # c1.col * c2.col => p.col * (p.group_total_col - p.col)
#                 new_select = pattern_mul.sub(
#                     f"p.{col} * (p.group_total_{col} - p.{col})", new_select
#                 )
#                 continue
                
#             # POWER(c1.col - c2.col, 2) pattern
#             power_pattern = re.compile(
#                 r'POWER\s*\(\s*' + re.escape(alias1) + r'\.' + re.escape(col) + r'\s*-\s*'
#                 + re.escape(alias2) + r'\.(\w+)\s*,\s*2\s*\)',
#                 re.IGNORECASE
#             )
#             power_m = power_pattern.search(new_select)
#             if power_m:
#                 col2 = power_m.group(1)
#                 # For variance-like: POWER(c1.x - c2.y, 2) summed over all c2
#                 # This requires a different approach - keep as a note
#                 new_select = power_pattern.sub(
#                     f"POWER(p.{col} - (p.group_total_{col2} - p.{col2}), 2)", new_select
#                 )
#                 continue
                
#             # Simple alias2.col reference => (group_total_col - col)
#             simple_pattern = re.compile(re.escape(alias2) + r'\.' + re.escape(col), re.IGNORECASE)
#             new_select = simple_pattern.sub(f"(p.group_total_{col} - p.{col})", new_select)
        
#         # Replace alias1.col with p.col
#         for col in sorted(col_refs_1):
#             new_select = re.sub(
#                 re.escape(alias1) + r'\.' + re.escape(col),
#                 f"p.{col}", new_select, flags=re.IGNORECASE
#             )
        
#         # Replace the self-join FROM clause with the pre-agg CTE
#         # Find everything from the self-join SELECT to the end
#         full_self_join_block = query[select_match.start():]
        
#         # Build the new query block
#         new_from = f"FROM {pre_agg_cte_name} p"
        
#         # Replace the FROM...JOIN block
#         from_join_end = m.end()
#         new_query = query[:select_match.start()]
#         new_query += f"SELECT {new_select}\n{new_from}\n"
        
#         # Get the rest after the JOIN ON clause (WHERE, ORDER BY, LIMIT, etc.)
#         rest = query[from_join_end:]
        
#         # Clean up alias references in the rest (WHERE, ORDER BY)
#         rest = re.sub(re.escape(alias1) + r'\.', 'p.', rest, flags=re.IGNORECASE)
#         rest = re.sub(re.escape(alias2) + r'\.', 'p.', rest, flags=re.IGNORECASE)
#         new_query += rest
        
#         # Insert the pre-agg CTE
#         # Find the last CTE definition to insert after it
#         last_paren = query[:select_match.start()].rfind(')')
#         if last_paren != -1:
#             new_query = query[:last_paren + 1] + f",\n{pre_agg_cte}\n" + new_query[len(query[:select_match.start()]):]
        
#         return new_query

#     def push_filters_into_cte(self, query: str) -> str:
#         """Move qualifying WHERE filters from an outer query into a CTE definition.
        
#         Uses balanced parenthesis counting to safely extract CTE bodies (avoids
#         regex catastrophic backtracking on nested CASE/WHEN expressions).
#         Only pushes filters on aggregated columns (those produced by GROUP BY).
#         """
#         result = query
        
#         # Step 1: Find all CTE definitions by name using balanced-paren parsing
#         cte_name_pattern = re.compile(
#             r'(\w+)\s+AS\s+(?:MATERIALIZED\s+)?\(', re.IGNORECASE
#         )
        
#         ctes = {}  # name -> (body_start, body_end, body_text)
#         for m in cte_name_pattern.finditer(query):
#             cte_name = m.group(1)
#             # Skip SQL keywords that look like CTE names
#             if cte_name.upper() in ('SELECT', 'WITH', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'RECURSIVE'):
#                 continue
            
#             open_paren = m.end() - 1  # position of the opening '('
#             depth = 1
#             pos = open_paren + 1
#             while pos < len(query) and depth > 0:
#                 if query[pos] == '(':
#                     depth += 1
#                 elif query[pos] == ')':
#                     depth -= 1
#                 pos += 1
            
#             if depth == 0:
#                 body = query[open_paren + 1 : pos - 1].strip()
#                 ctes[cte_name] = (open_paren + 1, pos - 1, body)
        
#         # Step 2: Find the outer WHERE clause at paren-depth 0 (after all CTEs)
#         # Uses balanced-paren walking instead of regex to avoid catastrophic
#         # backtracking on queries with deeply nested subqueries.
#         last_cte_end = max((end for _, end, _ in ctes.values()), default=0) if ctes else 0
#         outer_part = query[last_cte_end:]

#         # Walk the outer_part tracking paren depth to find WHERE at depth 0
#         paren_depth = 0
#         where_start = -1
#         where_end = len(outer_part)
#         i = 0
#         while i < len(outer_part):
#             ch = outer_part[i]
#             if ch == '(':
#                 paren_depth += 1
#             elif ch == ')':
#                 paren_depth -= 1
#             elif paren_depth == 0:
#                 # Check for WHERE keyword at depth 0
#                 if where_start == -1 and outer_part[i:i+5].upper() == 'WHERE' and (
#                     i == 0 or not outer_part[i-1].isalnum()) and (
#                     i + 5 >= len(outer_part) or not outer_part[i+5].isalnum()):
#                     where_start = i + 5  # skip past 'WHERE'
#                 # Check for ORDER BY or LIMIT at depth 0 (end of WHERE)
#                 elif where_start != -1:
#                     rest_upper = outer_part[i:i+10].upper()
#                     if rest_upper.startswith('ORDER') and re.match(r'ORDER\s+BY\b', rest_upper):
#                         where_end = i
#                         break
#                     if rest_upper.startswith('LIMIT') and (i == 0 or not outer_part[i-1].isalnum()):
#                         where_end = i
#                         break
#             i += 1

#         if where_start == -1:
#             return result

#         where_text = outer_part[where_start:where_end].strip()
        
#         # Step 3: Find filter conditions like alias.column > N
#         cond_pattern = re.compile(
#             r'(\w+)\.(\w+)\s*(>|<|>=|<=|=|!=)\s*(\d+)', re.IGNORECASE
#         )
        
#         for cond_m in cond_pattern.finditer(where_text):
#             alias, col, op, val = cond_m.group(1), cond_m.group(2), cond_m.group(3), cond_m.group(4)
            
#             # Step 4: Find which CTE this alias maps to
#             # Look for FROM cte_name alias in the outer query
#             for cte_name, (body_start, body_end, body) in ctes.items():
#                 alias_usage = re.search(
#                     r'\bFROM\s+' + re.escape(cte_name) + r'\s+' + re.escape(alias) + r'\b',
#                     outer_part, re.IGNORECASE
#                 )
#                 if not alias_usage:
#                     continue
                
#                 body_upper = body.upper()
                
#                 # Only act on CTEs with GROUP BY (aggregation CTEs)
#                 if 'GROUP BY' not in body_upper:
#                     continue
                
#                 # Only push if the column is an aggregate alias (appears in SELECT as "AGG(...) as col")
#                 # Check if col appears as an alias: "as col" or "AS col"
#                 if not re.search(r'\bAS\s+' + re.escape(col) + r'\b', body, re.IGNORECASE):
#                     continue
                
#                 # Step 4.5: Find the actual expression to avoid using alias in HAVING (Postgres forbids it)
#                 expr = col
#                 expr_match = re.search(r'(?:SELECT|,)\s*([^,]+?)(?:\s+AS\s+|\s+)' + re.escape(col) + r'\b', body, re.IGNORECASE | re.DOTALL)
#                 if expr_match:
#                     expr = expr_match.group(1).strip()
                
#                 # Step 5: Add HAVING clause to the CTE
#                 # Use [\w\.]+ instead of \S+ to prevent catastrophic backtracking
#                 group_by_match = re.search(r'(GROUP\s+BY\s+[\w\.]+(?:\s*,\s*[\w\.]+)*)', body, re.IGNORECASE)
#                 if not group_by_match:
#                     continue
                
#                 group_by_end = group_by_match.end()
                
#                 if re.search(r'\bHAVING\b', body, re.IGNORECASE):
#                     # Append to existing HAVING
#                     having_match = re.search(r'(\bHAVING\b\s*)', body, re.IGNORECASE)
#                     if having_match:
#                         insert_pos = having_match.end()
#                         new_body = body[:insert_pos] + f"{expr} {op} {val} AND " + body[insert_pos:]
#                 else:
#                     # Insert HAVING after GROUP BY
#                     new_body = body[:group_by_end] + f"\n    HAVING {expr} {op} {val}" + body[group_by_end:]
                
#                 # Replace in result
#                 result = result[:body_start] + new_body + result[body_end:]
#                 # Adjust positions for subsequent replacements
#                 offset = len(new_body) - len(body)
#                 # Update the body in our dict
#                 ctes[cte_name] = (body_start, body_end + offset, new_body)
#                 break
        
#         return result

#     def create_optimized_query(self, query: str, suggestions: List, aggressive: bool = False) -> str:
#         """Create an optimized version of the query"""
#         optimized = self.apply_suggestions(query, suggestions)
#         # Structural optimizations (always equivalent)
#         optimized = self.rewrite_correlated_subqueries(optimized)
#         optimized = self.push_filters_into_cte(optimized)
#         # NOTE: rewrite_self_join_to_window is NOT called here because it changes
#         # semantics for pairwise queries. The LLM handles self-join rewrites when safe.
#         return optimized

#     def _strip_inline_comments(self, sql: str) -> str:
#         """Strip advisory inline comments (-- ...) added during optimization.
        
#         This ensures the final output is clean and the agent does not output
#         commented-out fragments as part of the "optimized" query.
#         """
#         lines = sql.split('\n')
#         clean_lines = []
#         for line in lines:
#             # Only strip lines that are ONLY comments
#             if line.strip().startswith('--'):
#                 continue
#             # Remove trailing comments
#             comment_idx = line.find('--')
#             if comment_idx != -1:
#                 line = line[:comment_idx].rstrip()
#             if line.strip(): # keep non-empty lines
#                  clean_lines.append(line)
#         return '\n'.join(clean_lines)