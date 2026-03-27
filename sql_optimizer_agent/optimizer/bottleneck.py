"""
Bottleneck Detector - Identifies performance bottlenecks in SQL queries
"""

from typing import Dict, List, Any
from dataclasses import dataclass
import re


@dataclass
class Bottleneck:
    """Represents a detected performance bottleneck"""
    bottleneck_type: str
    severity: str  # HIGH, MEDIUM, LOW
    line_number: int
    line_content: str
    description: str
    impact: str
    suggestion: str


class BottleneckDetector:
    """Detects performance bottlenecks in SQL queries( checklist of "bad practices") think of it as knowledge base of "bad practices" """
    
    def __init__(self):
        self.detection_rules = self._initialize_rules()
    
    def _initialize_rules(self) -> List[Dict[str, Any]]:
        """Initialize bottleneck detection rules
        
        Severity Rubric:
        - HIGH:   slowdown (flawed logic).
                  Examples: CROSS JOIN, N+1 queries.
        - MEDIUM: Linear slowdown or resource waste.
                  Examples: No index usage, full table sorts.
        - LOW:    Minor inefficiency.
                  Examples: SELECT DISTINCT on unique cols.
        """
        return [
            {
                'name': 'CROSS_JOIN_EXPLOSION',
                'pattern': r'CROSS\s+JOIN', #a specific pattern to look for 
                'severity': 'HIGH', # how critical is this bottleneck 
                'description': 'Cross join creates cartesian product (n×m rows)',
                'impact_template': 'Multiplies row count',# too large statements
                'suggestion': 'Use Window Functions (COUNT OVER, ROW_NUMBER) to replace self-joins, or add explicit JOIN conditions' # suggestions to fix the issue
            },
            {
                'name': 'LARGE_GENERATE_SERIES',
                'pattern': r'generate_series\s*\(\s*\d+\s*,\s*(\d+)',#
                'severity': 'HIGH',
                'description': 'Large generate_series creates many rows in memory',#
                'impact_template': 'Generates {0} rows',
                'suggestion': 'Process in smaller batches or use an indexed table'
            },
            {
                'name': 'PERCENTILE_CONT',
                'pattern': r'PERCENTILE_CONT\s*\(',
                'severity': 'LOW',
                'description': 'PERCENTILE_CONT requires sorting all rows',
                'impact_template': 'Full sort required for percentile calculation',
                'suggestion': 'Ensure the sort column is indexed to speed up percentile calculation'
            },
            {
                'name': 'ORDER_BY_WITHOUT_LIMIT',
                'pattern': r'\bORDER\s+BY\b(?!.*LIMIT)',
                'severity': 'MEDIUM',
                'description': 'ORDER BY without LIMIT sorts entire result set',
                'impact_template': 'Full sort on potentially large dataset',
                'suggestion': 'Add LIMIT clause or ensure columns are indexed'
            },
            {
                'name': 'UNBOUND_WHERE_RANGE',
                'pattern': r'WHERE\s+\w+\s*[<>]=?\s*\d+\s*$',
                'severity': 'MEDIUM',
                'description': 'Range filter may return large number of rows',
                'impact_template': 'May scan large portion of table',
                'suggestion': 'Ensure columns are indexed to speed up range scans'
            },
            {
                'name': 'STDDEV_AGGREGATION',
                'pattern': r'STDDEV\s*\(|VARIANCE\s*\(',
                'severity': 'LOW',
                'description': 'Statistical functions require two passes over data',
                'impact_template': 'Double pass computation required',
                'suggestion': 'Consider pre-computing if data is static'
            },
            {
                'name': 'RECURSIVE_CTE',
                'pattern': r'WITH\s+RECURSIVE',
                'severity': 'MEDIUM',
                'description': 'Recursive CTE may iterate many times',
                'impact_template': 'Iteration count depends on data',
                'suggestion': 'Add a clear termination condition (e.g., depth limit)'
            },
            {
                'name': 'SUBQUERY_IN_SELECT',
                'pattern': r'SELECT\s+[^(]*\(\s*SELECT',
                'severity': 'HIGH',
                'description': 'Subquery in SELECT executes per row (N+1)',
                'impact_template': 'Executes subquery for each row',
                'suggestion': 'Rewrite as JOIN or CTE'
            },
            {
                'name': 'DISTINCT_LARGE_SET',
                'pattern': r'SELECT\s+DISTINCT',
                'severity': 'LOW',
                'description': 'DISTINCT requires sorting or hashing all rows',
                'impact_template': 'Hash/sort operation on full result set',
                'suggestion': 'Ensure columns are indexed or use GROUP BY'
            },
            {
                'name': 'FUNCTION_IN_WHERE',
                'pattern': r'WHERE\s+\w+\s*\(\s*\w+\s*\)',
                'severity': 'MEDIUM',
                'description': 'Function on column prevents index usage',
                'impact_template': 'Index on column cannot be used',
                'suggestion': 'Move function to right side or create expression index'
            },
            {
                'name': 'LARGE_CTE_OUTPUT',
                'pattern': r'\bAS\s*\(\s*$',
                'severity': 'MEDIUM',
                'description': 'CTE output may be large, slowing down downstream JOINs',
                'impact_template': 'Intermediate result set size unclear',
                'suggestion': 'Use JOINs instead of large IN clauses or filter earlier in the CTE'
            },
            # ── NEW RULE ──────────────────────────────────────────────────────
            # CAST(col AS TYPE) = CAST(col AS TYPE) in a JOIN ON clause silently
            # disables index usage on BOTH sides of the join and forces a full
            # sequential scan + hash join regardless of existing indexes.
            # This is one of the most damaging (and easiest to miss) anti-patterns.
            {
                'name': 'CAST_IN_JOIN_CONDITION',
                'pattern': r'\bON\b.*CAST\s*\(',
                'severity': 'HIGH',
                'description': (
                    'CAST() in JOIN ON condition prevents index usage on both sides. '
                    'PostgreSQL cannot use an index on CAST(a.bid AS TEXT) — it must '
                    'scan every row and cast it at runtime.'
                ),
                'impact_template': 'Full sequential scan on both joined tables — index on join columns is completely bypassed',
                'suggestion': (
                    'Remove the CAST: join on the native column types directly '
                    '(e.g. ON a.bid = b.bid). If types genuinely differ, add a '
                    'generated/functional index: CREATE INDEX ON table ((col::target_type)). '
                    'This single change can improve join speed by 100x on large tables.'
                )
            },
        ]
    
    def detect(self, query: str, 
               parsed_query: Any = None,
               execution_plan: Any = None) -> List[Bottleneck]:
        """Detect bottlenecks in a SQL query"""
        bottlenecks = []
        lines = query.split('\n')
        
        # Apply pattern-based rules
        for i, line in enumerate(lines, 1):
            line_upper = line.upper()
            
            for rule in self.detection_rules:
                if re.search(rule['pattern'], line_upper, re.IGNORECASE):
                    # SAFETY: If this is an ORDER BY rule, skip if it's part of a window function OVER(...)
                    if rule['name'] == 'ORDER_BY_WITHOUT_LIMIT':
                        if re.search(r'OVER\s*\(', line_upper, re.IGNORECASE):
                            continue
                    
                    # Extract additional info for impact calculation
                    impact = rule['impact_template']
                    match = re.search(rule['pattern'], line, re.IGNORECASE)
                    if match and match.groups():
                        try:
                            impact = rule['impact_template'].format(*match.groups())
                        except:
                            pass
                    
                    bottlenecks.append(Bottleneck(
                        bottleneck_type=rule['name'],
                        severity=rule['severity'],
                        line_number=i,
                        line_content=line.strip(),
                        description=rule['description'],
                        impact=impact,
                        suggestion=rule['suggestion']
                    ))
        
        # Add execution plan bottlenecks if available
        if execution_plan and hasattr(execution_plan, 'bottlenecks'):
            for bn in execution_plan.bottlenecks:
                if 'error' not in bn:
                    bottlenecks.append(Bottleneck(
                        bottleneck_type=bn.get('type', 'UNKNOWN'),
                        severity=bn.get('severity', 'MEDIUM'),
                        line_number=0,  # From execution plan, line not known
                        line_content='[From Execution Plan]',
                        description=f"{bn.get('type', 'Issue')} detected",
                        impact=f"Affects {bn.get('rows', bn.get('loops', 'many'))} rows/loops",
                        suggestion='Review execution plan for optimization'
                    ))
        
        # Detect cross-join row explosion
        self._detect_cross_join_explosion(query, lines, bottlenecks)
        
        # Detect CAST() used in JOIN ON conditions (disables index usage)
        self._detect_cast_in_join(query, lines, bottlenecks)

        # Detect correlated subqueries that span multiple lines
        # (pattern-based rule only fires when SELECT and ( are on the same line)
        self._detect_multiline_correlated_subqueries(query, lines, bottlenecks)
        
        # Sort by severity
        severity_order = {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2}
        bottlenecks.sort(key=lambda x: severity_order.get(x.severity, 3))
        
        return bottlenecks
    
    def _detect_cross_join_explosion(self, query: str, lines: List[str], 
                                     bottlenecks: List[Bottleneck]):
        """Detect cross join OR self-join with large row counts"""
        query_upper = query.upper()
        
        # Detect both explicit CROSS JOIN and self-join patterns
        # Self-join: JOIN ... ON ... col != col (acts like a cross join)
        # Constrain regex to avoid catastrophic backtracking on large queries
        has_cross = 'CROSS JOIN' in query_upper
        has_self_join = bool(re.search(
            r'JOIN\s+\w+\s+\w+\s+ON\s+[^;]+?!=', query_upper, re.DOTALL | re.IGNORECASE
        ))
        
        if not has_cross and not has_self_join:
            return
        
        # Look for WHERE clauses that define large ranges
        large_ranges = []
        for i, line in enumerate(lines, 1):
            # Pattern: aid <= 50000 or similar (multiple matches on same line)
            for match in re.finditer(r'(\w+)\s*<=?\s*(\d+)', line):
                col, value = match.groups()
                value = int(value)
                if value > 1000:
                    large_ranges.append({
                        'column': col,
                        'value': value,
                        'line': i
                    })
        
        # If we have multiple large ranges with cross/self join
        if len(large_ranges) >= 2:
            estimated_rows = 1
            for r in large_ranges[:2]:
                estimated_rows *= r['value']
            
            if estimated_rows > 1000000:
                bottlenecks.append(Bottleneck(
                    bottleneck_type='CROSS_JOIN_ROW_EXPLOSION',
                    severity='HIGH',
                    line_number=large_ranges[0]['line'],
                    line_content=f"Self/cross join between ~{large_ranges[0]['value']} and ~{large_ranges[1]['value']} rows",
                    description=f"Join creates ~{estimated_rows:,} row combinations",
                    impact=f"Estimated {estimated_rows:,} rows to process",
                    suggestion=f"Add missing JOIN conditions or use more selective filters"
                ))
    
    def _detect_cast_in_join(self, query: str, lines: List[str],
                              bottlenecks: List[Bottleneck]):
        """
        Scan JOIN … ON clauses for CAST(col AS TYPE) patterns.

        The rule-pattern above fires once per line, but a JOIN ON can span
        multiple lines (e.g. the condition is on the line after ON).  This
        method does a multi-line scan so nothing is missed, and de-duplicates
        against bottlenecks already added by the single-line pass.

        It also fires for the PostgreSQL shorthand cast syntax  col::TYPE
        inside a JOIN ON condition, which has the same index-defeating effect.
        """
        already_flagged_lines = {
            b.line_number for b in bottlenecks
            if b.bottleneck_type == 'CAST_IN_JOIN_CONDITION'
        }

        # Walk through lines looking for ON … CAST or ON … ::TYPE patterns
        in_join = False
        for i, line in enumerate(lines, 1):
            line_upper = line.upper().strip()

            # Track whether we're inside a JOIN … ON block
            if re.search(r'\bJOIN\b', line_upper):
                in_join = True
            if re.search(r'\bWHERE\b|\bGROUP\s+BY\b|\bORDER\s+BY\b|\bHAVING\b', line_upper):
                in_join = False

            if not in_join:
                continue

            has_cast  = bool(re.search(r'\bCAST\s*\(', line_upper))
            has_colon = bool(re.search(r'::\s*\w+', line))   # e.g. a.bid::TEXT

            if (has_cast or has_colon) and i not in already_flagged_lines:
                # Check this line or the previous line has ON/JOIN context
                context = (lines[i - 2].upper() if i >= 2 else '') + line_upper
                if re.search(r'\bON\b|\bJOIN\b', context):
                    already_flagged_lines.add(i)
                    cast_type = 'CAST()' if has_cast else '::<TYPE> shorthand'
                    bottlenecks.append(Bottleneck(
                        bottleneck_type='CAST_IN_JOIN_CONDITION',
                        severity='HIGH',
                        line_number=i,
                        line_content=line.strip(),
                        description=(
                            f'{cast_type} in JOIN ON condition prevents index usage '
                            'on both sides of the join.'
                        ),
                        impact='Full sequential scan on both joined tables — index is bypassed',
                        suggestion=(
                            'Join on native column types directly (ON a.bid = b.bid). '
                            'If types genuinely differ, add a functional index.'
                        )
                    ))

    def _detect_multiline_correlated_subqueries(self, query: str, lines: List[str],
                                                  bottlenecks: List[Bottleneck]):
        """
        Detect correlated subqueries in the SELECT list that span multiple lines.

        The single-line rule pattern  SELECT ... ( SELECT  fires only when the
        opening paren and SELECT keyword are on the same line.  In practice,
        authors often write:

            (                     ← line N   (bare open-paren)
                SELECT COUNT(*)   ← line N+1
                FROM …
                WHERE alias.col = outer.col
            ) AS name

        This method scans for bare '(' lines inside the SELECT list and checks
        whether the next non-blank line starts with SELECT, flagging each
        occurrence once.
        """
        already_flagged_lines = {
            b.line_number for b in bottlenecks
            if b.bottleneck_type == 'SUBQUERY_IN_SELECT'
        }

        # We only care about subqueries inside the outer SELECT list, i.e.
        # before the first FROM clause at depth 0.  Track paren depth.
        depth = 0
        in_select_list = False
        query_upper = query.upper()

        # Find where the top-level FROM starts (very rough — enough for flagging)
        # We walk line-by-line to stay aligned with line numbers.
        past_first_from = False

        for i, line in enumerate(lines, 1):
            stripped = line.strip()
            upper    = stripped.upper()

            # Enter SELECT list on the very first SELECT
            if not in_select_list and upper.startswith('SELECT'):
                in_select_list = True

            # Once we hit top-level FROM, stop looking
            if in_select_list and depth == 0 and re.match(r'^FROM\b', upper):
                past_first_from = True

            if past_first_from:
                break

            if not in_select_list:
                continue

            # Update depth
            depth += stripped.count('(') - stripped.count(')')

            # A bare '(' line (possibly with only a comment before it)
            # means a subquery is opening here
            if re.match(r'^\(\s*(--.*)?$', stripped) and i not in already_flagged_lines:
                # Peek at next non-blank line
                for j in range(i, min(i + 3, len(lines))):
                    next_stripped = lines[j].strip().upper()
                    if next_stripped:
                        if next_stripped.startswith('SELECT'):
                            already_flagged_lines.add(i)
                            bottlenecks.append(Bottleneck(
                                bottleneck_type='SUBQUERY_IN_SELECT',
                                severity='HIGH',
                                line_number=i,
                                line_content=stripped,
                                description='Correlated subquery in SELECT executes once per row (N+1 problem)',
                                impact='Executes subquery for every row in the outer result set',
                                suggestion='Rewrite all correlated subqueries as a single CTE + LEFT JOIN'
                            ))
                        break

    def get_bottleneck_summary(self, bottlenecks: List[Bottleneck]) -> Dict[str, Any]:
        """Get a summary of detected bottlenecks"""
        return {
            'total': len(bottlenecks),
            'high': len([b for b in bottlenecks if b.severity == 'HIGH']),
            'medium': len([b for b in bottlenecks if b.severity == 'MEDIUM']),
            'low': len([b for b in bottlenecks if b.severity == 'LOW']),
            'types': list(set(b.bottleneck_type for b in bottlenecks))
        }