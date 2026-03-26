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
            }
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
    
    def get_bottleneck_summary(self, bottlenecks: List[Bottleneck]) -> Dict[str, Any]:
        """Get a summary of detected bottlenecks"""
        return {
            'total': len(bottlenecks),
            'high': len([b for b in bottlenecks if b.severity == 'HIGH']),
            'medium': len([b for b in bottlenecks if b.severity == 'MEDIUM']),
            'low': len([b for b in bottlenecks if b.severity == 'LOW']),
            'types': list(set(b.bottleneck_type for b in bottlenecks))
        }
