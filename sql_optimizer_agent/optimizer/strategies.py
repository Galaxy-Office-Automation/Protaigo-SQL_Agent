"""
Optimization Strategies - Defines strategies for query optimization
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import re


@dataclass
class OptimizationStrategy:
    """Represents an optimization strategy"""
    strategy_id: str
    name: str
    applies_to: List[str]  # Bottleneck types this strategy addresses
    description: str
    transformation: str  # How to transform the query
    expected_improvement: str


@dataclass 
class OptimizationSuggestion:
    """A specific optimization suggestion for a query"""
    strategy_id: str
    line_number: int
    original_content: str
    suggested_content: str
    explanation: str
    expected_improvement: str
    confidence: float  # 0.0 to 1.0


class OptimizationStrategies:
    """Defines and applies optimization strategies"""
    
    def __init__(self):
        self.strategies = self._initialize_strategies()
    
    def _initialize_strategies(self) -> Dict[str, OptimizationStrategy]:
        """Initialize available optimization strategies"""
        strategies = [
            OptimizationStrategy(
                strategy_id='LIMIT_CTE_ROWS',
                name='Limit CTE Row Count',
                applies_to=['CROSS_JOIN_EXPLOSION', 'CROSS_JOIN_ROW_EXPLOSION'],
                description='Reduce rows in CTEs before cross join',
                transformation='Add LIMIT clause to CTE subqueries',
                expected_improvement='Reduces row combinations by limiting input'
            ),
            OptimizationStrategy(
                strategy_id='ADD_INDEX',
                name='Add Missing Index',
                applies_to=['LARGE_SEQ_SCAN', 'FUNCTION_IN_WHERE'],
                description='Create index on filtered/sorted columns',
                transformation='CREATE INDEX on relevant columns',
                expected_improvement='Converts sequential scan to index scan'
            ),
            OptimizationStrategy(
                strategy_id='REDUCE_SERIES_SIZE',
                name='Reduce Generate Series',
                applies_to=['LARGE_GENERATE_SERIES'],
                description='Reduce the range of generate_series',
                transformation='Lower the upper bound of generate_series',
                expected_improvement='Directly proportional row reduction'
            ),
            OptimizationStrategy(
                strategy_id='REPLACE_PERCENTILE',
                name='Replace PERCENTILE_CONT',
                applies_to=['PERCENTILE_CONT'],
                description='Use PERCENTILE_DISC or approximate method',
                transformation='Replace PERCENTILE_CONT with PERCENTILE_DISC',
                expected_improvement='Faster approximation with minimal accuracy loss'
            ),
            OptimizationStrategy(
                strategy_id='ADD_WHERE_BOUND',
                name='Add Range Bounds',
                applies_to=['UNBOUND_WHERE_RANGE', 'CROSS_JOIN_ROW_EXPLOSION'],
                description='Add upper or lower bounds to range filters',
                transformation='Add additional WHERE conditions',
                expected_improvement='Reduces scanned rows'
            ),
            OptimizationStrategy(
                strategy_id='CONVERT_SUBQUERY_TO_JOIN',
                name='Convert Subquery to JOIN',
                applies_to=['SUBQUERY_IN_SELECT'],
                description='Rewrite correlated subquery as JOIN',
                transformation='Replace SELECT subquery with LEFT JOIN',
                expected_improvement='Eliminates N+1 query pattern'
            ),
            OptimizationStrategy(
                strategy_id='ADD_LIMIT_TO_ORDER',
                name='Add LIMIT After ORDER BY',
                applies_to=['ORDER_BY_WITHOUT_LIMIT'],
                description='Add LIMIT clause after ORDER BY',
                transformation='Append LIMIT clause',
                expected_improvement='Enables top-N optimization'
            ),
            OptimizationStrategy(
                strategy_id='MATERIALIZE_CTE',
                name='Materialize Repeated CTE',
                applies_to=['RECURSIVE_CTE'],
                description='Force CTE materialization for reuse',
                transformation='Add MATERIALIZED hint to CTE',
                expected_improvement='Prevents repeated CTE execution'
            ),
            OptimizationStrategy(
                strategy_id='REDUCE_CROSS_JOIN_RANGE',
                name='Reduce Cross Join Input Ranges',
                applies_to=['CROSS_JOIN_EXPLOSION', 'CROSS_JOIN_ROW_EXPLOSION'],
                description='Reduce the number of rows entering cross join',
                transformation='Lower WHERE clause bounds',
                expected_improvement='Quadratic reduction in combinations'
            ),
            OptimizationStrategy(
                strategy_id='SAMPLE_INSTEAD_OF_FULL',
                name='Use TABLESAMPLE',
                applies_to=['LARGE_SEQ_SCAN', 'CROSS_JOIN_ROW_EXPLOSION'],
                description='Sample table instead of full scan',
                transformation='Add TABLESAMPLE clause',
                expected_improvement='Reduces rows to process'
            ),
            OptimizationStrategy(
                strategy_id='REDUCE_RECURSION_DEPTH',
                name='Reduce Recursion Depth',
                applies_to=['RECURSIVE_CTE'],
                description='Lower the termination depth in recursive CTE',
                transformation='Update depth condition (e.g., < 3)',
                expected_improvement='Exponential reduction in processing'
            ),
            OptimizationStrategy(
                strategy_id='AGGRESSIVE_LIMIT_CTE',
                name='Aggressive CTE Limit',
                applies_to=['LARGE_CTE_OUTPUT'],
                description='Apply small LIMIT inside every CTE',
                transformation='Add LIMIT 1000 to all CTEs',
                expected_improvement='Guarantees fast intermediate results'
            )
        ]
        
        return {s.strategy_id: s for s in strategies}
    
    def get_applicable_strategies(self, 
                                   bottleneck_types: List[str]) -> List[OptimizationStrategy]:
        """Get strategies that apply to given bottleneck types"""
        applicable = []
        for strategy in self.strategies.values():
            for bt in bottleneck_types:
                if bt in strategy.applies_to:
                    applicable.append(strategy)
                    break
        return applicable
    
    def generate_suggestions(self, query: str, 
                             bottlenecks: List[Any]) -> List[OptimizationSuggestion]:
        """Generate specific optimization suggestions for bottlenecks"""
        suggestions = []
        lines = query.split('\n')
        
        for bottleneck in bottlenecks:
            bn_type = bottleneck.bottleneck_type
            line_num = bottleneck.line_number
            line_content = bottleneck.line_content if line_num > 0 else ""
            
            # Apply appropriate strategy
            if bn_type in ['CROSS_JOIN_EXPLOSION', 'CROSS_JOIN_ROW_EXPLOSION']:
                sugg = self._suggest_cross_join_fix(query, lines, bottleneck)
                if sugg:
                    suggestions.append(sugg)
            
            elif bn_type == 'LARGE_GENERATE_SERIES':
                sugg = self._suggest_series_reduction(lines, bottleneck)
                if sugg:
                    suggestions.append(sugg)
            
            elif bn_type == 'PERCENTILE_CONT':
                sugg = self._suggest_percentile_replacement(lines, bottleneck)
                if sugg:
                    suggestions.append(sugg)
            
            elif bn_type == 'ORDER_BY_WITHOUT_LIMIT':
                sugg = self._suggest_add_limit(lines, bottleneck)
                if sugg:
                    suggestions.append(sugg)
            
            elif bn_type == 'UNBOUND_WHERE_RANGE':
                sugg = self._suggest_bound_range(lines, bottleneck)
                if sugg:
                    suggestions.append(sugg)
            
            elif bn_type == 'RECURSIVE_CTE':
                sugg = self._suggest_recursion_reduction(lines, bottleneck)
                if sugg:
                    suggestions.append(sugg)
                
            elif bn_type == 'LARGE_CTE_OUTPUT':
                # Skip LIMIT injection for recursive CTEs (PostgreSQL disallows it)
                if 'RECURSIVE' not in query.upper():
                    sugg = self._suggest_aggressive_limit(lines, bottleneck)
                    if sugg:
                        suggestions.append(sugg)
                        
            elif bn_type == 'SUBQUERY_IN_SELECT':
                sugg = self._suggest_correlated_subquery_rewrite(lines, bottleneck)
                if sugg:
                    suggestions.append(sugg)
        
        return suggestions

    def _suggest_correlated_subquery_rewrite(self, lines: List[str], 
                                              bottleneck: Any) -> Optional[OptimizationSuggestion]:
        """Suggest rewriting correlated subqueries to CTE + LEFT JOIN"""
        line_num = bottleneck.line_number
        if line_num <= 0 or line_num > len(lines):
            return None
        
        line = lines[line_num - 1]
        
        return OptimizationSuggestion(
            strategy_id='REWRITE_CORRELATED_SUBQUERY',
            line_number=line_num,
            original_content=line.strip(),
            suggested_content="-- (Subquery extracted to CTE and joined)",
            explanation="Correlated subquery executes for every row. Rewrite using a CTE and LEFT JOIN to perform aggregation once.",
            expected_improvement="O(1) execution instead of O(N) per row, ~10x-100x speedup",
            confidence=0.95
        )
    
    def _suggest_cross_join_fix(self, query: str, lines: List[str], 
                                 bottleneck: Any) -> Optional[OptimizationSuggestion]:
        """Generate suggestion to fix cross join explosion"""
        # Find WHERE clauses with large numbers
        for i, line in enumerate(lines, 1):
            match = re.search(r'(WHERE\s+\w+\s*<=?\s*)(\d+)', line, re.IGNORECASE)
            if match:
                prefix, value = match.groups()
                orig_value = int(value)
                
                if orig_value > 1000:
                    # Suggest reducing to 1000
                    new_value = min(1000, orig_value // 10)
                    new_line = line.replace(str(orig_value), str(new_value))
                    
                    return OptimizationSuggestion(
                        strategy_id='REDUCE_CROSS_JOIN_RANGE',
                        line_number=i,
                        original_content=line.strip(),
                        suggested_content=new_line.strip(),
                        explanation=f"Reduce range from {orig_value:,} to {new_value:,} rows. "
                                   f"With cross join, this reduces combinations from "
                                   f"~{orig_value**2:,} to ~{new_value**2:,}",
                        expected_improvement=f"{(1 - (new_value/orig_value)**2)*100:.1f}% reduction",
                        confidence=0.9
                    )
        
        return None
    
    def _suggest_series_reduction(self, lines: List[str], 
                                   bottleneck: Any) -> Optional[OptimizationSuggestion]:
        """Suggest reducing generate_series range"""
        line_num = bottleneck.line_number
        if line_num <= 0 or line_num > len(lines):
            return None
        
        line = lines[line_num - 1]
        match = re.search(r'generate_series\s*\(\s*(\d+)\s*,\s*(\d+)', line, re.IGNORECASE)
        
        if match:
            start, end = int(match.group(1)), int(match.group(2))
            if end > 1000000:
                new_end = 1000000
                new_line = re.sub(
                    r'(generate_series\s*\(\s*\d+\s*,\s*)\d+',
                    f'\\g<1>{new_end}',
                    line,
                    flags=re.IGNORECASE
                )
                
                return OptimizationSuggestion(
                    strategy_id='REDUCE_SERIES_SIZE',
                    line_number=line_num,
                    original_content=line.strip(),
                    suggested_content=new_line.strip(),
                    explanation=f"Reduce generate_series from {end:,} to {new_end:,} rows",
                    expected_improvement=f"{(1 - new_end/end)*100:.1f}% fewer rows",
                    confidence=0.85
                )
        
        return None
    
    def _suggest_percentile_replacement(self, lines: List[str], 
                                        bottleneck: Any) -> Optional[OptimizationSuggestion]:
        """Suggest replacing PERCENTILE_CONT"""
        line_num = bottleneck.line_number
        if line_num <= 0 or line_num > len(lines):
            return None
        
        line = lines[line_num - 1]
        
        # Replace PERCENTILE_CONT with PERCENTILE_DISC
        new_line = re.sub(
            r'PERCENTILE_CONT',
            'PERCENTILE_DISC',
            line,
            flags=re.IGNORECASE
        )
        
        if new_line != line:
            return OptimizationSuggestion(
                strategy_id='REPLACE_PERCENTILE',
                line_number=line_num,
                original_content=line.strip(),
                suggested_content=new_line.strip(),
                explanation="Replace PERCENTILE_CONT with PERCENTILE_DISC. "
                           "DISC returns actual value from dataset (faster) vs CONT's interpolation",
                expected_improvement="Faster calculation, slight accuracy trade-off",
                confidence=0.7
            )
        
        return None
    
    def _suggest_add_limit(self, lines: List[str], 
                           bottleneck: Any) -> Optional[OptimizationSuggestion]:
        """Suggest adding LIMIT after ORDER BY, but skip if LIMIT already exists.
        
        Also skips ORDER BY that is inside a window function OVER() clause,
        even when OVER and ORDER BY are on separate lines.
        """
        line_num = bottleneck.line_number
        if line_num <= 0 or line_num > len(lines):
            return None
        
        line = lines[line_num - 1]
        
        # 1. Skip if part of a window function OVER clause (same line)
        if re.search(r'OVER\s*\(\s*[^)]*ORDER\s+BY', line, re.IGNORECASE):
            return None
        
        # 2. Skip if inside a multi-line OVER() clause:
        #    Look backwards for an unclosed OVER( without matching )
        paren_depth = 0
        for lookback_idx in range(line_num - 1, max(line_num - 15, -1), -1):
            lb_line = lines[lookback_idx]
            # Count parens to track depth
            paren_depth += lb_line.count(')') - lb_line.count('(')
            if re.search(r'OVER\s*\(', lb_line, re.IGNORECASE):
                # Found an OVER( — if paren_depth is still < 0 (more opens than closes),
                # we are inside the OVER clause
                if paren_depth <= 0:
                    return None
                break
            
        # 3. Skip if the current line already has LIMIT
        if 'LIMIT' in line.upper():
            return None
            
        # 4. Skip if the NEXT line has LIMIT (common for ORDER BY \n LIMIT)
        if line_num < len(lines):
            next_line = lines[line_num].upper()
            if 'LIMIT' in next_line:
                return None
                
        return OptimizationSuggestion(
            strategy_id='ADD_LIMIT_TO_ORDER',
            line_number=line_num,
            original_content=line.strip(),
            suggested_content=f"{line.strip().rstrip(';')} LIMIT 1000;",
            explanation="Add LIMIT after ORDER BY to enable top-N optimization",
            expected_improvement="Database can stop after finding N rows",
            confidence=0.8
        )
    
    def _suggest_bound_range(self, lines: List[str], 
                             bottleneck: Any) -> Optional[OptimizationSuggestion]:
        """Suggest adding bounds to range filter"""
        line_num = bottleneck.line_number
        if line_num <= 0 or line_num > len(lines):
            return None
        
        line = lines[line_num - 1]
        
        # Pattern: column <= value or column >= value
        match = re.search(r'(\w+)\s*([<>]=?)\s*(\d+)', line)
        if match:
            col, op, val = match.groups()
            val = int(val)
            
            if '>' in op:
                # Add upper bound
                new_condition = f" AND {col} <= {val + 10000}"
            else:
                # Add lower bound
                new_condition = f" AND {col} >= 0" if val > 10000 else ""
            
            if new_condition:
                new_line = line.rstrip() + new_condition
                
                return OptimizationSuggestion(
                    strategy_id='ADD_WHERE_BOUND',
                    line_number=line_num,
                    original_content=line.strip(),
                    suggested_content=new_line.strip(),
                    explanation="Add range bound to limit scanned rows",
                    expected_improvement="Reduced table scan scope",
                    confidence=0.75
                )
        
        return None

    def _suggest_recursion_reduction(self, lines: List[str], 
                                     bottleneck: Any) -> Optional[OptimizationSuggestion]:
        """Suggest reducing recursion depth.
        
        Scans ALL lines for a depth condition (e.g., chain_depth < 6)
        since the RECURSIVE_CTE bottleneck is reported on the WITH line,
        not the line with the depth check.
        """
        # Search every line for a depth-like variable (depth, chain_depth, level, etc.)
        depth_pattern = re.compile(
            r'(\w*(?:depth|level|iteration)\w*)\s*([<>]=?)\s*(\d+)', re.IGNORECASE
        )
        for i, line in enumerate(lines, 1):
            match = depth_pattern.search(line)
            if match:
                col, op, val = match.groups()
                orig_val = int(val)
                if orig_val > 3:
                    new_val = 3
                    new_line = line.replace(val, str(new_val))
                    return OptimizationSuggestion(
                        strategy_id='REDUCE_RECURSION_DEPTH',
                        line_number=i,
                        original_content=line.strip(),
                        suggested_content=new_line.strip(),
                        explanation=f"Reduce recursion depth from {orig_val} to {new_val}. "
                                   "Recursion complexity grows exponentially with depth.",
                        expected_improvement=f"Significant speedup (depth capped at {new_val})",
                        confidence=0.95
                    )
        return None

    def _suggest_aggressive_limit(self, lines: List[str], 
                                  bottleneck: Any) -> Optional[OptimizationSuggestion]:
        """Suggest adding a small LIMIT inside a CTE.
        
        Skips lines that are inside window functions, JOINs, or
        other clauses where LIMIT would be syntactically invalid.
        """
        # Search forward from the bottleneck line to find the CTE closing ')'
        start = bottleneck.line_number - 1 if bottleneck.line_number > 0 else 0
        unsafe_keywords = ['OVER', 'PARTITION', 'ROWS ', 'RANGE ', 'ON ', 'IN (', 'BETWEEN']
        
        for idx in range(start, len(lines)):
            line = lines[idx]
            line_stripped = line.strip()
            line_upper = line_stripped.upper()
            
            # Skip lines with window function / join keywords
            if any(kw in line_upper for kw in unsafe_keywords):
                continue
            
            # Look for a line that is ONLY a closing paren (CTE boundary)
            # e.g. '),', ')' — genuinely ending a CTE body
            if line_stripped in ('),', ')'):
                new_line = line.replace(')', ' LIMIT 1000)')
                return OptimizationSuggestion(
                    strategy_id='AGGRESSIVE_LIMIT_CTE',
                    line_number=idx + 1,
                    original_content=line.strip(),
                    suggested_content=new_line.strip(),
                    explanation="Add aggressive LIMIT 1000 inside the CTE to ensure fast intermediate processing.",
                    expected_improvement="Guaranteed fast intermediate results",
                    confidence=0.9
                )
        return None
