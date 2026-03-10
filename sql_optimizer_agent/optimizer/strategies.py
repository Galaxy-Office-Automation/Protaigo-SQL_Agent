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
                description='Filter rows in CTEs as early as possible',
                transformation='Move WHERE clauses into the earliest possible CTE',
                expected_improvement='Reduces row combinations for downstream joins'
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
                description='Use a physical table or temporary table for large series',
                transformation='Replace generate_series with a pre-populated numbers table',
                expected_improvement='Disk-backed or indexed access'
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
                strategy_id='REDUNDANT_SORT_REMOVAL',
                name='Remove Redundant Sort',
                applies_to=['ORDER_BY_WITHOUT_LIMIT'],
                description='Remove sorting in CTEs or subqueries that is ignored by the outer query',
                transformation='Delete ORDER BY clause from internal nodes',
                expected_improvement='Eliminates unnecessary sort operations'
            ),
            OptimizationStrategy(
                strategy_id='MATERIALIZE_CTE',
                name='Materialize Complex CTE',
                applies_to=['LARGE_CTE_OUTPUT'],
                description='Force CTE materialization for complex calculations',
                transformation='Add MATERIALIZED hint to CTE (PostgreSQL 12+)',
                expected_improvement='Prevents repeated execution and stabilizes the plan'
            ),
            OptimizationStrategy(
                strategy_id='SELF_JOIN_TO_WINDOW',
                name='Self-Join to Window Function',
                applies_to=['CROSS_JOIN_EXPLOSION', 'CROSS_JOIN_ROW_EXPLOSION'],
                description='Replace self-join for counts/ranks with Window Functions',
                transformation='Use COUNT(*) OVER() or ROW_NUMBER()',
                expected_improvement='Converts O(N^2) join to O(N log N) scan'
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
                sugg = self._suggest_window_function_replacement(query, lines, bottleneck)
                if sugg:
                    suggestions.append(sugg)
            
            elif bn_type == 'ORDER_BY_WITHOUT_LIMIT':
                sugg = self._suggest_sort_removal(lines, bottleneck)
                if sugg:
                    suggestions.append(sugg)
            
            elif bn_type == 'LARGE_CTE_OUTPUT':
                sugg = self._suggest_materialization(lines, bottleneck)
                if sugg:
                    suggestions.append(sugg)
                        
            elif bn_type == 'SUBQUERY_IN_SELECT':
                sugg = self._suggest_lateral_join(lines, bottleneck)
                if sugg:
                    suggestions.append(sugg)
        
        return suggestions

    def _suggest_window_function_replacement(self, query: str, lines: List[str], 
                                           bottleneck: Any) -> Optional[OptimizationSuggestion]:
        """Suggest replacing self-join with window function.
        
        This is advisory-only (line_number=0) because the rewrite is structural
        and cannot be safely done via simple line replacement. The LLM handles
        the actual rewrite when enabled.
        """
        line_num = bottleneck.line_number
        if line_num <= 0: return None
        
        line = lines[line_num - 1]
        
        return OptimizationSuggestion(
            strategy_id='SELF_JOIN_TO_WINDOW',
            line_number=0,  # Advisory only - do NOT replace any line
            original_content=line.strip(),
            suggested_content="-- Requires structural rewrite: use Window Function COUNT(*) OVER instead of self-join",
            explanation="This self-join for peer/group comparison creates a cartesian product. Use Window Functions like COUNT(*) OVER (PARTITION BY ...) or ROW_NUMBER() to achieve the same result in a single scan. This optimization requires a structural rewrite and cannot be applied as a simple line replacement.",
            expected_improvement="O(N^2) to O(N log N) speedup",
            confidence=0.85
        )

    def _suggest_sort_removal(self, lines: List[str], 
                            bottleneck: Any) -> Optional[OptimizationSuggestion]:
        """Suggest removing redundant sorts in CTEs."""
        line_num = bottleneck.line_number
        if line_num <= 0: return None
        
        line = lines[line_num - 1]
        
        # Context check: are we inside a multi-line window function?
        # Look backwards a few lines for 'OVER' or 'PARTITION'
        is_window = False
        start_idx = max(0, line_num - 5)
        for i in range(start_idx, line_num):
            if re.search(r'\bOVER\s*\(', lines[i], re.IGNORECASE) or 'PARTITION' in lines[i].upper():
                is_window = True
                break
                
        if is_window or re.search(r'\bOVER\s*\(', line, re.IGNORECASE) or 'PARTITION' in line.upper():
            return None
            
        # Check if we are inside a CTE (approximate check)
        # If the query ends later, and we have an ORDER BY here, it's often redundant.
        if line_num < len(lines) - 5: 
            return OptimizationSuggestion(
                strategy_id='REDUNDANT_SORT_REMOVAL',
                line_number=line_num,
                original_content=line.strip(),
                suggested_content="-- (Redundant ORDER BY removed)",
                explanation="Internal ORDER BY clauses in CTEs or subqueries are typically ignored by the outer query's planner unless accompanied by a LIMIT. Removing them saves significant sort overhead.",
                expected_improvement="Eliminates unnecessary sorting of intermediate results",
                confidence=0.9
            )
        return None

    def _suggest_lateral_join(self, lines: List[str], 
                             bottleneck: Any) -> Optional[OptimizationSuggestion]:
        """Suggest using LATERAL JOIN for correlated subqueries."""
        line_num = bottleneck.line_number
        if line_num <= 0: return None
        
        line = lines[line_num - 1]
        
        return OptimizationSuggestion(
            strategy_id='USE_LATERAL_JOIN',
            line_number=line_num,
            original_content=line.strip(),
            suggested_content="-- (Convert to LATERAL JOIN)",
            explanation="Correlated subqueries in the SELECT list often force the planner into inefficient nested loops. A LATERAL join allows the planner to choose better join strategies while maintaining access to outer columns.",
            expected_improvement="More flexible execution plan options",
            confidence=0.8
        )

    def _suggest_materialization(self, lines: List[str], 
                                  bottleneck: Any) -> Optional[OptimizationSuggestion]:
        """Suggest adding a MATERIALIZED hint to a CTE.
        
        Skips recursive CTEs where MATERIALIZED is not valid PostgreSQL.
        Preserves original case of CTE names.
        """
        start = bottleneck.line_number - 1 if bottleneck.line_number > 0 else 0
        for idx in range(start, len(lines)):
            line = lines[idx]
            if 'AS (' in line.upper():
                # Skip recursive CTEs - MATERIALIZED is not valid on them in PostgreSQL
                if 'RECURSIVE' in line.upper():
                    return None
                if idx > 0 and 'RECURSIVE' in lines[idx - 1].upper():
                    return None
                
                # Preserve original case - only inject MATERIALIZED keyword
                new_line = re.sub(r'\bAS\s*\(', 'AS MATERIALIZED (', line, count=1, flags=re.IGNORECASE)
                return OptimizationSuggestion(
                    strategy_id='MATERIALIZE_CTE',
                    line_number=idx + 1,
                    original_content=line.strip(),
                    suggested_content=new_line.strip(),
                    explanation="Force the planner to materialize this CTE. Useful for complex queries where the planner might choose a poor join order.",
                    expected_improvement="Predictable performance for complex intermediate sets",
                    confidence=0.7
                )
        return None
