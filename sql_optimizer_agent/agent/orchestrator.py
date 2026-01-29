"""
Agent Orchestrator - Coordinates query optimization workflow
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import sys
sys.path.insert(0, '/home/galaxy/DB_setup/sql_optimizer_agent')

from analyzer.sql_parser import SQLParser, ParsedQuery
from analyzer.execution_plan import ExecutionPlanParser, ExecutionPlan
from analyzer.metadata import MetadataExtractor
from optimizer.bottleneck import BottleneckDetector, Bottleneck
from optimizer.strategies import OptimizationStrategies, OptimizationSuggestion
from optimizer.rewriter import QueryRewriter
from agent.llm_interface import LLMInterface


@dataclass
class OptimizationResult:
    """Complete result of query optimization"""
    original_query: str
    parsed_query: ParsedQuery
    execution_plan: Optional[ExecutionPlan]
    bottlenecks: List[Bottleneck]
    suggestions: List[OptimizationSuggestion]
    optimized_query: str
    llm_analysis: Optional[Dict[str, Any]]
    expected_improvement: str


class AgentOrchestrator:
    """Orchestrates the query optimization workflow"""
    
    def __init__(self, use_llm: bool = True, use_explain: bool = False):
        self.sql_parser = SQLParser()
        self.exec_plan_parser = ExecutionPlanParser()
        self.metadata_extractor = MetadataExtractor()
        self.bottleneck_detector = BottleneckDetector()
        self.strategies = OptimizationStrategies()
        self.rewriter = QueryRewriter()
        self.use_llm = use_llm
        self.use_explain = use_explain
        
        if use_llm:
            self.llm = LLMInterface()
        else:
            self.llm = None
    
    def optimize(self, query: str) -> OptimizationResult:
        """Run the complete optimization workflow"""
        
        # Step 1: Parse the query
        parsed_query = self.sql_parser.parse(query)
        
        # Step 2: Get execution plan (optional - may be slow for complex queries)
        execution_plan = None
        if self.use_explain:
            try:
                execution_plan = self.exec_plan_parser.get_execution_plan(query)
            except Exception as e:
                print(f"Warning: Could not get execution plan: {e}")
        
        # Step 3: Detect bottlenecks
        bottlenecks = self.bottleneck_detector.detect(
            query, 
            parsed_query, 
            execution_plan
        )
        
        # Step 4: Generate optimization suggestions
        suggestions = self.strategies.generate_suggestions(query, bottlenecks)
        
        # Step 5: Get LLM analysis (if enabled)
        llm_analysis = None
        if self.use_llm and self.llm:
            try:
                llm_analysis = self.llm.analyze_query(query, bottlenecks)
            except Exception as e:
                print(f"Warning: LLM analysis failed: {e}")
        
        # Step 6: Generate optimized query
        if llm_analysis and 'optimized_query' in llm_analysis:
            optimized_query = llm_analysis['optimized_query']
        elif suggestions:
            optimized_query = self.rewriter.apply_suggestions(query, suggestions)
        else:
            # Apply aggressive optimizations if no specific suggestions
            optimized_query = self.rewriter.create_optimized_query(
                query, suggestions, aggressive=True
            )
        
        # Calculate expected improvement
        expected_improvement = self._calculate_improvement(
            bottlenecks, suggestions, llm_analysis
        )
        
        return OptimizationResult(
            original_query=query,
            parsed_query=parsed_query,
            execution_plan=execution_plan,
            bottlenecks=bottlenecks,
            suggestions=suggestions,
            optimized_query=optimized_query,
            llm_analysis=llm_analysis,
            expected_improvement=expected_improvement
        )
    
    def _calculate_improvement(self, bottlenecks: List[Bottleneck],
                               suggestions: List[OptimizationSuggestion],
                               llm_analysis: Optional[Dict]) -> str:
        """Estimate the expected performance improvement"""
        
        if llm_analysis and 'expected_speedup' in llm_analysis:
            return llm_analysis['expected_speedup']
        
        # Estimate based on bottleneck severity
        high_count = len([b for b in bottlenecks if b.severity == 'HIGH'])
        medium_count = len([b for b in bottlenecks if b.severity == 'MEDIUM'])
        
        if high_count >= 2:
            return "Expected: 100x-1000x faster (minutes → seconds)"
        elif high_count == 1:
            return "Expected: 10x-100x faster"
        elif medium_count >= 2:
            return "Expected: 5x-10x faster"
        elif medium_count == 1:
            return "Expected: 2x-5x faster"
        else:
            return "Minor improvement expected"
    
    def get_line_by_line_report(self, result: OptimizationResult) -> List[Dict[str, Any]]:
        """Generate a line-by-line optimization report"""
        report = []
        
        for bottleneck in result.bottlenecks:
            entry = {
                'line': bottleneck.line_number,
                'original': bottleneck.line_content,
                'issue': bottleneck.description,
                'severity': bottleneck.severity,
                'impact': bottleneck.impact,
                'suggestion': bottleneck.suggestion
            }
            
            # Find matching optimization suggestion
            for sugg in result.suggestions:
                if sugg.line_number == bottleneck.line_number:
                    entry['optimized'] = sugg.suggested_content
                    entry['explanation'] = sugg.explanation
                    break
            
            report.append(entry)
        
        # Add LLM suggestions if available
        if result.llm_analysis and 'suggestions' in result.llm_analysis:
            for llm_sugg in result.llm_analysis['suggestions']:
                line_num = llm_sugg.get('line_number', 0)
                # Check if line already in report
                existing = [r for r in report if r['line'] == line_num]
                if not existing:
                    report.append({
                        'line': line_num,
                        'original': llm_sugg.get('original', ''),
                        'optimized': llm_sugg.get('suggested', ''),
                        'issue': 'LLM detected issue',
                        'severity': 'MEDIUM',
                        'explanation': llm_sugg.get('explanation', ''),
                        'suggestion': llm_sugg.get('estimated_improvement', '')
                    })
        
        # Sort by line number
        report.sort(key=lambda x: x['line'])
        
        return report
    
    def close(self):
        """Clean up resources"""
        self.metadata_extractor.close()
