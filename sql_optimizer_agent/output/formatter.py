"""
Output Formatter - Formats optimization results for display (no external dependencies)
"""

from typing import List, Dict, Any


class OutputFormatter:
    """Formats optimization results for human-readable output"""
    
    def print_result(self, result: Any):
        """Print complete optimization result"""
        print()
        print("=" * 70)
        print("          SQL QUERY OPTIMIZER AGENT - ANALYSIS COMPLETE")
        print("=" * 70)
        
        self._print_bottleneck_summary(result.bottlenecks)
        self._print_suggestions(result.bottlenecks, result.suggestions)
        self._print_optimized_query(result.optimized_query)
        
        print()
        print("-" * 70)
        print(f"EXPECTED IMPROVEMENT: {result.expected_improvement}")
        print("-" * 70)
    
    def _print_bottleneck_summary(self, bottlenecks: List):
        """Print bottleneck summary"""
        print()
        print("DETECTED BOTTLENECKS:")
        print("-" * 50)
        
        if not bottlenecks:
            print("  No bottlenecks detected")
            return
        
        print(f"{'Line':<8}{'Severity':<10}{'Type':<30}{'Description':<50}")
        print("-" * 98)
        
        for bn in bottlenecks:
            severity_marker = {'HIGH': '!!', 'MEDIUM': '!', 'LOW': '.'}.get(bn.severity, ' ')
            print(f"{bn.line_number:<8}{severity_marker} {bn.severity:<8}{bn.bottleneck_type:<30}{bn.description[:47]}...")
    
    def _print_suggestions(self, bottlenecks: List, suggestions: List):
        """Print line-by-line optimization suggestions"""
        print()
        print("LINE-BY-LINE SUGGESTIONS:")
        print("-" * 50)
        
        for bn in bottlenecks:
            print()
            print(f"LINE {bn.line_number}: {bn.line_content[:60]}...")
            print(f"  ISSUE:      {bn.description}")
            print(f"  IMPACT:     {bn.impact}")
            print(f"  SUGGESTION: {bn.suggestion}")
            
            for sugg in suggestions:
                if sugg.line_number == bn.line_number:
                    print(f"  OPTIMIZED:  {sugg.suggested_content}")
                    print(f"  IMPROVEMENT: {sugg.expected_improvement}")
    
    def _print_optimized_query(self, query: str):
        """Print the optimized query"""
        print()
        print("OPTIMIZED QUERY:")
        print("-" * 50)
        lines = query.split('\n')
        for i, line in enumerate(lines, 1):
            print(f"{i:4}: {line}")
    
    def format_as_text(self, result: Any) -> str:
        """Format result as plain text"""
        lines = []
        lines.append("=" * 60)
        lines.append("SQL QUERY OPTIMIZER - ANALYSIS REPORT")
        lines.append("=" * 60)
        
        lines.append("")
        lines.append("DETECTED BOTTLENECKS:")
        for bn in result.bottlenecks:
            lines.append(f"  [{bn.severity}] Line {bn.line_number}: {bn.bottleneck_type}")
            lines.append(f"      {bn.description}")
        
        lines.append("")
        lines.append("LINE-BY-LINE SUGGESTIONS:")
        for bn in result.bottlenecks:
            lines.append(f"\nLINE {bn.line_number}: {bn.line_content}")
            lines.append(f"  ISSUE: {bn.description}")
            lines.append(f"  SUGGESTION: {bn.suggestion}")
        
        lines.append("")
        lines.append("OPTIMIZED QUERY:")
        lines.append(result.optimized_query)
        
        lines.append("")
        lines.append(f"EXPECTED IMPROVEMENT: {result.expected_improvement}")
        
        return "\n".join(lines)
    
    def format_as_json(self, result: Any) -> Dict[str, Any]:
        """Format result as JSON-serializable dict"""
        return {
            "bottlenecks": [
                {
                    "line": bn.line_number,
                    "content": bn.line_content,
                    "type": bn.bottleneck_type,
                    "severity": bn.severity,
                    "description": bn.description,
                    "impact": bn.impact,
                    "suggestion": bn.suggestion
                }
                for bn in result.bottlenecks
            ],
            "suggestions": [
                {
                    "line": s.line_number,
                    "original": s.original_content,
                    "suggested": s.suggested_content,
                    "explanation": s.explanation,
                    "improvement": s.expected_improvement
                }
                for s in result.suggestions
            ],
            "optimized_query": result.optimized_query,
            "expected_improvement": result.expected_improvement,
            "llm_analysis": result.llm_analysis
        }
