#!/usr/bin/env python3
"""
SQL Query Optimizer Agent - Main Entry Point
=============================================
Analyzes slow SQL queries and suggests line-by-line optimizations.

Usage:
    python main.py <query_file>
    python main.py --query "SELECT * FROM ..."
    python main.py --interactive
"""

import argparse
import json
import sys
sys.path.insert(0, '/home/galaxy/DB_setup/sql_optimizer_agent')

from agent.orchestrator import AgentOrchestrator
from output.formatter import OutputFormatter
from validator.equivalence import EquivalenceValidator


def main():
    parser = argparse.ArgumentParser(
        description='SQL Query Optimizer Agent - Analyze and optimize slow SQL queries'
    )
    parser.add_argument('query_file', nargs='?', help='Path to file containing SQL query')
    parser.add_argument('--query', '-q', type=str, help='SQL query string')
    parser.add_argument('--no-llm', action='store_true', help='Disable LLM analysis')
    parser.add_argument('--explain', action='store_true', help='Run EXPLAIN ANALYZE (may be slow)')
    parser.add_argument('--validate', action='store_true', help='Validate output equivalence')
    parser.add_argument('--output', '-o', choices=['text', 'json', 'rich'], default='rich',
                       help='Output format (default: rich)')
    parser.add_argument('--save', '-s', type=str, help='Save optimized query to file')
    
    args = parser.parse_args()
    
    # Get query
    query = None
    if args.query:
        query = args.query
    elif args.query_file:
        try:
            with open(args.query_file, 'r') as f:
                query = f.read()
        except FileNotFoundError:
            print(f"Error: File not found: {args.query_file}")
            sys.exit(1)
    else:
        # Read from stdin
        print("Enter SQL query (Ctrl+D to finish):")
        query = sys.stdin.read()
    
    if not query or not query.strip():
        print("Error: No query provided")
        sys.exit(1)
    
    # Initialize orchestrator
    orchestrator = AgentOrchestrator(
        use_llm=not args.no_llm,
        use_explain=args.explain
    )
    formatter = OutputFormatter()
    
    try:
        # Run optimization
        print("Analyzing query...")
        result = orchestrator.optimize(query)
        
        # Output results
        if args.output == 'json':
            output = formatter.format_as_json(result)
            print(json.dumps(output, indent=2))
        elif args.output == 'text':
            print(formatter.format_as_text(result))
        else:
            formatter.print_result(result)
        
        # Validate if requested
        if args.validate:
            print("\nValidating output equivalence...")
            validator = EquivalenceValidator()
            validation = validator.validate(query, result.optimized_query)
            if validation.get('valid'):
                print(f"✓ Validation passed: {validation.get('reason')}")
            else:
                print(f"✗ Validation failed: {validation.get('reason')}")
        
        # Save optimized query if requested
        if args.save:
            with open(args.save, 'w') as f:
                f.write(result.optimized_query)
            print(f"\nOptimized query saved to: {args.save}")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        orchestrator.close()


if __name__ == "__main__":
    main()
