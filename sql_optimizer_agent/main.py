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

# Ensure Python can find modules in the project root directory
sys.path.insert(0, '/home/galaxy/DB_setup/sql_optimizer_agent')

# Import core application modules for execution, display, and parsing
from agent.orchestrator import AgentOrchestrator
from output.formatter import OutputFormatter
from validator.equivalence import EquivalenceValidator


def main():
    # Setup ArgumentParser for command-line options
    parser = argparse.ArgumentParser(
        description='SQL Query Optimizer Agent - Analyze and optimize slow SQL queries'
    )
    # Positional argument for the path of the query file
    parser.add_argument('query_file', nargs='?', help='Path to file containing SQL query')
    # Optional flags for string input, skipping AI analysis, or executing explain
    parser.add_argument('--query', '-q', type=str, help='SQL query string')
    parser.add_argument('--no-llm', action='store_true', help='Disable LLM analysis')
    parser.add_argument('--explain', action='store_true', help='Run EXPLAIN ANALYZE (may be slow)')
    parser.add_argument('--validate', action='store_true', help='Validate output equivalence')
    # Output formatting flags
    parser.add_argument('--output', '-o', choices=['text', 'json', 'rich'], default='rich',
                       help='Output format (default: rich)')
    # Flag to request saving the final optimized SQL into a specific file
    parser.add_argument('--save', '-s', type=str, help='Save optimized query to file')
    
    # Parse provided arguments
    args = parser.parse_args()
    
    # Get query from provided string, file path, or stdin prompt
    query = None
    if args.query:
        query = args.query
    elif args.query_file:
        try:
            # Read from file
            with open(args.query_file, 'r') as f:
                query = f.read()
        except FileNotFoundError:
            print(f"Error: File not found: {args.query_file}")
            sys.exit(1)
    else:
        # Read directly from Standard Input if no params are passed
        print("Enter SQL query (Ctrl+D to finish):")
        query = sys.stdin.read()
    
    # Validating query string is not blank
    if not query or not query.strip():
        print("Error: No query provided")
        sys.exit(1)
    
    # Initialize the core orchestrator representing the full agent logic
    orchestrator = AgentOrchestrator(
        use_llm=not args.no_llm,      # Determines if we want smart LLM suggestions
        use_explain=args.explain      # Determines if we want database feedback
    )
    # Initialize tool for printing results based on choices (json, text, formatting logs)
    formatter = OutputFormatter()
    
    try:
        # Step 1: Execute optimization process
        print("Analyzing query...")
        result = orchestrator.optimize(query)
        
        # Step 2: Print output appropriately depending on requested output format
        if args.output == 'json':
            output = formatter.format_as_json(result)
            print(json.dumps(output, indent=2))
        elif args.output == 'text':
            print(formatter.format_as_text(result))
        else:
            formatter.print_result(result)
        
        # Step 3: Optional syntactic/semantic validation of the newly generated SQL
        if args.validate:
            print("\nValidating output equivalence...")
            validator = EquivalenceValidator()
            # Compare original query side-by-side with optimized variation
            validation = validator.validate(query, result.optimized_query)
            if validation.get('valid'):
                print(f"✓ Validation passed: {validation.get('reason')}")
            else:
                print(f"✗ Validation failed: {validation.get('reason')}")
        
        # Step 4: Write output optimized SQL text back to a file system location
        if args.save:
            with open(args.save, 'w') as f:
                f.write(result.optimized_query)
            print(f"\nOptimized query saved to: {args.save}")
        
    except Exception as e:
        # Blanket exception handler for unexpected engine errors
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        # Ensures that database connections created within metadata extractor are cleaned up
        orchestrator.close()


if __name__ == "__main__":
    main()
