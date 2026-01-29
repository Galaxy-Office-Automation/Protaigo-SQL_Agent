# SQL Query Optimizer Agent

An agentic system that analyzes slow SQL queries and provides line-by-line optimization suggestions.

## Features

- **Query Analysis**: Parses SQL and detects expensive patterns
- **Bottleneck Detection**: Identifies CROSS JOINs, large scans, PERCENTILE_CONT, etc.
- **LLM Integration**: Uses AI to generate optimization suggestions
- **Line-by-Line Output**: Shows exactly which lines to change and why
- **Output Validation**: Verifies optimized query produces same results

## Installation

```bash
cd /home/galaxy/DB_setup/sql_optimizer_agent
pip install -r requirements.txt
```

## Usage

### Command Line

```bash
# Analyze a query file
python main.py query.sql

# Analyze inline query
python main.py --query "SELECT * FROM users WHERE id > 1000"

# With JSON output
python main.py query.sql --output json

# Without LLM (faster, rule-based only)
python main.py query.sql --no-llm

# Validate output equivalence
python main.py query.sql --validate
```

### As Library

```python
from agent.orchestrator import AgentOrchestrator
from output.formatter import OutputFormatter

orchestrator = AgentOrchestrator(use_llm=True)
result = orchestrator.optimize(slow_query)

# Print formatted output
formatter = OutputFormatter()
formatter.print_result(result)

# Get line-by-line report
report = orchestrator.get_line_by_line_report(result)
```

## Project Structure

```
sql_optimizer_agent/
├── main.py              # CLI entry point
├── config.py            # Database & LLM config
├── analyzer/            # Query parsing & analysis
├── optimizer/           # Bottleneck detection & strategies
├── agent/               # Orchestration & LLM interface
├── validator/           # Output equivalence checking
├── output/              # Formatting & display
└── tests/               # Test cases
```

## Testing

```bash
python tests/test_optimizer.py
```
