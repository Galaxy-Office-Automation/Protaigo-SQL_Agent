
#LLM Interface - Integration with OpenAI-compatible LLM API
#This module handles network requests to the AI model, formats prompts, and extracts structured JSON from responses.

import json
from typing import Dict, List, Any, Optional
import urllib.request
import urllib.error
import sys

# Ensure modules in root can be imported
sys.path.insert(0, '/home/galaxy/DB_setup/sql_optimizer_agent')
from config import LLM_CONFIG


class LLMInterface:
    """Interface for sending query profiles to an LLM to receive optimized AI suggestions"""
    
    def __init__(self, config: Dict = None):
        # Load default or provided config Dictionary
        cfg = config or LLM_CONFIG
        self.api_base_url = cfg['api_base_url']
        self.api_key = cfg['api_key']
        self.model = cfg['model']
        self.temperature = cfg.get('temperature', 0.1)  # Low temperature for deterministic code changes
        self.max_tokens = cfg.get('max_tokens', 4096)   # Sufficient tokens for returning large queries
    
    def _make_request(self, messages: List[Dict], temperature: float = None, 
                      max_tokens: int = None) -> Dict:
        """Send raw synchronous HTTP request to the designated OpenAI-compatible API"""
        url = f"{self.api_base_url}/chat/completions"
        
        # Build the JSON payload to send to the AI
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature or self.temperature,
            "max_tokens": max_tokens or self.max_tokens
        }
        
        # Encode payload string into bytes for urllib
        data = json.dumps(payload).encode('utf-8')
        # Setup Bearer token authentication header
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.api_key}'
        }
        
        # Create HTTP POST request object
        req = urllib.request.Request(url, data=data, headers=headers, method='POST')
        
        import time
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Execute HTTP Request, timeout after 120 seconds to support complex query reasoning
                with urllib.request.urlopen(req, timeout=120) as response:
                    # Read, decode, and parse the JSON returning from the network
                    return json.loads(response.read().decode('utf-8'))
            except urllib.error.HTTPError as e:
                if e.code == 429 and attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    # log or print if needed: print(f"Rate limited (429). Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                return {"error": str(e)}
            except urllib.error.URLError as e:
                # Returns gracefully wrapped error dict if URL fails (DNS, Refused)
                return {"error": str(e)}
            except Exception as e:
                # Returns other errors cleanly
                return {"error": str(e)}
        return {"error": "Max retries exceeded"}
    
    def analyze_query(self, query: str, bottlenecks: List[Any], 
                      metadata: Dict = None) -> Dict[str, Any]:
        """Use LLM to review the SQL query and the bottlenecks found by the rule engine."""
        
        # Compress the list of rule-based bottlenecks into a multiline string for the prompt
        bottleneck_summary = "\\n".join([
            f"- Line {b.line_number}: {b.bottleneck_type} - {b.description}"
            for b in bottlenecks
        ]) if bottlenecks else "No bottlenecks detected by rule engine."
        
        prompt = f"""You are a PostgreSQL query optimization expert. Analyze this SQL query and provide optimization suggestions.

## SQL Query:
```sql
{query}
```

## Detected Bottlenecks:
{bottleneck_summary}

## Task:
1. Identify the specific lines causing performance issues
2. Provide exact code changes (original → suggested)
3. Explain why each change improves performance
4. Estimate the performance improvement

## Response Format (JSON):
{{
    "non_technical_summary": "A 2-3 sentence explanation in extremely simple, non-technical terms of the issue and how the agent solved it. Do not use SQL jargon.",
    "analysis": "Brief analysis of why the query is slow",
    "suggestions": [
        {{
            "line_number": <int>,
            "original": "<original code>",
            "suggested": "<optimized code>",
            "explanation": "<why this helps>",
            "estimated_improvement": "<percentage or description>"
        }}
    ],
    "optimized_query": "<full optimized query>",
    "expected_speedup": "<e.g., '10x faster' or 'seconds instead of minutes'>"
}}

## PostgreSQL Constraints and Performance (you MUST follow these):
- CORE REQUIREMENT: The optimized query MUST return the EXACT SAME DATA as the original. 
- PROHIBITED: Do NOT add 'LIMIT', 'OFFSET', or 'TABLESAMPLE' unless they were already present in the original query.
- PROHIBITED: Do NOT add or change 'WHERE' clauses in a way that filters out rows (e.g., changing 'aid <= 80000' to 'aid <= 500').
- PROHIBITED: Do NOT replace PERCENTILE_CONT with PERCENTILE_DISC. They produce mathematically different results and are NOT interchangeable.
- RECURSION DEPTH: Only cap recursion if it's a safety guard that DOES NOT change the final result set.

## STRUCTURAL OPTIMIZATION HANDBOOK (Advanced Patterns):

### 1. Self-Join to Window Function (O(N^2) -> O(N log N))
**Pattern**: 
```sql
SELECT a1.id, count(a2.id) 
FROM table a1 JOIN table a2 ON a1.grp = a2.grp AND a1.id != a2.id
GROUP BY a1.id
```
**Optimization**: Use `COUNT(*) OVER (PARTITION BY grp) - 1`.
**Why**: Avoids the cartesian product of the join.

### 2. Correlated Subquery to LATERAL JOIN
**Pattern**:
```sql
SELECT t1.id, (SELECT t2.val FROM t2 WHERE t2.ref = t1.id ORDER BY t2.ts DESC LIMIT 1)
FROM t1
```
**Optimization**: `CROSS JOIN LATERAL (SELECT val FROM t2 WHERE ref = t1.id ORDER BY ts DESC LIMIT 1)`.
**Why**: Allows the planner to choose more efficient join strategies (Hash/Merge) than simple nested loops.

### 3. Redundant Sort Removal
**Pattern**: `WITH cte AS (SELECT * FROM table ORDER BY col) SELECT * FROM cte`
**Optimization**: Remove the `ORDER BY` inside the CTE.
**Why**: Sorting intermediate result sets is expensive and usually ignored by the outer query.

### 4. Filter Pushdown to CTE
**Pattern**: `WITH cte AS (SELECT * FROM table) SELECT * FROM cte WHERE col = val`
**Optimization**: Move `WHERE col = val` inside the `cte` definition.
**Why**: Reduces the volume of data materialized or processed by downstream joins.

### 5. Multi-Pass to CTE Materialization
**Pattern**: Multiple subqueries or CTEs hitting the same large table with similar filters.
**Optimization**: Consolidate into a single `MATERIALIZED` CTE.
**Why**: Ensures the table is scanned only once.

## FINAL CONSTRAINTS:
- YOU MUST NOT use LIMIT/TABLESAMPLE/Range-truncation to speed up the query.
- YOU MUST NOT return the original query if a HIGH severity bottleneck (like a self-join) is detectable. TRY a structural fix first.
- If you can't find a speedup, focus on clarity and CTE structure.
- Window functions cannot be nested inside aggregate functions.
- Recursive CTEs must have exactly one UNION ALL between the base case and recursive term.
- Ensure all parentheses are properly balanced.
- The optimized query must be valid PostgreSQL syntax that can be parsed by EXPLAIN without errors.
"""

        # Define system context along with our user prompt
        messages = [
            {"role": "system", "content": "You are a PostgreSQL performance expert. Always respond with valid JSON."},
            {"role": "user", "content": prompt}
        ]
        
        # Issue synchronous request for AI analysis
        response = self._make_request(messages)
        
        # Handle cases where network request entirely failed
        if "error" in response:
            return {"error": response["error"], "analysis": "LLM analysis failed", "suggestions": []}
        
        try:
            # Extract plain string output from the OpenAI schema
            content = response['choices'][0]['message']['content']
            
            # Since AI outputs JSON within string blocks, we need to parse it carefully
            try:
                # First try to directly parse the entire response as bare JSON
                result = json.loads(content)
            except json.JSONDecodeError:
                # If parsing fails, fall back to searching for a ```json codeblock via regex
                import re
                json_match = re.search(r'```(?:json)?\\s*([\\s\\S]*?)\\s*```', content)
                if json_match:
                    # Strip markdown barriers and parses inner block directly
                    result = json.loads(json_match.group(1))
                else:
                    # Fail loudly if we still cannot extract JSON
                    result = {"raw_response": content, "error": "Could not parse JSON"}
            
            return result
            
        except Exception as e:
            # Fail safely returning missing structured object
            return {"error": str(e), "analysis": "LLM analysis failed", "suggestions": []}
    
    def explain_bottleneck(self, bottleneck: Any) -> str:
        """Get LLM explanation for a specific bottleneck without doing full query evaluation"""
        prompt = f"""Explain this SQL performance bottleneck:

Type: {bottleneck.bottleneck_type}
Line: {bottleneck.line_content}
Description: {bottleneck.description}

Provide:
1. Why this is slow (1-2 sentences)
2. How to fix it (specific code change)
3. Expected improvement"""

        messages = [{"role": "user", "content": prompt}]
        # Limit token response because this is meant to be a short description
        response = self._make_request(messages, max_tokens=500)
        
        # Cleanly surface networking errors instead of crashing
        if "error" in response:
            return f"Error: {response['error']}"
        
        try:
            # Return raw string message straight to user
            return response['choices'][0]['message']['content']
        except:
            return "Error parsing response"
