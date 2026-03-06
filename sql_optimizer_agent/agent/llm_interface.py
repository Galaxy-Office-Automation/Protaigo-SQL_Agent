
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
        
        try:
            # Execute HTTP Request, timeout after 60 seconds
            with urllib.request.urlopen(req, timeout=60) as response:
                # Read, decode, and parse the JSON returning from the network
                return json.loads(response.read().decode('utf-8'))
        except urllib.error.URLError as e:
            # Returns gracefully wrapped error dict if URL fails (DNS, Refused)
            return {"error": str(e)}
        except Exception as e:
            # Returns other errors cleanly
            return {"error": str(e)}
    
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
- PERFORMANCE TARGET: The optimized query MUST execute in under 10 seconds.
- LIMIT and OFFSET are NOT allowed inside recursive CTEs (WITH RECURSIVE). Place them only in the final outer SELECT.
- RECURSION DEPTH: Cap recursion at 3 levels (e.g., depth < 3) unless absolutely necessary for correctness.
- AGGRESSIVE SAMPLING: If tables are large (>1M rows), use TABLESAMPLE SYSTEM (1) or aggressive WHERE filtering on indexed IDs.
- AGGRESSIVE LIMITS: Add LIMIT 1000 to intermediate CTEs to prevent row explosion.
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
