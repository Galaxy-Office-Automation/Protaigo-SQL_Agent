"""
LLM Interface - Integration with OpenAI-compatible LLM API
"""

import json
from typing import Dict, List, Any, Optional
import urllib.request
import urllib.error
import sys
sys.path.insert(0, '/home/galaxy/DB_setup/sql_optimizer_agent')
from config import LLM_CONFIG


class LLMInterface:
    """Interface for LLM-powered query analysis"""
    
    def __init__(self, config: Dict = None):
        cfg = config or LLM_CONFIG
        self.api_base_url = cfg['api_base_url']
        self.api_key = cfg['api_key']
        self.model = cfg['model']
        self.temperature = cfg.get('temperature', 0.1)
        self.max_tokens = cfg.get('max_tokens', 4096)
    
    def _make_request(self, messages: List[Dict], temperature: float = None, 
                      max_tokens: int = None) -> Dict:
        """Make HTTP request to OpenAI-compatible API"""
        url = f"{self.api_base_url}/chat/completions"
        
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature or self.temperature,
            "max_tokens": max_tokens or self.max_tokens
        }
        
        data = json.dumps(payload).encode('utf-8')
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.api_key}'
        }
        
        req = urllib.request.Request(url, data=data, headers=headers, method='POST')
        
        try:
            with urllib.request.urlopen(req, timeout=60) as response:
                return json.loads(response.read().decode('utf-8'))
        except urllib.error.URLError as e:
            return {"error": str(e)}
        except Exception as e:
            return {"error": str(e)}
    
    def analyze_query(self, query: str, bottlenecks: List[Any], 
                      metadata: Dict = None) -> Dict[str, Any]:
        """Use LLM to analyze query and suggest optimizations"""
        
        bottleneck_summary = "\n".join([
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

Important: The optimized query MUST produce the same results as the original."""

        messages = [
            {"role": "system", "content": "You are a PostgreSQL performance expert. Always respond with valid JSON."},
            {"role": "user", "content": prompt}
        ]
        
        response = self._make_request(messages)
        
        if "error" in response:
            return {"error": response["error"], "analysis": "LLM analysis failed", "suggestions": []}
        
        try:
            content = response['choices'][0]['message']['content']
            
            # Try to parse directly
            try:
                result = json.loads(content)
            except json.JSONDecodeError:
                # Try to extract JSON from markdown code block
                import re
                json_match = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', content)
                if json_match:
                    result = json.loads(json_match.group(1))
                else:
                    result = {"raw_response": content, "error": "Could not parse JSON"}
            
            return result
            
        except Exception as e:
            return {"error": str(e), "analysis": "LLM analysis failed", "suggestions": []}
    
    def explain_bottleneck(self, bottleneck: Any) -> str:
        """Get LLM explanation for a specific bottleneck"""
        prompt = f"""Explain this SQL performance bottleneck:

Type: {bottleneck.bottleneck_type}
Line: {bottleneck.line_content}
Description: {bottleneck.description}

Provide:
1. Why this is slow (1-2 sentences)
2. How to fix it (specific code change)
3. Expected improvement"""

        messages = [{"role": "user", "content": prompt}]
        response = self._make_request(messages, max_tokens=500)
        
        if "error" in response:
            return f"Error: {response['error']}"
        
        try:
            return response['choices'][0]['message']['content']
        except:
            return "Error parsing response"
