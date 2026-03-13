"""
SQL Parser - Parses and analyzes SQL query structure (no external dependencies)
Breaks down raw SQL strings into structural components for analysis.
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
import re


@dataclass
class QueryComponent:
    """Represents a discrete keyword-based component of a SQL query mapping block to its line information."""
    component_type: str              # e.g., SELECT, FROM, WHERE, JOIN
    content: str                     # The string snippet contained in this block
    start_line: int                  # Start line number in the original query 
    end_line: int                    # End line number in the original query
    tokens: List[str] = field(default_factory=list) # Elements extracted from the snippet for fine-grained check


@dataclass
class ParsedQuery:
    """Complete, parsed, and mapped representation of a SQL AST (Abstract Syntax Tree) alternative."""
    original_query: str              # The raw input SQL text
    query_type: str                  # Operational type (SELECT, INSERT, UPDATE, DELETE)
    components: List[QueryComponent] # Sequential list of structured query blocks
    ctes: List[Dict[str, Any]]       # Track Common Table Expressions (WITH clauses) names and lines
    tables: List[str]                # Extracted names of root tables accessed in FROM
    joins: List[Dict[str, Any]]      # Extracted map of join logic and relations
    where_clauses: List[str]         # Extracted logical filtering statements
    order_by: Optional[str]          # Extracted ORDER BY rules
    group_by: Optional[str]          # Extracted GROUP BY logic
    limit: Optional[int]             # Hard row limit returned
    has_cross_join: bool             # Security/performance flag for runaway cartesian products
    has_subquery: bool               # Flag for nesting logic which can dictate execution paths
    line_mapping: Dict[int, str]     # Fast lookup dict mapping 1-indexed numbers to raw line query text


class SQLParser:
    """Parses raw SQL queries into structured components iteratively safely without complex libs."""
    
    def __init__(self):
        # Master list of strings denoting slow or problematic logic
        self.expensive_patterns = [
            'CROSS JOIN', 'PERCENTILE_CONT', 'PERCENTILE_DISC',
            'generate_series', 'pg_sleep', 'ORDER BY', 'DISTINCT',
            'UNION ALL', 'RECURSIVE'
        ]
    
    def parse(self, query: str) -> ParsedQuery:
        """Entry point that explodes the query string and triggers analysis routines."""
        # Explode string into raw line-by-line arrays
        lines = query.split('\n')
        # Cache line contexts securely from 1 indexing
        line_mapping = {i + 1: line for i, line in enumerate(lines)}
        # Normalize uppercase variations to catch patterns safely
        query_upper = query.upper()
        
        # Default fallback is SELECT, but we check if action keyword present
        query_type = "SELECT"
        for qt in ["INSERT", "UPDATE", "DELETE"]:
            if qt in query_upper:
                query_type = qt
                break
        
        # Call subroutines sequentially to extract structural objects
        components = self._extract_components(query, lines)
        ctes = self._extract_ctes(query, lines)
        tables = self._extract_tables(query)
        joins = self._extract_joins(query, lines)
        where_clauses = self._extract_where_clauses(query)
        order_by = self._extract_order_by(query)
        group_by = self._extract_group_by(query)
        limit = self._extract_limit(query)
        
        # Hydrate dataclass successfully and return
        return ParsedQuery(
            original_query=query,
            query_type=query_type,
            components=components,
            ctes=ctes,
            tables=tables,
            joins=joins,
            where_clauses=where_clauses,
            order_by=order_by,
            group_by=group_by,
            limit=limit,
            has_cross_join='CROSS JOIN' in query_upper,
            has_subquery=query_upper.count('SELECT') > 1,
            line_mapping=line_mapping
        )
    
    def _extract_components(self, query: str, lines: List[str]) -> List[QueryComponent]:
        """Loops sequentially capturing everything between major clause keywords into isolated segments."""
        components = []
        # Structural keys that initiate a new logical segment
        keywords = ['WITH', 'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT JOIN',
                   'RIGHT JOIN', 'INNER JOIN', 'CROSS JOIN', 'GROUP BY',
                   'HAVING', 'ORDER BY', 'LIMIT', 'UNION']
        
        current_component = None
        current_start = 1
        
        # Loop over every line string array looking for leading keywords
        for i, line in enumerate(lines, 1):
            line_upper = line.strip().upper()
            for kw in keywords:
                # If a line opens with a keyword, terminate the previous block and start a new one
                if line_upper.startswith(kw):
                    if current_component:
                        # Append the closed component history 
                        components.append(QueryComponent(
                            component_type=current_component,
                            content='\n'.join(lines[current_start-1:i-1]),
                            start_line=current_start,
                            end_line=i - 1
                        ))
                    current_component = kw
                    current_start = i
                    break
        
        # When array finishes, append the dangling active component block
        if current_component:
            components.append(QueryComponent(
                component_type=current_component,
                content='\n'.join(lines[current_start-1:]),
                start_line=current_start,
                end_line=len(lines)
            ))
        
        return components
    
    def _extract_ctes(self, query: str, lines: List[str]) -> List[Dict[str, Any]]:
        """Identify instances of Common Table Expressions 'WITH xyz AS (...)'"""
        ctes = []
        # Fast exit if query contains no WITH statement
        if 'WITH' not in query.upper():
            return ctes
        
        # Regex seeking word before an AS ( clause initiation
        pattern = r'(\w+)\s+AS\s*\('
        for match in re.finditer(pattern, query, re.IGNORECASE):
            cte_name = match.group(1)
            ctes.append({'name': cte_name, 'start_line': 0})
        
        return ctes
    
    def _extract_tables(self, query: str) -> List[str]:
        """Simple regex extraction mapping table names directly following a FROM or JOIN keyword."""
        tables = []
        pattern = r'(?:FROM|JOIN)\s+(\w+)'
        for match in re.finditer(pattern, query, re.IGNORECASE):
            tables.append(match.group(1))
        return list(set(tables))
    
    def _extract_joins(self, query: str, lines: List[str]) -> List[Dict[str, Any]]:
        """Finds all JOIN logic mapped to precise original line tracking details."""
        joins = []
        join_keywords = ['CROSS JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'JOIN']
        
        for i, line in enumerate(lines, 1):
            line_upper = line.upper()
            for jk in join_keywords:
                # Match line against dictionary of known join styles
                if jk in line_upper:
                    joins.append({'type': jk, 'line': i, 'content': line.strip()})
                    break
        return joins
    
    def _extract_where_clauses(self, query: str) -> List[str]:
        """Hunts for filtering condition content positioned between WHERE block and NEXT logical block."""
        clauses = []
        # Dot-all multi-line regex catching text after WHERE safely stopping
        match = re.search(r'WHERE\s+(.+?)(?:GROUP BY|ORDER BY|LIMIT|$)', 
                          query, re.IGNORECASE | re.DOTALL)
        if match:
            clauses.append(match.group(1).strip())
        return clauses
    
    def _extract_order_by(self, query: str) -> Optional[str]:
        """Hunts for ORDER BY logic and cuts it out."""
        match = re.search(r'ORDER BY\s+(.+?)(?:LIMIT|$)', query, re.IGNORECASE | re.DOTALL)
        return match.group(0).strip() if match else None
    
    def _extract_group_by(self, query: str) -> Optional[str]:
        """Hunts for GROUP BY aggregation rules."""
        match = re.search(r'GROUP BY\s+(.+?)(?:HAVING|ORDER BY|LIMIT|$)', 
                          query, re.IGNORECASE | re.DOTALL)
        return match.group(0).strip() if match else None
    
    def _extract_limit(self, query: str) -> Optional[int]:
        """Locates strict explicit numerical output boundaries."""
        match = re.search(r'LIMIT\s+(\d+)', query, re.IGNORECASE)
        # Type convert bound to int if caught successfully
        return int(match.group(1)) if match else None
    
    def find_expensive_patterns(self, query: str) -> List[Dict[str, Any]]:
        """Sweeps every line of the raw query validating it against the bad-pattern dictionary."""
        patterns_found = []
        lines = query.split('\n')
        
        for i, line in enumerate(lines, 1):
            line_upper = line.upper()
            for pattern in self.expensive_patterns:
                if pattern in line_upper:
                    # Logs any matching failure payload along with derived severity mapping
                    patterns_found.append({
                        'pattern': pattern,
                        'line': i,
                        'content': line.strip(),
                        'severity': self._get_pattern_severity(pattern)
                    })
        return patterns_found
    
    def _get_pattern_severity(self, pattern: str) -> str:
        """Determines logic impact scale depending on known architecture pain thresholds."""
        # Catastrophic behaviors guaranteeing extreme latency blocks
        high_severity = ['CROSS JOIN', 'RECURSIVE', 'generate_series']
        # Risky but standard behaviors affecting scale metrics
        medium_severity = ['PERCENTILE_CONT', 'DISTINCT', 'ORDER BY']
        
        if pattern in high_severity:
            return 'HIGH'
        elif pattern in medium_severity:
            return 'MEDIUM'
        # Default severity output
        return 'LOW'
