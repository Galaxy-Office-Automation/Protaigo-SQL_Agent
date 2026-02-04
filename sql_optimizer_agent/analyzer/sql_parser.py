"""
SQL Parser - Parses and analyzes SQL query structure (no external dependencies)
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
import re


@dataclass
class QueryComponent:
    """Represents a component of a SQL query with line information"""
    component_type: str
    content: str
    start_line: int
    end_line: int
    tokens: List[str] = field(default_factory=list)


@dataclass
class ParsedQuery:
    """Complete parsed representation of a SQL query"""
    original_query: str          # The raw SQL
    query_type: str              # SELECT, INSERT, UPDATE, DELETE
    components: List[QueryComponent] # List of query components
    ctes: List[Dict[str, Any]]       # List of CTEs(common table expressions)
    tables: List[str]                # List of tables
    joins: List[Dict[str, Any]]      # List of joins
    where_clauses: List[str]         # List of WHERE clauses
    order_by: Optional[str]          # ORDER BY clause
    group_by: Optional[str]          # GROUP BY clause
    limit: Optional[int]             # LIMIT clause
    has_cross_join: bool             # Whether the query has a CROSS JOIN
    has_subquery: bool               # Whether the query has a subquery
    line_mapping: Dict[int, str]     # Mapping of line numbers to content


class SQLParser:
    """Parses SQL queries into structured components with line information"""
    
    def __init__(self):
        self.expensive_patterns = [
            'CROSS JOIN', 'PERCENTILE_CONT', 'PERCENTILE_DISC',
            'generate_series', 'pg_sleep', 'ORDER BY', 'DISTINCT',
            'UNION ALL', 'RECURSIVE'
        ]
    
    def parse(self, query: str) -> ParsedQuery:
        """Parse a SQL query into structured components"""
        lines = query.split('\n')
        line_mapping = {i + 1: line for i, line in enumerate(lines)}
        query_upper = query.upper()
        
        # Determine query type
        query_type = "SELECT"
        for qt in ["INSERT", "UPDATE", "DELETE"]:
            if qt in query_upper:
                query_type = qt
                break
        
        components = self._extract_components(query, lines)
        ctes = self._extract_ctes(query, lines)
        tables = self._extract_tables(query)
        joins = self._extract_joins(query, lines)
        where_clauses = self._extract_where_clauses(query)
        order_by = self._extract_order_by(query)
        group_by = self._extract_group_by(query)
        limit = self._extract_limit(query)
        
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
        """Extract query components with line numbers"""
        components = []
        keywords = ['WITH', 'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT JOIN',
                   'RIGHT JOIN', 'INNER JOIN', 'CROSS JOIN', 'GROUP BY',
                   'HAVING', 'ORDER BY', 'LIMIT', 'UNION']
        
        current_component = None
        current_start = 1
        
        for i, line in enumerate(lines, 1):
            line_upper = line.strip().upper()
            for kw in keywords:
                if line_upper.startswith(kw):
                    if current_component:
                        components.append(QueryComponent(
                            component_type=current_component,
                            content='\n'.join(lines[current_start-1:i-1]),
                            start_line=current_start,
                            end_line=i - 1
                        ))
                    current_component = kw
                    current_start = i
                    break
        
        if current_component:
            components.append(QueryComponent(
                component_type=current_component,
                content='\n'.join(lines[current_start-1:]),
                start_line=current_start,
                end_line=len(lines)
            ))
        
        return components
    
    def _extract_ctes(self, query: str, lines: List[str]) -> List[Dict[str, Any]]:
        """Extract Common Table Expressions"""
        ctes = []
        if 'WITH' not in query.upper():
            return ctes
        
        pattern = r'(\w+)\s+AS\s*\('
        for match in re.finditer(pattern, query, re.IGNORECASE):
            cte_name = match.group(1)
            ctes.append({'name': cte_name, 'start_line': 0})
        
        return ctes
    
    def _extract_tables(self, query: str) -> List[str]:
        """Extract table names from FROM clause"""
        tables = []
        pattern = r'FROM\s+(\w+)'
        for match in re.finditer(pattern, query, re.IGNORECASE):
            tables.append(match.group(1))
        return tables
    
    def _extract_joins(self, query: str, lines: List[str]) -> List[Dict[str, Any]]:
        """Extract JOIN clauses"""
        joins = []
        join_keywords = ['CROSS JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'JOIN']
        
        for i, line in enumerate(lines, 1):
            line_upper = line.upper()
            for jk in join_keywords:
                if jk in line_upper:
                    joins.append({'type': jk, 'line': i, 'content': line.strip()})
                    break
        return joins
    
    def _extract_where_clauses(self, query: str) -> List[str]:
        """Extract WHERE clause"""
        clauses = []
        match = re.search(r'WHERE\s+(.+?)(?:GROUP BY|ORDER BY|LIMIT|$)', 
                          query, re.IGNORECASE | re.DOTALL)
        if match:
            clauses.append(match.group(1).strip())
        return clauses
    
    def _extract_order_by(self, query: str) -> Optional[str]:
        """Extract ORDER BY clause"""
        match = re.search(r'ORDER BY\s+(.+?)(?:LIMIT|$)', query, re.IGNORECASE | re.DOTALL)
        return match.group(0).strip() if match else None
    
    def _extract_group_by(self, query: str) -> Optional[str]:
        """Extract GROUP BY clause"""
        match = re.search(r'GROUP BY\s+(.+?)(?:HAVING|ORDER BY|LIMIT|$)', 
                          query, re.IGNORECASE | re.DOTALL)
        return match.group(0).strip() if match else None
    
    def _extract_limit(self, query: str) -> Optional[int]:
        """Extract LIMIT value"""
        match = re.search(r'LIMIT\s+(\d+)', query, re.IGNORECASE)
        return int(match.group(1)) if match else None
    
    def find_expensive_patterns(self, query: str) -> List[Dict[str, Any]]:
        """Find potentially expensive patterns in the query"""
        patterns_found = []
        lines = query.split('\n')
        
        for i, line in enumerate(lines, 1):
            line_upper = line.upper()
            for pattern in self.expensive_patterns:
                if pattern in line_upper:
                    patterns_found.append({
                        'pattern': pattern,
                        'line': i,
                        'content': line.strip(),
                        'severity': self._get_pattern_severity(pattern)
                    })
        return patterns_found
    
    def _get_pattern_severity(self, pattern: str) -> str:
        """Get severity level for an expensive pattern"""
        high_severity = ['CROSS JOIN', 'RECURSIVE', 'generate_series']
        medium_severity = ['PERCENTILE_CONT', 'DISTINCT', 'ORDER BY']
        
        if pattern in high_severity:
            return 'HIGH'
        elif pattern in medium_severity:
            return 'MEDIUM'
        return 'LOW'
