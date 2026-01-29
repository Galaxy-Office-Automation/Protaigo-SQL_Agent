"""
Execution Plan Parser - Analyzes PostgreSQL EXPLAIN ANALYZE output
"""

import psycopg2
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
import re
import sys
sys.path.insert(0, '/home/galaxy/DB_setup/sql_optimizer_agent')
from config import DB_CONFIG


@dataclass
class PlanNode:
    """Represents a node in the execution plan tree"""
    node_type: str  # Seq Scan, Index Scan, Hash Join, etc.
    relation: Optional[str]  # Table name if applicable
    estimated_rows: float
    actual_rows: float
    estimated_cost: float
    actual_time: float  # in milliseconds
    loops: int
    filter_condition: Optional[str]
    index_name: Optional[str]
    children: List['PlanNode'] = field(default_factory=list)
    raw_text: str = ""


@dataclass
class ExecutionPlan:
    """Complete execution plan analysis"""
    query: str
    planning_time: float  # ms
    execution_time: float  # ms
    total_time: float  # ms
    root_node: Optional[PlanNode]
    all_nodes: List[PlanNode]
    seq_scans: List[PlanNode]
    index_scans: List[PlanNode]
    hash_operations: List[PlanNode]
    sort_operations: List[PlanNode]
    nested_loops: List[PlanNode]
    bottlenecks: List[Dict[str, Any]]


class ExecutionPlanParser:
    """Parses PostgreSQL EXPLAIN ANALYZE output"""
    
    def __init__(self, db_config: Dict = None):
        self.db_config = db_config or DB_CONFIG
    
    def get_execution_plan(self, query: str) -> ExecutionPlan:
        """Execute EXPLAIN ANALYZE and parse the result"""
        explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {query}"
        
        try:
            conn = psycopg2.connect(**self.db_config)
            # Set statement timeout to prevent long-running explains
            cursor = conn.cursor()
            cursor.execute("SET statement_timeout = '60s'")
            cursor.execute(explain_query)
            plan_rows = cursor.fetchall()
            cursor.close()
            conn.close()
            
            plan_text = '\n'.join([row[0] for row in plan_rows])
            return self._parse_plan_text(query, plan_text)
            
        except psycopg2.Error as e:
            # Return empty plan on error
            return ExecutionPlan(
                query=query,
                planning_time=0,
                execution_time=0,
                total_time=0,
                root_node=None,
                all_nodes=[],
                seq_scans=[],
                index_scans=[],
                hash_operations=[],
                sort_operations=[],
                nested_loops=[],
                bottlenecks=[{'error': str(e)}]
            )
    
    def _parse_plan_text(self, query: str, plan_text: str) -> ExecutionPlan:
        """Parse the text output of EXPLAIN ANALYZE"""
        lines = plan_text.split('\n')
        
        all_nodes = []
        seq_scans = []
        index_scans = []
        hash_operations = []
        sort_operations = []
        nested_loops = []
        
        planning_time = 0.0
        execution_time = 0.0
        
        for line in lines:
            # Extract planning and execution times
            if 'Planning Time:' in line:
                match = re.search(r'Planning Time: ([\d.]+) ms', line)
                if match:
                    planning_time = float(match.group(1))
            elif 'Execution Time:' in line:
                match = re.search(r'Execution Time: ([\d.]+) ms', line)
                if match:
                    execution_time = float(match.group(1))
            
            # Parse plan nodes
            node = self._parse_node_line(line)
            if node:
                all_nodes.append(node)
                
                # Categorize nodes
                node_type_upper = node.node_type.upper()
                if 'SEQ SCAN' in node_type_upper:
                    seq_scans.append(node)
                elif 'INDEX' in node_type_upper:
                    index_scans.append(node)
                elif 'HASH' in node_type_upper:
                    hash_operations.append(node)
                elif 'SORT' in node_type_upper:
                    sort_operations.append(node)
                elif 'NESTED LOOP' in node_type_upper:
                    nested_loops.append(node)
        
        # Identify bottlenecks
        bottlenecks = self._identify_bottlenecks(all_nodes, execution_time)
        
        return ExecutionPlan(
            query=query,
            planning_time=planning_time,
            execution_time=execution_time,
            total_time=planning_time + execution_time,
            root_node=all_nodes[0] if all_nodes else None,
            all_nodes=all_nodes,
            seq_scans=seq_scans,
            index_scans=index_scans,
            hash_operations=hash_operations,
            sort_operations=sort_operations,
            nested_loops=nested_loops,
            bottlenecks=bottlenecks
        )
    
    def _parse_node_line(self, line: str) -> Optional[PlanNode]:
        """Parse a single line of the execution plan"""
        # Pattern: NodeType on table  (cost=X..Y rows=Z width=W) (actual time=A..B rows=C loops=D)
        pattern = r'->?\s*(\w+(?:\s+\w+)*)\s+(?:on\s+(\w+))?\s*\(cost=([\d.]+)\.\.([\d.]+)\s+rows=(\d+)'
        match = re.search(pattern, line)
        
        if not match:
            # Try simpler pattern for top-level nodes
            simple_pattern = r'(\w+(?:\s+\w+)*)\s+\(cost=([\d.]+)\.\.([\d.]+)\s+rows=(\d+)'
            match = re.search(simple_pattern, line)
            if match:
                node_type = match.group(1)
                relation = None
                estimated_cost = float(match.group(3))
                estimated_rows = float(match.group(4))
            else:
                return None
        else:
            node_type = match.group(1)
            relation = match.group(2)
            estimated_cost = float(match.group(4))
            estimated_rows = float(match.group(5))
        
        # Extract actual values
        actual_pattern = r'actual time=([\d.]+)\.\.([\d.]+)\s+rows=(\d+)\s+loops=(\d+)'
        actual_match = re.search(actual_pattern, line)
        
        if actual_match:
            actual_time = float(actual_match.group(2))
            actual_rows = float(actual_match.group(3))
            loops = int(actual_match.group(4))
        else:
            actual_time = 0
            actual_rows = 0
            loops = 1
        
        # Extract filter condition
        filter_pattern = r'Filter:\s*(.+?)(?:\s*Rows Removed|$)'
        filter_match = re.search(filter_pattern, line)
        filter_condition = filter_match.group(1) if filter_match else None
        
        # Extract index name
        index_pattern = r'Index.*?using\s+(\w+)'
        index_match = re.search(index_pattern, line, re.IGNORECASE)
        index_name = index_match.group(1) if index_match else None
        
        return PlanNode(
            node_type=node_type,
            relation=relation,
            estimated_rows=estimated_rows,
            actual_rows=actual_rows,
            estimated_cost=estimated_cost,
            actual_time=actual_time,
            loops=loops,
            filter_condition=filter_condition,
            index_name=index_name,
            raw_text=line.strip()
        )
    
    def _identify_bottlenecks(self, nodes: List[PlanNode], 
                               total_time: float) -> List[Dict[str, Any]]:
        """Identify performance bottlenecks from the execution plan"""
        bottlenecks = []
        
        for node in nodes:
            # Check for row estimation errors
            if node.estimated_rows > 0 and node.actual_rows > 0:
                ratio = node.actual_rows / node.estimated_rows
                if ratio > 10 or ratio < 0.1:
                    bottlenecks.append({
                        'type': 'ROW_ESTIMATION_ERROR',
                        'node': node.node_type,
                        'table': node.relation,
                        'estimated': node.estimated_rows,
                        'actual': node.actual_rows,
                        'ratio': ratio,
                        'severity': 'HIGH' if ratio > 100 else 'MEDIUM'
                    })
            
            # Check for expensive sequential scans
            if 'SEQ SCAN' in node.node_type.upper():
                if node.actual_rows > 10000:
                    bottlenecks.append({
                        'type': 'LARGE_SEQ_SCAN',
                        'node': node.node_type,
                        'table': node.relation,
                        'rows': node.actual_rows,
                        'time_ms': node.actual_time,
                        'severity': 'HIGH' if node.actual_rows > 100000 else 'MEDIUM'
                    })
            
            # Check for expensive sorts
            if 'SORT' in node.node_type.upper():
                if node.actual_time > 1000:  # > 1 second
                    bottlenecks.append({
                        'type': 'EXPENSIVE_SORT',
                        'node': node.node_type,
                        'rows': node.actual_rows,
                        'time_ms': node.actual_time,
                        'severity': 'HIGH'
                    })
            
            # Check for expensive hash operations
            if 'HASH' in node.node_type.upper():
                if node.actual_time > 5000:  # > 5 seconds
                    bottlenecks.append({
                        'type': 'EXPENSIVE_HASH',
                        'node': node.node_type,
                        'time_ms': node.actual_time,
                        'severity': 'MEDIUM'
                    })
            
            # Check for nested loops with many iterations
            if 'NESTED LOOP' in node.node_type.upper():
                if node.loops > 1000:
                    bottlenecks.append({
                        'type': 'EXPENSIVE_NESTED_LOOP',
                        'node': node.node_type,
                        'loops': node.loops,
                        'time_ms': node.actual_time * node.loops,
                        'severity': 'HIGH'
                    })
        
        # Sort by severity
        severity_order = {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2}
        bottlenecks.sort(key=lambda x: severity_order.get(x.get('severity', 'LOW'), 3))
        
        return bottlenecks
