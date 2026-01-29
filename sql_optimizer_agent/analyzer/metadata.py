"""
Metadata Extractor - Extracts database schema, indexes, and statistics
"""

import psycopg2
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import sys
sys.path.insert(0, '/home/galaxy/DB_setup/sql_optimizer_agent')
from config import DB_CONFIG


@dataclass
class TableMetadata:
    """Metadata for a database table"""
    schema: str
    name: str
    row_count: int
    size_bytes: int
    columns: List[Dict[str, Any]]
    indexes: List[Dict[str, Any]]
    primary_key: Optional[str]
    foreign_keys: List[Dict[str, Any]]


@dataclass
class DatabaseMetadata:
    """Complete database metadata"""
    tables: Dict[str, TableMetadata]
    all_indexes: List[Dict[str, Any]]
    database_size: int


class MetadataExtractor:
    """Extracts database metadata for query optimization"""
    
    def __init__(self, db_config: Dict = None):
        self.db_config = db_config or DB_CONFIG
        self._conn = None
    
    def _get_connection(self):
        """Get database connection"""
        if not self._conn or self._conn.closed:
            self._conn = psycopg2.connect(**self.db_config)
        return self._conn
    
    def close(self):
        """Close database connection"""
        if self._conn and not self._conn.closed:
            self._conn.close()
    
    def get_table_metadata(self, table_name: str, 
                           schema: str = 'public') -> Optional[TableMetadata]:
        """Get metadata for a specific table"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Get row count
            cursor.execute(f"""
                SELECT reltuples::BIGINT 
                FROM pg_class 
                WHERE relname = %s
            """, (table_name,))
            row_result = cursor.fetchone()
            row_count = row_result[0] if row_result else 0
            
            # Get table size
            cursor.execute(f"""
                SELECT pg_total_relation_size(%s::regclass)
            """, (f"{schema}.{table_name}",))
            size_result = cursor.fetchone()
            size_bytes = size_result[0] if size_result else 0
            
            # Get columns
            cursor.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, table_name))
            columns = [
                {
                    'name': row[0],
                    'type': row[1],
                    'nullable': row[2] == 'YES',
                    'default': row[3]
                }
                for row in cursor.fetchall()
            ]
            
            # Get indexes
            cursor.execute("""
                SELECT 
                    i.relname as index_name,
                    a.attname as column_name,
                    ix.indisunique as is_unique,
                    ix.indisprimary as is_primary
                FROM pg_class t
                JOIN pg_index ix ON t.oid = ix.indrelid
                JOIN pg_class i ON i.oid = ix.indexrelid
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                WHERE t.relname = %s AND t.relkind = 'r'
            """, (table_name,))
            indexes = [
                {
                    'name': row[0],
                    'column': row[1],
                    'unique': row[2],
                    'primary': row[3]
                }
                for row in cursor.fetchall()
            ]
            
            # Get primary key
            primary_key = None
            for idx in indexes:
                if idx['primary']:
                    primary_key = idx['column']
                    break
            
            # Get foreign keys
            cursor.execute("""
                SELECT
                    kcu.column_name,
                    ccu.table_name AS foreign_table,
                    ccu.column_name AS foreign_column
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage ccu
                    ON ccu.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_name = %s
            """, (table_name,))
            foreign_keys = [
                {
                    'column': row[0],
                    'references_table': row[1],
                    'references_column': row[2]
                }
                for row in cursor.fetchall()
            ]
            
            return TableMetadata(
                schema=schema,
                name=table_name,
                row_count=row_count,
                size_bytes=size_bytes,
                columns=columns,
                indexes=indexes,
                primary_key=primary_key,
                foreign_keys=foreign_keys
            )
            
        except psycopg2.Error as e:
            print(f"Error getting metadata for {table_name}: {e}")
            return None
        finally:
            cursor.close()
    
    def get_tables_from_query(self, tables: List[str]) -> Dict[str, TableMetadata]:
        """Get metadata for all tables mentioned in a query"""
        metadata = {}
        for table in tables:
            # Handle schema.table format
            if '.' in table:
                schema, name = table.split('.', 1)
            else:
                schema, name = 'public', table
            
            table_meta = self.get_table_metadata(name, schema)
            if table_meta:
                metadata[table] = table_meta
        
        return metadata
    
    def get_index_suggestions(self, table_name: str, 
                               columns: List[str]) -> List[Dict[str, Any]]:
        """Suggest indexes for given columns"""
        table_meta = self.get_table_metadata(table_name)
        if not table_meta:
            return []
        
        suggestions = []
        existing_indexed_cols = {idx['column'] for idx in table_meta.indexes}
        
        for col in columns:
            if col not in existing_indexed_cols:
                suggestions.append({
                    'table': table_name,
                    'column': col,
                    'suggestion': f"CREATE INDEX idx_{table_name}_{col} ON {table_name}({col});",
                    'reason': f"Column '{col}' is used in query but not indexed"
                })
        
        return suggestions
    
    def estimate_query_rows(self, table_name: str, 
                            filter_column: str = None,
                            filter_value: Any = None) -> int:
        """Estimate number of rows that would be returned"""
        table_meta = self.get_table_metadata(table_name)
        if not table_meta:
            return 0
        
        # Without filter, return total rows
        if not filter_column:
            return table_meta.row_count
        
        # With filter, try to estimate based on column statistics
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT n_distinct
                FROM pg_stats
                WHERE tablename = %s AND attname = %s
            """, (table_name, filter_column))
            
            result = cursor.fetchone()
            if result and result[0]:
                n_distinct = abs(result[0])
                if n_distinct < 1:
                    # n_distinct is a ratio
                    n_distinct = int(table_meta.row_count * n_distinct)
                
                # Estimate rows per distinct value
                return int(table_meta.row_count / max(n_distinct, 1))
            
            return table_meta.row_count
            
        except psycopg2.Error:
            return table_meta.row_count
        finally:
            cursor.close()
