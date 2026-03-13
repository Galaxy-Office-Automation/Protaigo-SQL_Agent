import psycopg2
from typing import Dict, List, Tuple, Optional, Any
from analyzer.metadata import MetadataExtractor

class SchemaAnalyzer:
    """Analyzes table schemas to find relational anchoring keys for stratified sampling."""
    
    def __init__(self, db_config: Dict):
        self.extractor = MetadataExtractor(db_config)
        
    def find_anchor_key(self, used_tables: List[str]) -> Optional[Dict[str, Any]]:
        """
        Dynamically finds the best table and key to use as a sampling anchor.
        Returns:
            dict with:
                'anchor_table': str
                'anchor_key': str
                'related_tables': dict mapping table_name -> foreign_key_column
        """
        if not used_tables:
            return None
            
        tables_meta = self.extractor.get_tables_from_query(used_tables)
        if not tables_meta:
            return None
            
        # Build a graph of relationships
        # key: referenced_table, value: list of (referencing_table, referencing_column, referenced_column)
        incoming_fks = {t: [] for t in used_tables}
        
        for t_name, meta in tables_meta.items():
            for fk in meta.foreign_keys:
                ref_table = fk['references_table']
                if ref_table in incoming_fks:
                    incoming_fks[ref_table].append({
                        'table': t_name,
                        'fk_column': fk['column'],
                        'pk_column': fk['references_column']
                    })
                    
        # Find the table with the most incoming relationships within the used_tables
        best_anchor = None
        max_incoming = 0
        
        for t_name, incoming in incoming_fks.items():
            if len(incoming) > max_incoming:
                best_anchor = t_name
                max_incoming = len(incoming)
                
        if best_anchor and max_incoming > 0:
            # We found an explicit anchor via foreign keys
            anchor_meta = tables_meta[best_anchor]
            # Use the column that is most frequently referenced
            # Count references per column
            col_refs = {}
            for inc in incoming_fks[best_anchor]:
                pk_col = inc['pk_column']
                col_refs[pk_col] = col_refs.get(pk_col, 0) + 1
                
            best_pk_col = max(col_refs, key=col_refs.get)
            
            related = {}
            # We map every table to the column it uses to point to best_anchor
            for inc in incoming_fks[best_anchor]:
                if inc['pk_column'] == best_pk_col:
                    related[inc['table']] = inc['fk_column']
                    
            return {
                'anchor_table': best_anchor,
                'anchor_key': best_pk_col,
                'related_tables': related,
                'method': 'Explicit Foreign Keys'
            }
            
        # Fallback: Implicit detection based on identical column names (e.g. 'account_id' in both)
        # We look for columns ending in '_id' or 'id' that appear in multiple tables
        col_presence = {} # maps col_name -> list of tables it appears in
        for t_name, meta in tables_meta.items():
            for col in meta.columns:
                cname = col['name']
                if cname.endswith('id'):
                    if cname not in col_presence:
                        col_presence[cname] = []
                    col_presence[cname].append(t_name)
                    
        # Find the column shared by the most tables
        best_implicit_col = None
        max_shared = 1 # Must be shared by at least 2 tables
        
        for cname, tables in col_presence.items():
            if len(tables) > max_shared:
                best_implicit_col = cname
                max_shared = len(tables)
                
        if best_implicit_col:
            # Pick the "primary" table for this column arbitrarily or heuristically
            # Priority: A table whose name matches the prefix of the column (e.g. 'branch_id' -> 'branches')
            prefix = best_implicit_col.replace('_id', '').replace('id', '')
            best_implicit_anchor = tables[0]
            for t in tables:
                if t.startswith(prefix) or prefix.startswith(t):
                    best_implicit_anchor = t
                    break
                    
            related = {t: best_implicit_col for t in col_presence[best_implicit_col] if t != best_implicit_anchor}
            
            return {
                'anchor_table': best_implicit_anchor,
                'anchor_key': best_implicit_col,
                'related_tables': related,
                'method': 'Implicit Column Name Match'
            }
            
        return None
