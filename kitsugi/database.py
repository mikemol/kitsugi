import sqlite3
import os
import json

# ==============================================================================
#  Section 1: The Abstract Schema Definition (The Structure)
# ==============================================================================
# This list of dictionaries is the single source of truth for the database structure.

DATABASE_SCHEMA = [
    # --- Tables ---
    {
        'type': 'table', 'name': 'hash_index',
        'columns': [
            {'name': 'hash', 'type': 'TEXT', 'constraints': ['NOT NULL']},
            {'name': 'location', 'type': 'TEXT', 'constraints': ['NOT NULL']}
        ]
    },
    {
        'type': 'table', 'name': 'hash_graph',
        'columns': [
            {'name': 'parent_hash', 'type': 'TEXT', 'constraints': ['NOT NULL']},
            {'name': 'child_key', 'type': 'TEXT', 'constraints': ['NOT NULL']},
            {'name': 'child_hash', 'type': 'TEXT', 'constraints': ['NOT NULL']}
        ]
    },
    {
        'type': 'table', 'name': 'hash_to_data',
        'columns': [
            {'name': 'hash', 'type': 'TEXT', 'constraints': ['PRIMARY KEY']},
            {'name': 'data', 'type': 'TEXT', 'constraints': []}
        ]
    },
    {
        'type': 'table', 'name': 'reconstructed_docs',
        'columns': [
            {'name': 'doc_name', 'type': 'TEXT', 'constraints': ['PRIMARY KEY']},
            {'name': 'root_hash', 'type': 'TEXT', 'constraints': ['NOT NULL', 'UNIQUE']}
        ]
    },
    # --- Virtual Tables ---
    {
        'type': 'virtual_table', 'name': 'data_search_idx', 'module': 'fts5',
        'options': [
            {'name': 'hash', 'value': 'UNINDEXED'},
            {'name': 'data'},
            {'name': 'content', 'value': "'hash_to_data'"},
            {'name': 'content_rowid', 'value': "'rowid'"}
        ]
    },
    # --- Indices ---
    {
        'type': 'index', 'name': 'idx_hash_index_hash',
        'table': 'hash_index', 'columns': ['hash']
    },
    {
        'type': 'index', 'name': 'idx_hash_graph_parent',
        'table': 'hash_graph', 'columns': ['parent_hash']
    },
    {
        'type': 'index', 'name': 'idx_hash_graph_child',
        'table': 'hash_graph', 'columns': ['child_hash']
    }
]

# ==============================================================================
#  Section 2: The Implementation Morphism (The Translation)
# ==============================================================================

def translate_schema_to_sql(schema_list):
    """A morphism that translates a list of schema dictionaries into SQL strings."""
    sql_commands = []
    for schema in schema_list:
        if schema['type'] == 'table':
            cols = [f"{col['name']} {col['type']} {' '.join(col['constraints'])}".strip() for col in schema['columns']]
            command = f"CREATE TABLE IF NOT EXISTS {schema['name']} (\n    "
            command += ',\n    '.join(cols)
            command += '\n);'
            sql_commands.append(command)
        elif schema['type'] == 'virtual_table':
            opts = [f"{opt['name']}{'=' + str(opt['value']) if 'value' in opt else ''}" for opt in schema['options']]
            command = f"CREATE VIRTUAL TABLE IF NOT EXISTS {schema['name']} "
            command += f"USING {schema['module']}({', '.join(opts)});"
            sql_commands.append(command)
        elif schema['type'] == 'index':
            command = f"CREATE INDEX IF NOT EXISTS {schema['name']} "
            command += f"ON {schema['table']}({', '.join(schema['columns'])});"
            sql_commands.append(command)
    return sql_commands

# ==============================================================================
#  Section 3: The Declarative Query Engine (The Repository)
# ==============================================================================

class Repository:
    """Encapsulates all database interactions via a declarative request engine."""
    def __init__(self, connection):
        self.conn = connection

    def setup_schema(self):
        """Sets up all necessary tables and indices from the declarative schema."""
        print("  - Translating abstract schema to SQL...")
        sql_commands = translate_schema_to_sql(DATABASE_SCHEMA)
        print("  - Executing SQL to create database schema and indices...")
        cursor = self.conn.cursor()
        for command in sql_commands:
            cursor.execute(command)
        self.conn.commit()
        print("  - Database schema is up to date.")

    def execute(self, request):
        """The single, universal method to execute any data request."""
        cursor = self.conn.cursor()
        req_type = request.get('type')
        table = request.get('table')

        if req_type == 'QUERY':
            cols = ', '.join(request['select'])
            sql = f"SELECT {cols} FROM {table}"
            params = []

            if 'where' in request:
                where_clause, where_params = self._build_where_clause(request['where'])
                sql += f" WHERE {where_clause}"
                params.extend(where_params)
            if 'order_by' in request:
                sql += f" ORDER BY {request['order_by']}"
            if 'limit' in request:
                sql += f" LIMIT {request['limit']}"

            cursor.execute(sql, params)
            return cursor.fetchall()

        elif req_type == 'INSERT':
            data = request.get('data')
            if not data: return
            cols = list(data[0].keys())
            placeholders = ', '.join(['?'] * len(cols))
            ignore_clause = "OR IGNORE " if request.get('ignore') else ""
            sql = f"INSERT {ignore_clause}INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
            data_tuples = [tuple(row[col] for col in cols) for row in data]
            cursor.executemany(sql, data_tuples)
            return cursor.rowcount

        elif req_type == 'DELETE':
            sql = f"DELETE FROM {table}"
            params = []
            if 'where' in request:
                where_clause, where_params = self._build_where_clause(request['where'])
                sql += f" WHERE {where_clause}"
                params.extend(where_params)
            cursor.execute(sql, params)
            return cursor.rowcount
            
        elif req_type == 'REBUILD_FTS':
            # Special command for FTS population
            print("  - Populating search index...")
            cursor.execute(f"INSERT INTO {table}({table}, content='hash_to_data', content_rowid='rowid') VALUES('rebuild');")
            return cursor.rowcount

        else:
            raise ValueError(f"Unsupported request type: {req_type}")

    def _build_where_clause(self, where_data):
        """Helper to safely build WHERE clauses and parameter lists."""
        col = where_data['column']
        op = where_data['operator']
        val = where_data['value']
        
        if op.upper() == 'IN':
            if not isinstance(val, (list, tuple)) or not val:
                # Handle empty IN clause to avoid SQL errors
                return "0=1", [] # A condition that is always false
            placeholders = ', '.join('?' for _ in val)
            return f"{col} IN ({placeholders})", list(val)
        else:
            return f"{col} {op} ?", [val]

    def commit(self):
        self.conn.commit()

def connect_to_db(db_path, read_only=False):
    """Establishes a connection to the SQLite database."""
    if read_only and not os.path.exists(db_path):
        print(f"Error: Database file not found at '{db_path}'")
        return None
    
    try:
        if read_only:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            print(f"Connected to database '{db_path}' in read-only mode.")
        else:
            conn = sqlite3.connect(db_path)
            print(f"Connected to database '{db_path}'.")
        return conn
    except sqlite3.OperationalError as e:
        print(f"Error connecting to database at '{db_path}': {e}")
        return None
