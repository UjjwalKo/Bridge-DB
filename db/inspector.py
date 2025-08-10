from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Float, inspect
from sqlalchemy.types import TypeEngine
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class SchemaInspector:
    def __init__(self, connector):
        self.connector = connector
    
    def inspect_table(self, connection_id: str, database: str, table: str) -> Dict[str, Any]:
        """
        Inspect a table schema and return column information
        """
        if connection_id not in self.connector.engines:
            raise ValueError(f"No connection with ID: {connection_id}")
        
        engine_info = self.connector.engines[connection_id]
        db_type = engine_info["type"]
        engine = engine_info["engine"]
        config = engine_info["config"]
        
        try:
            # Get connection to the specific database
            if db_type in ["mysql", "postgresql", "sqlserver"]:
                conn_string = self.connector.get_connection_string(db_type, config)
                db_engine = create_engine(f"{conn_string}/{database}")
            else:  # Oracle
                db_engine = engine
                schema = database.upper()
            
            # Get inspector
            inspector = inspect(db_engine)
            
            # Get column info
            if db_type == "oracle":
                columns = inspector.get_columns(table, schema=schema)
            else:
                columns = inspector.get_columns(table)
            
            # Format column information
            column_info = []
            for col in columns:
                column_info.append({
                    "name": col["name"],
                    "type": str(col["type"]),
                    "nullable": col["nullable"],
                    "default": str(col.get("default", ""))
                })
            
            # Get primary keys
            if db_type == "oracle":
                pk_constraint = inspector.get_pk_constraint(table, schema=schema)
            else:
                pk_constraint = inspector.get_pk_constraint(table)
            
            # Get indices
            if db_type == "oracle":
                indices = inspector.get_indexes(table, schema=schema)
            else:
                indices = inspector.get_indexes(table)
            
            return {
                "table": table,
                "columns": column_info,
                "primary_keys": pk_constraint.get("constrained_columns", []),
                "indices": [{"name": idx["name"], "columns": idx["column_names"]} for idx in indices]
            }
        except Exception as e:
            logger.error(f"Error inspecting table schema: {str(e)}")
            raise
    
    def map_data_type(self, source_type: str, source_db_type: str, target_db_type: str) -> str:
        """
        Map data types between different database engines
        """
        source_type = source_type.lower()
        
        # Define type mappings
        type_mappings = {
            # MySQL to others
            ("mysql", "postgresql"): {
                "int": "integer",
                "bigint": "bigint",
                "varchar": "varchar",
                "text": "text",
                "datetime": "timestamp",
                "timestamp": "timestamp",
                "float": "float",
                "double": "double precision",
                "decimal": "decimal",
                "tinyint(1)": "boolean"
            },
            ("mysql", "oracle"): {
                "int": "NUMBER(10)",
                "bigint": "NUMBER(19)",
                "varchar": "VARCHAR2",
                "text": "CLOB",
                "datetime": "TIMESTAMP",
                "timestamp": "TIMESTAMP",
                "float": "FLOAT",
                "double": "FLOAT",
                "decimal": "NUMBER",
                "tinyint(1)": "NUMBER(1)"
            },
            ("mysql", "sqlserver"): {
                "int": "INT",
                "bigint": "BIGINT",
                "varchar": "VARCHAR",
                "text": "TEXT",
                "datetime": "DATETIME",
                "timestamp": "DATETIME",
                "float": "FLOAT",
                "double": "FLOAT",
                "decimal": "DECIMAL",
                "tinyint(1)": "BIT"
            },
            
            # PostgreSQL to others
            ("postgresql", "mysql"): {
                "integer": "INT",
                "bigint": "BIGINT",
                "varchar": "VARCHAR",
                "text": "TEXT",
                "timestamp": "DATETIME",
                "float": "FLOAT",
                "double precision": "DOUBLE",
                "numeric": "DECIMAL",
                "boolean": "TINYINT(1)"
            },
            ("postgresql", "oracle"): {
                "integer": "NUMBER(10)",
                "bigint": "NUMBER(19)",
                "varchar": "VARCHAR2",
                "text": "CLOB",
                "timestamp": "TIMESTAMP",
                "float": "FLOAT",
                "double precision": "FLOAT",
                "numeric": "NUMBER",
                "boolean": "NUMBER(1)"
            },
            ("postgresql", "sqlserver"): {
                "integer": "INT",
                "bigint": "BIGINT",
                "varchar": "VARCHAR",
                "text": "TEXT",
                "timestamp": "DATETIME",
                "float": "FLOAT",
                "double precision": "FLOAT",
                "numeric": "DECIMAL",
                "boolean": "BIT"
            },
            
            # Oracle to others
            ("oracle", "mysql"): {
                "number(10)": "INT",
                "number(19)": "BIGINT",
                "varchar2": "VARCHAR",
                "clob": "TEXT",
                "timestamp": "DATETIME",
                "float": "FLOAT",
                "number": "DECIMAL",
                "number(1)": "TINYINT(1)"
            },
            ("oracle", "postgresql"): {
                "number(10)": "INTEGER",
                "number(19)": "BIGINT",
                "varchar2": "VARCHAR",
                "clob": "TEXT",
                "timestamp": "TIMESTAMP",
                "float": "FLOAT",
                "number": "NUMERIC",
                "number(1)": "BOOLEAN"
            },
            ("oracle", "sqlserver"): {
                "number(10)": "INT",
                "number(19)": "BIGINT",
                "varchar2": "VARCHAR",
                "clob": "TEXT",
                "timestamp": "DATETIME",
                "float": "FLOAT",
                "number": "DECIMAL",
                "number(1)": "BIT"
            },
            
            # SQL Server to others
            ("sqlserver", "mysql"): {
                "int": "INT",
                "bigint": "BIGINT",
                "varchar": "VARCHAR",
                "text": "TEXT",
                "datetime": "DATETIME",
                "float": "FLOAT",
                "decimal": "DECIMAL",
                "bit": "TINYINT(1)"
            },
            ("sqlserver", "postgresql"): {
                "int": "INTEGER",
                "bigint": "BIGINT",
                "varchar": "VARCHAR",
                "text": "TEXT",
                "datetime": "TIMESTAMP",
                "float": "FLOAT",
                "decimal": "NUMERIC",
                "bit": "BOOLEAN"
            },
            ("sqlserver", "oracle"): {
                "int": "NUMBER(10)",
                "bigint": "NUMBER(19)",
                "varchar": "VARCHAR2",
                "text": "CLOB",
                "datetime": "TIMESTAMP",
                "float": "FLOAT",
                "decimal": "NUMBER",
                "bit": "NUMBER(1)"
            }
        }
        
        # If same database type, no need to convert
        if source_db_type == target_db_type:
            return source_type
        
        # Extract base type without length/precision specifiers
        base_type = source_type.split("(")[0].strip().lower()
        
        # Get the mapping dictionary for this source->target combination
        mapping = type_mappings.get((source_db_type, target_db_type), {})
        
        # Try to find the mapped type
        mapped_type = mapping.get(base_type)
        if mapped_type:
            return mapped_type
            
        # If we have a specific type with precision, try to map the base type
        if "(" in source_type:
            base_mapped = mapping.get(base_type)
            if base_mapped:
                precision_part = source_type.split("(")[1].split(")")[0]
                return f"{base_mapped}({precision_part})"
        
        # Default: return the original type as fallback
        logger.warning(f"No mapping found for {source_type} from {source_db_type} to {target_db_type}")
        return source_type
    
    def generate_create_table_sql(self, table_schema: Dict[str, Any], 
                                  source_db_type: str, target_db_type: str, 
                                  target_table_name: Optional[str] = None) -> str:
        """
        Generate CREATE TABLE SQL statement for the target database
        """
        table_name = target_table_name or table_schema["table"]
        columns = table_schema["columns"]
        primary_keys = table_schema["primary_keys"]
        
        # Start SQL statement
        if target_db_type == "oracle":
            sql = f"CREATE TABLE {table_name} (\n"
        else:
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        
        # Add columns
        column_defs = []
        for col in columns:
            name = col["name"]
            col_type = self.map_data_type(col["type"], source_db_type, target_db_type)
            nullable = "" if col["nullable"] else "NOT NULL"
            
            # Handle default values
            default = col.get("default", "")
            if default and default.lower() != "null" and default != "":
                default = f"DEFAULT {default}"
            else:
                default = ""
                
            column_defs.append(f"    {name} {col_type} {nullable} {default}".strip())
        
        # Add primary key constraint if available
        if primary_keys:
            pk_names = ", ".join(primary_keys)
            column_defs.append(f"    PRIMARY KEY ({pk_names})")
        
        # Finish SQL statement
        sql += ",\n".join(column_defs)
        sql += "\n)"
        
        # Add specific database engine syntax if needed
        if target_db_type == "mysql":
            sql += " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        
        return sql
    
    def sample_data(self, connection_id: str, database: str, table: str, limit: int = 10) -> pd.DataFrame:
        """
        Get a sample of data from the specified table
        """
        if connection_id not in self.connector.engines:
            raise ValueError(f"No connection with ID: {connection_id}")
        
        engine_info = self.connector.engines[connection_id]
        db_type = engine_info["type"]
        engine = engine_info["engine"]
        config = engine_info["config"]
        
        try:
            # Get connection to the specific database
            if db_type in ["mysql", "postgresql", "sqlserver"]:
                conn_string = self.connector.get_connection_string(db_type, config)
                db_engine = create_engine(f"{conn_string}/{database}")
                query = f"SELECT * FROM {table} LIMIT {limit}"
                
                # SQL Server uses different syntax for LIMIT
                if db_type == "sqlserver":
                    query = f"SELECT TOP {limit} * FROM {table}"
                    
                df = pd.read_sql(query, db_engine)
            else:  # Oracle
                schema = database.upper()
                query = f"SELECT * FROM {schema}.{table} WHERE ROWNUM <= {limit}"
                df = pd.read_sql(query, engine)
            
            return df
        except Exception as e:
            logger.error(f"Error sampling data: {str(e)}")
            raise