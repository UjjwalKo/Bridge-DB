from sqlalchemy import create_engine, MetaData
from typing import Dict, Optional, List, Tuple, Any
import pandas as pd
import mysql.connector
import psycopg2
import pyodbc
import cx_Oracle
import time
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)

class DatabaseConnector:
    SUPPORTED_ENGINES = ["mysql", "postgresql", "oracle", "sqlserver"]
    
    def __init__(self):
        self.engines = {}
        self.connections = {}
        self.raw_connections = {}
    
    def get_connection_string(self, db_type: str, config: Dict[str, Any]) -> str:
        """Generate connection string based on database type and configuration"""
        if db_type == "mysql":
            return f"mysql+pymysql://{config['username']}:{config['password']}@{config['host']}:{config['port']}"
        elif db_type == "postgresql":
            return f"postgresql://{config['username']}:{config['password']}@{config['host']}:{config['port']}"
        elif db_type == "oracle":
            dsn = cx_Oracle.makedsn(config['host'], config['port'], service_name=config.get('service_name', ''))
            return f"oracle+cx_oracle://{config['username']}:{config['password']}@{dsn}"
        elif db_type == "sqlserver":
            return f"mssql+pyodbc://{config['username']}:{config['password']}@{config['host']}:{config['port']}?driver=ODBC+Driver+17+for+SQL+Server"
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    async def connect(self, db_type: str, config: Dict[str, Any], connection_id: str = "default") -> Dict[str, Any]:
        """Connect to database and return available databases"""
        if db_type not in self.SUPPORTED_ENGINES:
            raise ValueError(f"Unsupported database type: {db_type}")
        
        try:
            # Create SQLAlchemy engine for metadata operations
            conn_string = self.get_connection_string(db_type, config)
            engine = create_engine(conn_string, echo=False)
            
            # Test connection
            with engine.connect() as connection:
                pass
            
            # Store engine and create raw connection based on type
            self.engines[connection_id] = {
                "type": db_type,
                "engine": engine,
                "config": config
            }
            
            # Get available databases
            databases = self.get_databases(connection_id)
            
            return {
                "status": "success",
                "message": "Connected successfully",
                "databases": databases
            }
        except Exception as e:
            logger.error(f"Connection error: {str(e)}")
            return {
                "status": "error",
                "message": f"Failed to connect: {str(e)}"
            }
    
    def get_databases(self, connection_id: str = "default") -> List[str]:
        """Get list of available databases"""
        if connection_id not in self.engines:
            raise ValueError(f"No connection with ID: {connection_id}")
        
        engine_info = self.engines[connection_id]
        db_type = engine_info["type"]
        engine = engine_info["engine"]
        
        databases = []
        
        try:
            with engine.connect() as connection:
                if db_type == "mysql":
                    result = connection.execute("SHOW DATABASES")
                    databases = [row[0] for row in result]
                elif db_type == "postgresql":
                    result = connection.execute("SELECT datname FROM pg_database WHERE datistemplate = false")
                    databases = [row[0] for row in result]
                elif db_type == "oracle":
                    # In Oracle, we list schemas instead of databases
                    result = connection.execute("SELECT username FROM all_users ORDER BY username")
                    databases = [row[0] for row in result]
                elif db_type == "sqlserver":
                    result = connection.execute("SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')")
                    databases = [row[0] for row in result]
        except Exception as e:
            logger.error(f"Error getting databases: {str(e)}")
            raise
            
        return databases
    
    def get_tables(self, connection_id: str, database: str) -> List[str]:
        """Get list of tables in a database"""
        if connection_id not in self.engines:
            raise ValueError(f"No connection with ID: {connection_id}")
        
        engine_info = self.engines[connection_id]
        db_type = engine_info["type"]
        engine = engine_info["engine"]
        config = engine_info["config"]
        
        tables = []
        
        try:
            if db_type == "mysql":
                # Create a new engine with the database specified
                db_engine = create_engine(f"{self.get_connection_string(db_type, config)}/{database}")
                metadata = MetaData()
                metadata.reflect(bind=db_engine)
                tables = list(metadata.tables.keys())
            elif db_type == "postgresql":
                db_engine = create_engine(f"{self.get_connection_string(db_type, config)}/{database}")
                with db_engine.connect() as conn:
                    result = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
                    tables = [row[0] for row in result]
            elif db_type == "oracle":
                # In Oracle, database parameter is actually the schema name
                with engine.connect() as conn:
                    result = conn.execute(f"SELECT table_name FROM all_tables WHERE owner = '{database.upper()}'")
                    tables = [row[0] for row in result]
            elif db_type == "sqlserver":
                db_engine = create_engine(f"{self.get_connection_string(db_type, config)}/{database}")
                with db_engine.connect() as conn:
                    result = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'")
                    tables = [row[0] for row in result]
        except Exception as e:
            logger.error(f"Error getting tables: {str(e)}")
            raise
            
        return tables
    
    def disconnect(self, connection_id: str = "default"):
        """Close and remove connection"""
        if connection_id in self.engines:
            if connection_id in self.raw_connections:
                try:
                    self.raw_connections[connection_id].close()
                except:
                    pass
                del self.raw_connections[connection_id]
                
            if connection_id in self.connections:
                try:
                    self.connections[connection_id].close()
                except:
                    pass
                del self.connections[connection_id]
                
            if connection_id in self.engines:
                try:
                    self.engines[connection_id]["engine"].dispose()
                except:
                    pass
                del self.engines[connection_id]
    
    def __del__(self):
        """Clean up connections on deletion"""
        for connection_id in list(self.engines.keys()):
            self.disconnect(connection_id)