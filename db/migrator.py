import pandas as pd
from sqlalchemy import create_engine, text
import time
import asyncio
import logging
from typing import Dict, List, Any, Callable, Optional, Tuple
import cx_Oracle
import psycopg2
import pyodbc
import mysql.connector
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

class DatabaseMigrator:
    def __init__(self, connector, inspector):
        self.connector = connector
        self.inspector = inspector
        self.progress_callbacks = {}
        self.cancel_flags = {}
        self.executor = ThreadPoolExecutor(max_workers=5)
    
    async def migrate_tables(self, 
                       source_connection_id: str, 
                       target_connection_id: str,
                       source_database: str,
                       target_database: str,
                       tables: List[str],
                       progress_callback: Optional[Callable] = None,
                       task_id: str = "default") -> Dict[str, Any]:
        """
        Migrate selected tables from source to target database
        """
        if source_connection_id not in self.connector.engines:
            raise ValueError(f"No source connection with ID: {source_connection_id}")
        
        if target_connection_id not in self.connector.engines:
            raise ValueError(f"No target connection with ID: {target_connection_id}")
        
        source_engine_info = self.connector.engines[source_connection_id]
        target_engine_info = self.connector.engines[target_connection_id]
        
        source_db_type = source_engine_info["type"]
        target_db_type = target_engine_info["type"]
        
        source_config = source_engine_info["config"]
        target_config = target_engine_info["config"]
        
        # Store callback and initialize cancel flag
        if progress_callback:
            self.progress_callbacks[task_id] = progress_callback
        self.cancel_flags[task_id] = False
        
        # Start the migration in a separate thread
        loop = asyncio.get_event_loop()
        future = loop.run_in_executor(
            self.executor,
            self._migrate_tables_sync,
            source_connection_id,
            target_connection_id,
            source_database,
            target_database,
            tables,
            source_db_type,
            target_db_type,
            source_config,
            target_config,
            task_id
        )
        
        return {
            "status": "started",
            "message": f"Migration started for {len(tables)} tables",
            "task_id": task_id
        }
    
    def _migrate_tables_sync(self,
                           source_connection_id: str,
                           target_connection_id: str,
                           source_database: str,
                           target_database: str,
                           tables: List[str],
                           source_db_type: str,
                           target_db_type: str,
                           source_config: Dict[str, Any],
                           target_config: Dict[str, Any],
                           task_id: str):
        """
        Synchronous method to handle table migration
        """
        total_tables = len(tables)
        tables_migrated = 0
        failed_tables = []
        start_time = time.time()
        
        try:
            # Create source engine with database
            if source_db_type in ["mysql", "postgresql", "sqlserver"]:
                source_conn_string = self.connector.get_connection_string(source_db_type, source_config)
                source_engine = create_engine(f"{source_conn_string}/{source_database}")
            else:  # Oracle
                source_engine = self.connector.engines[source_connection_id]["engine"]
                source_schema = source_database.upper()
            
            # Create target engine with database
            if target_db_type in ["mysql", "postgresql", "sqlserver"]:
                target_conn_string = self.connector.get_connection_string(target_db_type, target_config)
                target_engine = create_engine(f"{target_conn_string}/{target_database}")
            else:  # Oracle
                target_engine = self.connector.engines[target_connection_id]["engine"]
                target_schema = target_database.upper()
            
            # Process each table
            for table in tables:
                if self.cancel_flags.get(task_id, False):
                    self._update_progress(task_id, {
                        "status": "cancelled",
                        "message": f"Migration cancelled after {tables_migrated}/{total_tables} tables",
                        "tables_completed": tables_migrated,
                        "tables_failed": failed_tables,
                        "total_tables": total_tables,
                        "elapsed_time": time.time() - start_time
                    })
                    return
                
                try:
                    self._update_progress(task_id, {
                        "status": "in_progress",
                        "message": f"Starting migration of table: {table}",
                        "current_table": table,
                        "tables_completed": tables_migrated,
                        "total_tables": total_tables
                    })
                    
                    # Get table schema
                    if source_db_type == "oracle":
                        table_schema = self.inspector.inspect_table(source_connection_id, source_schema, table)
                    else:
                        table_schema = self.inspector.inspect_table(source_connection_id, source_database, table)
                    
                    # Generate CREATE TABLE SQL
                    if target_db_type == "oracle":
                        create_table_sql = self.inspector.generate_create_table_sql(
                            table_schema, source_db_type, target_db_type, f"{target_schema}.{table}"
                        )
                    else:
                        create_table_sql = self.inspector.generate_create_table_sql(
                            table_schema, source_db_type, target_db_type, table
                        )
                    
                    # Create the table in target database
                    with target_engine.connect() as connection:
                        try:
                            connection.execute(text(create_table_sql))
                            connection.commit()
                        except Exception as e:
                            # Table might already exist
                            logger.warning(f"Error creating table {table}: {str(e)}")
                    
                    # Get row count (estimate)
                    row_count = self._estimate_row_count(source_engine, table, source_db_type, source_schema if source_db_type == "oracle" else None)
                    
                    # Use appropriate migration method based on database types and table size
                    if row_count > 1000000:  # 1 million rows threshold for chunking
                        self._migrate_table_chunked(
                            source_engine, target_engine, 
                            table, row_count, 
                            source_db_type, target_db_type,
                            source_schema if source_db_type == "oracle" else None,
                            target_schema if target_db_type == "oracle" else None,
                            task_id
                        )
                    else:
                        self._migrate_table_single(
                            source_engine, target_engine, 
                            table, row_count, 
                            source_db_type, target_db_type,
                            source_schema if source_db_type == "oracle" else None,
                            target_schema if target_db_type == "oracle" else None,
                            task_id
                        )
                    
                    tables_migrated += 1
                    self._update_progress(task_id, {
                        "status": "in_progress",
                        "message": f"Completed migration of table: {table}",
                        "tables_completed": tables_migrated,
                        "total_tables": total_tables
                    })
                    
                except Exception as e:
                    logger.error(f"Error migrating table {table}: {str(e)}")
                    failed_tables.append({"table": table, "error": str(e)})
            
            # Migration complete
            elapsed_time = time.time() - start_time
            self._update_progress(task_id, {
                "status": "completed",
                "message": f"Migration completed: {tables_migrated}/{total_tables} tables migrated successfully",
                "tables_completed": tables_migrated,
                "tables_failed": failed_tables,
                "total_tables": total_tables,
                "elapsed_time": elapsed_time
            })
            
        except Exception as e:
            logger.error(f"Migration process error: {str(e)}")
            self._update_progress(task_id, {
                "status": "error",
                "message": f"Migration failed: {str(e)}",
                "tables_completed": tables_migrated,
                "tables_failed": failed_tables,
                "total_tables": total_tables,
                "elapsed_time": time.time() - start_time
            })
    
    def _estimate_row_count(self, engine, table: str, db_type: str, schema: Optional[str] = None) -> int:
        """Estimate row count of a table"""
        try:
            with engine.connect() as connection:
                if db_type == "oracle":
                    query = f"SELECT COUNT(*) FROM {schema}.{table}"
                elif db_type == "postgresql":
                    # Use faster estimation for PostgreSQL
                    query = f"""
                        SELECT reltuples::bigint AS estimate
                        FROM pg_class
                        WHERE relname = '{table}'
                    """
                elif db_type == "sqlserver":
                    query = f"SELECT COUNT(*) FROM {table}"
                else:  # MySQL
                    query = f"SELECT COUNT(*) FROM {table}"
                
                result = connection.execute(text(query)).fetchone()
                return result[0]
        except Exception as e:
            logger.warning(f"Error estimating row count: {str(e)}")
            return 0
    
    def _migrate_table_single(self, 
                            source_engine, 
                            target_engine, 
                            table: str, 
                            row_count: int,
                            source_db_type: str, 
                            target_db_type: str,
                            source_schema: Optional[str] = None,
                            target_schema: Optional[str] = None,
                            task_id: str = "default"):
        """Migrate a table in a single operation"""
        try:
            self._update_progress(task_id, {
                "status": "in_progress",
                "message": f"Reading data from {table} ({row_count} rows)",
                "current_table": table,
                "current_progress": 0,
                "total_rows": row_count
            })
            
            # Construct the query
            if source_db_type == "oracle":
                query = f"SELECT * FROM {source_schema}.{table}"
            else:
                query = f"SELECT * FROM {table}"
            
            # Read data
            df = pd.read_sql(query, source_engine)
            
            # Handle NULL values
            df = df.where(pd.notnull(df), None)
            
            self._update_progress(task_id, {
                "status": "in_progress",
                "message": f"Writing {df.shape[0]} rows to {table}",
                "current_table": table,
                "current_progress": 0,
                "total_rows": df.shape[0]
            })
            
            # Use optimal insertion method for each database
            if target_db_type == "postgresql":
                # Use COPY FROM for PostgreSQL (fastest)
                conn = target_engine.raw_connection()
                cursor = conn.cursor()
                
                # Create a string buffer
                from io import StringIO
                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
                buffer.seek(0)
                
                try:
                    if target_schema:
                        cursor.copy_from(buffer, f"{target_schema}.{table}")
                    else:
                        cursor.copy_from(buffer, table)
                    conn.commit()
                finally:
                    cursor.close()
                    conn.close()
            
            elif target_db_type == "mysql":
                # Use executemany for MySQL
                with target_engine.connect() as connection:
                    connection.execute(text(f"TRUNCATE TABLE {table}"))
                    
                    # Prepare data and column names
                    columns = ", ".join(df.columns)
                    placeholders = ", ".join(["%s" for _ in df.columns])
                    
                    # Use raw connection for executemany
                    with target_engine.raw_connection() as conn:
                        cursor = conn.cursor()
                        cursor.executemany(
                            f"INSERT INTO {table} ({columns}) VALUES ({placeholders})",
                            df.values.tolist()
                        )
                        conn.commit()
            
            elif target_db_type == "oracle":
                # Use cx_Oracle's executemany with arraydmlrowcount
                column_names = ", ".join(df.columns)
                placeholders = ", ".join([f":{i+1}" for i in range(len(df.columns))])
                
                with target_engine.raw_connection() as conn:
                    cursor = conn.cursor()
                    cursor.setinputsizes(*[None for _ in range(len(df.columns))])
                    cursor.executemany(
                        f"INSERT INTO {target_schema}.{table} ({column_names}) VALUES ({placeholders})",
                        df.values.tolist(),
                        arraydmlrowcounts=True
                    )
                    conn.commit()
            
            elif target_db_type == "sqlserver":
                # Use fast_executemany for SQL Server
                with target_engine.connect() as connection:
                    connection.execution_options(fast_executemany=True).execute(
                        text(f"TRUNCATE TABLE {table}")
                    )
                
                # Use to_sql with method='multi'
                df.to_sql(
                    table,
                    target_engine,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=10000
                )
            
            self._update_progress(task_id, {
                "status": "in_progress",
                "message": f"Completed writing {df.shape[0]} rows to {table}",
                "current_table": table,
                "current_progress": df.shape[0],
                "total_rows": df.shape[0]
            })
            
        except Exception as e:
            logger.error(f"Error in single table migration for {table}: {str(e)}")
            raise
    
    def _migrate_table_chunked(self, 
                             source_engine, 
                             target_engine, 
                             table: str, 
                             row_count: int,
                             source_db_type: str, 
                             target_db_type: str,
                             source_schema: Optional[str] = None,
                             target_schema: Optional[str] = None,
                             task_id: str = "default"):
        """Migrate a table in chunks for large tables"""
        chunk_size = 100000  # 100k rows per chunk
        total_chunks = (row_count // chunk_size) + (1 if row_count % chunk_size > 0 else 0)
        rows_processed = 0
        
        try:
            # Get primary key for efficient chunking if available
            pk_column = self._get_primary_key(source_engine, table, source_db_type, source_schema)
            
            if pk_column:
                # Use keyset pagination if primary key is available
                self._migrate_with_keyset_pagination(
                    source_engine, target_engine, table, 
                    pk_column, chunk_size, row_count,
                    source_db_type, target_db_type,
                    source_schema, target_schema, task_id
                )
            else:
                # Use OFFSET/LIMIT if no primary key
                self._migrate_with_offset_pagination(
                    source_engine, target_engine, table, 
                    chunk_size, row_count,
                    source_db_type, target_db_type,
                    source_schema, target_schema, task_id
                )
                
        except Exception as e:
            logger.error(f"Error in chunked table migration for {table}: {str(e)}")
            raise
    
    def _get_primary_key(self, engine, table: str, db_type: str, schema: Optional[str] = None) -> Optional[str]:
        """Get primary key column of a table if available"""
        try:
            with engine.connect() as connection:
                if db_type == "mysql":
                    query = f"""
                        SELECT COLUMN_NAME
                        FROM information_schema.KEY_COLUMN_USAGE
                        WHERE TABLE_SCHEMA = DATABASE()
                        AND TABLE_NAME = '{table}'
                        AND CONSTRAINT_NAME = 'PRIMARY'
                        LIMIT 1
                    """
                elif db_type == "postgresql":
                    query = f"""
                        SELECT a.attname
                        FROM pg_index i
                        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                        WHERE i.indrelid = '{table}'::regclass
                        AND i.indisprimary
                        LIMIT 1
                    """
                elif db_type == "oracle":
                    query = f"""
                        SELECT cols.column_name
                        FROM all_constraints cons, all_cons_columns cols
                        WHERE cons.constraint_type = 'P'
                        AND cons.constraint_name = cols.constraint_name
                        AND cons.owner = '{schema}'
                        AND cols.table_name = '{table}'
                        AND ROWNUM = 1
                    """
                elif db_type == "sqlserver":
                    query = f"""
                        SELECT c.name
                        FROM sys.indexes i
                        JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                        JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                        JOIN sys.tables t ON i.object_id = t.object_id
                        WHERE i.is_primary_key = 1
                        AND t.name = '{table}'
                        ORDER BY ic.key_ordinal
                        OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY
                    """
                
                result = connection.execute(text(query)).fetchone()
                return result[0] if result else None
        except Exception as e:
            logger.warning(f"Error getting primary key: {str(e)}")
            return None
    
    def _migrate_with_keyset_pagination(self,
                                      source_engine, 
                                      target_engine, 
                                      table: str,
                                      pk_column: str,
                                      chunk_size: int,
                                      row_count: int,
                                      source_db_type: str,
                                      target_db_type: str,
                                      source_schema: Optional[str] = None,
                                      target_schema: Optional[str] = None,
                                      task_id: str = "default"):
        """Migrate large table using keyset pagination (more efficient)"""
        rows_processed = 0
        last_id = None
        
        while rows_processed < row_count:
            if self.cancel_flags.get(task_id, False):
                return
            
            self._update_progress(task_id, {
                "status": "in_progress",
                "message": f"Processing {table}: {rows_processed}/{row_count} rows",
                "current_table": table,
                "current_progress": rows_processed,
                "total_rows": row_count
            })
            
            # Construct query with keyset pagination
            if last_id is None:
                # First chunk
                if source_db_type == "oracle":
                    query = f"""
                        SELECT * FROM {source_schema}.{table}
                        WHERE ROWNUM <= {chunk_size}
                        ORDER BY {pk_column}
                    """
                else:
                    query = f"""
                        SELECT * FROM {table}
                        ORDER BY {pk_column}
                        LIMIT {chunk_size}
                    """
            else:
                # Subsequent chunks
                if source_db_type == "oracle":
                    query = f"""
                        SELECT * FROM {source_schema}.{table}
                        WHERE {pk_column} > :last_id
                        AND ROWNUM <= {chunk_size}
                        ORDER BY {pk_column}
                    """
                    params = {"last_id": last_id}
                elif source_db_type == "sqlserver":
                    query = f"""
                        SELECT TOP {chunk_size} * FROM {table}
                        WHERE {pk_column} > ?
                        ORDER BY {pk_column}
                    """
                    params = [last_id]
                else:
                    query = f"""
                        SELECT * FROM {table}
                        WHERE {pk_column} > :last_id
                        ORDER BY {pk_column}
                        LIMIT {chunk_size}
                    """
                    params = {"last_id": last_id}
                
                # Read data chunk
                df = pd.read_sql(text(query), source_engine, params=params)
            
            # Handle case where no query params (first chunk)
            if last_id is None:
                df = pd.read_sql(text(query), source_engine)
            
            if df.empty:
                break  # No more data
                
            # Update the last ID for next iteration
            if not df.empty:
                last_id = df[pk_column].iloc[-1]
            
            # Handle NULL values
            df = df.where(pd.notnull(df), None)
            
            # Insert into target using optimal method
            self._insert_chunk(target_engine, df, table, target_db_type, target_schema)
            
            rows_processed += len(df)
            
            self._update_progress(task_id, {
                "status": "in_progress",
                "message": f"Processed {rows_processed}/{row_count} rows in {table}",
                "current_table": table,
                "current_progress": rows_processed,
                "total_rows": row_count
            })
    
    def _migrate_with_offset_pagination(self,
                                      source_engine, 
                                      target_engine, 
                                      table: str,
                                      chunk_size: int,
                                      row_count: int,
                                      source_db_type: str,
                                      target_db_type: str,
                                      source_schema: Optional[str] = None,
                                      target_schema: Optional[str] = None,
                                      task_id: str = "default"):
        """Migrate large table using offset pagination (less efficient but works without PK)"""
        rows_processed = 0
        offset = 0
        
        while rows_processed < row_count:
            if self.cancel_flags.get(task_id, False):
                return
                
            self._update_progress(task_id, {
                "status": "in_progress",
                "message": f"Processing {table}: {rows_processed}/{row_count} rows",
                "current_table": table,
                "current_progress": rows_processed,
                "total_rows": row_count
            })
            
            # Construct query with offset pagination
            if source_db_type == "oracle":
                query = f"""
                    SELECT * FROM (
                        SELECT t.*, ROWNUM AS rnum 
                        FROM {source_schema}.{table} t
                        WHERE ROWNUM <= {offset + chunk_size}
                    ) WHERE rnum > {offset}
                """
            elif source_db_type == "sqlserver":
                query = f"""
                    SELECT * FROM {table}
                    ORDER BY (SELECT NULL)
                    OFFSET {offset} ROWS
                    FETCH NEXT {chunk_size} ROWS ONLY
                """
            else:
                query = f"""
                    SELECT * FROM {table}
                    LIMIT {chunk_size} OFFSET {offset}
                """
            
            # Read data chunk
            df = pd.read_sql(text(query), source_engine)
            
            if df.empty:
                break  # No more data
            
            # Handle NULL values
            df = df.where(pd.notnull(df), None)
            
            # Insert into target using optimal method
            self._insert_chunk(target_engine, df, table, target_db_type, target_schema)
            
            rows_processed += len(df)
            offset += chunk_size
            
            self._update_progress(task_id, {
                "status": "in_progress",
                "message": f"Processed {rows_processed}/{row_count} rows in {table}",
                "current_table": table,
                "current_progress": rows_processed,
                "total_rows": row_count
            })
    
    def _insert_chunk(self, target_engine, df, table: str, target_db_type: str, target_schema: Optional[str] = None):
        """Insert a chunk of data using the most efficient method for the target database"""
        if target_db_type == "postgresql":
            # Use COPY FROM for PostgreSQL
            conn = target_engine.raw_connection()
            cursor = conn.cursor()
            
            # Create a string buffer
            from io import StringIO
            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
            buffer.seek(0)
            
            try:
                if target_schema:
                    cursor.copy_from(buffer, f"{target_schema}.{table}")
                else:
                    cursor.copy_from(buffer, table)
                conn.commit()
            finally:
                cursor.close()
                conn.close()
        
        elif target_db_type == "mysql":
            # Use executemany for MySQL
            columns = ", ".join(df.columns)
            placeholders = ", ".join(["%s" for _ in df.columns])
            
            # Use raw connection for executemany
            with target_engine.raw_connection() as conn:
                cursor = conn.cursor()
                cursor.executemany(
                    f"INSERT INTO {table} ({columns}) VALUES ({placeholders})",
                    df.values.tolist()
                )
                conn.commit()
        
        elif target_db_type == "oracle":
            # Use cx_Oracle's executemany with arraydmlrowcount
            column_names = ", ".join(df.columns)
            placeholders = ", ".join([f":{i+1}" for i in range(len(df.columns))])
            
            with target_engine.raw_connection() as conn:
                cursor = conn.cursor()
                cursor.setinputsizes(*[None for _ in range(len(df.columns))])
                cursor.executemany(
                    f"INSERT INTO {target_schema}.{table} ({column_names}) VALUES ({placeholders})",
                    df.values.tolist(),
                    arraydmlrowcounts=True
                )
                conn.commit()
        
        elif target_db_type == "sqlserver":
            # Use fast_executemany for SQL Server
            with target_engine.connect().execution_options(fast_executemany=True) as connection:
                df.to_sql(
                    table,
                    connection,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=10000
                )
    
    def _update_progress(self, task_id: str, progress_data: Dict[str, Any]):
        """Update progress via callback if registered"""
        if task_id in self.progress_callbacks and self.progress_callbacks[task_id]:
            try:
                self.progress_callbacks[task_id](progress_data)
            except Exception as e:
                logger.error(f"Error in progress callback: {str(e)}")
    
    def cancel_migration(self, task_id: str) -> Dict[str, Any]:
        """Cancel an ongoing migration task"""
        if task_id in self.cancel_flags:
            self.cancel_flags[task_id] = True
            return {"status": "cancelling", "message": "Migration cancellation requested"}
        else:
            return {"status": "error", "message": "Task ID not found"}
    
    def __del__(self):
        """Clean up resources"""
        self.executor.shutdown(wait=False)