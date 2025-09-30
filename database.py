import mysql.connector
from mysql.connector import Error
import pandas as pd
import streamlit as st
from typing import Dict, List, Optional, Any
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class DatabaseManager:
    def __init__(self):
        """Initialize with environment variables only"""
        
        # Validate required environment variables
        required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        self.connection_config = {
            'host': os.getenv('DB_HOST'),
            'port': int(os.getenv('DB_PORT', '3306')),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'charset': 'utf8mb4',
            'autocommit': False,  # ← ปิด autocommit เพื่อควบคุม transaction
            'connection_timeout': 10,
            'use_pure': True,  # ← ใช้ pure Python implementation
            'ssl_disabled': False  # ← เปิด SSL ถ้า database รองรับ
        }
        self.connection = None
        self._connection_pool = None
    
    def get_connection(self):
        """Get database connection with error handling"""
        try:
            if self.connection is None or not self.connection.is_connected():
                self.connection = mysql.connector.connect(**self.connection_config)
            return self.connection
        except Error as e:
            logging.error(f"Database connection error: {e}")
            st.error("Database connection failed. Please check configuration.")
            return None
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            conn = self.get_connection()
            if conn and conn.is_connected():
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchall()
                cursor.close()
                return True
        except Error as e:
            logging.error(f"Connection test failed: {e}")
        return False
    
    def get_tables_with_info(self) -> List[Dict[str, Any]]:
        """Get list of all tables with their information"""
        try:
            conn = self.get_connection()
            if not conn:
                return []
            
            cursor = conn.cursor(dictionary=True)
            
            # Use parameterized query
            query = """
                SELECT 
                    TABLE_NAME,
                    TABLE_ROWS,
                    DATA_LENGTH,
                    INDEX_LENGTH,
                    CREATE_TIME,
                    UPDATE_TIME
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = %s 
                AND TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """
            
            cursor.execute(query, (self.connection_config['database'],))
            tables_info = cursor.fetchall()
            cursor.close()
            
            return tables_info if tables_info else []
            
        except Error as e:
            logging.error(f"Error getting tables: {e}")
            return []
    
    def get_table_preview(self, table_name: str, limit: int = 5) -> pd.DataFrame:
        """Get preview of table data - SECURE VERSION"""
        try:
            conn = self.get_connection()
            if not conn:
                return pd.DataFrame()
            
            # CRITICAL: Validate table exists in our database
            if not self._validate_table_exists(table_name):
                logging.warning(f"Attempt to access invalid table: {table_name}")
                return pd.DataFrame()
            
            # Whitelist approach - only allow tables from our database
            valid_tables = [t['TABLE_NAME'] for t in self.get_tables_with_info()]
            if table_name not in valid_tables:
                logging.warning(f"Table not in whitelist: {table_name}")
                return pd.DataFrame()
            
            # Use prepared statement
            cursor = conn.cursor()
            
            # Set timeout
            cursor.execute("SET SESSION max_execution_time = 5000")
            
            # Get primary key safely
            cursor.execute("""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                WHERE TABLE_SCHEMA = %s 
                AND TABLE_NAME = %s 
                AND CONSTRAINT_NAME = 'PRIMARY'
                LIMIT 1
            """, (self.connection_config['database'], table_name))
            
            pk_result = cursor.fetchone()
            
            if pk_result:
                # Build query with parameterized column name
                pk_column = pk_result[0]
                # Validate column name
                if not self._is_valid_identifier(pk_column):
                    cursor.close()
                    return pd.DataFrame()
                    
                query = f"""
                    SELECT * FROM `{self.connection_config['database']}`.`{table_name}` 
                    ORDER BY `{pk_column}` DESC 
                    LIMIT %s
                """
            else:
                query = f"""
                    SELECT * FROM `{self.connection_config['database']}`.`{table_name}` 
                    LIMIT %s
                """
            
            cursor.close()
            
            # Execute with parameterized limit
            df = pd.read_sql(query, conn, params=(limit,))
            
            return df
            
        except Error as e:
            logging.error(f"Error in get_table_preview: {e}")
            return pd.DataFrame()
    
    def get_table_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Get table column information - SECURE VERSION"""
        try:
            conn = self.get_connection()
            if not conn:
                return []
            
            # Validate table exists
            if not self._validate_table_exists(table_name):
                return []
            
            cursor = conn.cursor(dictionary=True)
            
            # Use parameterized query
            query = """
                SELECT 
                    COLUMN_NAME, 
                    DATA_TYPE, 
                    IS_NULLABLE, 
                    COLUMN_DEFAULT,
                    CHARACTER_MAXIMUM_LENGTH, 
                    NUMERIC_PRECISION, 
                    NUMERIC_SCALE
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = %s 
                AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """
            
            cursor.execute(query, (self.connection_config['database'], table_name))
            columns = cursor.fetchall()
            cursor.close()
            
            return columns
            
        except Error as e:
            logging.error(f"Error fetching columns: {e}")
            return []
    
    def import_data(self, table_name: str, df: pd.DataFrame, column_mapping: Dict[str, str]) -> Dict[str, Any]:
        """Import data - SECURE VERSION with transaction"""
        conn = None
        cursor = None
        
        try:
            conn = self.get_connection()
            if not conn:
                return {'success': False, 'error': 'No database connection'}
            
            # Validate table exists
            if not self._validate_table_exists(table_name):
                return {'success': False, 'error': 'Invalid table name'}
            
            # Get valid columns from database
            table_columns = self.get_table_columns(table_name)
            valid_column_names = [col['COLUMN_NAME'] for col in table_columns]
            
            # Validate mapped columns
            for db_col in column_mapping.values():
                if db_col not in valid_column_names:
                    return {'success': False, 'error': f'Invalid column: {db_col}'}
            
            # Prepare data
            mapped_df = pd.DataFrame()
            for file_col, db_col in column_mapping.items():
                if file_col in df.columns:
                    mapped_df[db_col] = df[file_col]
            
            if mapped_df.empty:
                return {'success': False, 'error': 'No data to import'}
            
            # Clean data
            mapped_df = mapped_df.fillna('')
            
            # Start transaction
            conn.start_transaction()
            
            cursor = conn.cursor()
            
            # Build parameterized INSERT query
            columns_str = ', '.join([f'`{col}`' for col in mapped_df.columns])
            placeholders = ', '.join(['%s'] * len(mapped_df.columns))
            
            query = f"""
                INSERT INTO `{self.connection_config['database']}`.`{table_name}` 
                ({columns_str}) 
                VALUES ({placeholders})
            """
            
            # Convert to tuples
            data_tuples = [tuple(row) for row in mapped_df.values]
            
            # Execute in batches for large datasets
            batch_size = 1000
            total_inserted = 0
            
            for i in range(0, len(data_tuples), batch_size):
                batch = data_tuples[i:i+batch_size]
                cursor.executemany(query, batch)
                total_inserted += cursor.rowcount
            
            # Commit transaction
            conn.commit()
            cursor.close()
            
            return {
                'success': True,
                'rows_affected': total_inserted,
                'message': f'Successfully imported {total_inserted} rows'
            }
            
        except Error as e:
            # Rollback on error
            if conn:
                conn.rollback()
            logging.error(f"Import error: {e}")
            return {'success': False, 'error': f'Database error: {str(e)}'}
            
        except Exception as e:
            if conn:
                conn.rollback()
            logging.error(f"Unexpected error: {e}")
            return {'success': False, 'error': f'Unexpected error: {str(e)}'}
        
        finally:
            if cursor:
                cursor.close()
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """Execute SELECT query - SECURE VERSION"""
        try:
            # Validate query is SELECT only
            query_upper = query.strip().upper()
            if not query_upper.startswith('SELECT'):
                logging.warning(f"Attempted non-SELECT query: {query[:50]}")
                return pd.DataFrame()
            
            conn = self.get_connection()
            if not conn:
                return pd.DataFrame()
            
            if params:
                df = pd.read_sql(query, conn, params=params)
            else:
                df = pd.read_sql(query, conn)
            
            return df
            
        except Error as e:
            logging.error(f"Query error: {e}")
            return pd.DataFrame()
    
    def execute_stored_procedure(self, procedure_name: str, parameters: Optional[List] = None) -> Dict[str, Any]:
        """Execute stored procedure - SECURE VERSION"""
        conn = None
        cursor = None
        
        try:
            conn = self.get_connection()
            if not conn:
                return {'success': False, 'error': 'No database connection'}
            
            # Validate procedure exists
            if not self._validate_procedure_exists(procedure_name):
                return {'success': False, 'error': 'Invalid procedure name'}
            
            cursor = conn.cursor()
            
            # Start transaction
            conn.start_transaction()
            
            # Build CALL statement with placeholders
            if parameters:
                placeholders = ', '.join(['%s'] * len(parameters))
                call_statement = f"CALL `{self.connection_config['database']}`.`{procedure_name}`({placeholders})"
                cursor.execute(call_statement, parameters)
            else:
                call_statement = f"CALL `{self.connection_config['database']}`.`{procedure_name}`()"
                cursor.execute(call_statement)
            
            # Fetch results
            results = []
            try:
                for result in cursor.stored_results():
                    rows = result.fetchall()
                    if rows:
                        columns = [desc[0] for desc in result.description]
                        results.append([dict(zip(columns, row)) for row in rows])
            except:
                pass
            
            rows_affected = cursor.rowcount
            
            # Get warnings
            cursor.execute("SHOW WARNINGS")
            warnings = cursor.fetchall()
            
            # Commit transaction
            conn.commit()
            cursor.close()
            
            return {
                'success': True,
                'message': f'Procedure {procedure_name} executed successfully',
                'results': results,
                'rows_affected': rows_affected if rows_affected > 0 else None,
                'warnings': warnings if warnings else []
            }
            
        except Error as e:
            if conn:
                conn.rollback()
                
            error_details = {
                'errno': e.errno if hasattr(e, 'errno') else None,
                'sqlstate': e.sqlstate if hasattr(e, 'sqlstate') else None,
                'msg': str(e)
            }
            
            logging.error(f"Procedure execution error: {e}")
            
            return {
                'success': False,
                'error': str(e),
                'error_details': error_details
            }
            
        except Exception as e:
            if conn:
                conn.rollback()
            logging.error(f"Unexpected error: {e}")
            return {'success': False, 'error': str(e)}
        
        finally:
            if cursor:
                cursor.close()
    
    def _validate_table_exists(self, table_name: str) -> bool:
        """Validate table exists in database - whitelist approach"""
        try:
            if not self._is_valid_identifier(table_name):
                return False
            
            conn = self.get_connection()
            if not conn:
                return False
            
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = %s 
                AND TABLE_NAME = %s
            """, (self.connection_config['database'], table_name))
            
            result = cursor.fetchone()
            cursor.close()
            
            return result[0] > 0
            
        except Error:
            return False
    
    def _validate_procedure_exists(self, procedure_name: str) -> bool:
        """Validate procedure exists in database"""
        try:
            if not self._is_valid_identifier(procedure_name):
                return False
            
            conn = self.get_connection()
            if not conn:
                return False
            
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.ROUTINES 
                WHERE ROUTINE_SCHEMA = %s 
                AND ROUTINE_NAME = %s
            """, (self.connection_config['database'], procedure_name))
            
            result = cursor.fetchone()
            cursor.close()
            
            return result[0] > 0
            
        except Error:
            return False
    
    def _is_valid_identifier(self, identifier: str) -> bool:
        """Validate SQL identifier (table/column/procedure name)"""
        if not identifier or not isinstance(identifier, str):
            return False
        
        # Only allow alphanumeric and underscore
        # Must start with letter or underscore
        # Max length 64 characters (MySQL limit)
        import re
        pattern = r'^[a-zA-Z_][a-zA-Z0-9_]{0,63}$'
        
        return bool(re.match(pattern, identifier))
    
    def close_connection(self):
        """Close database connection"""
        try:
            if self.connection and self.connection.is_connected():
                self.connection.close()
                logging.info("Database connection closed")
        except Error as e:
            logging.error(f"Error closing connection: {e}")
    
    def __del__(self):
        """Destructor"""
        self.close_connection()
