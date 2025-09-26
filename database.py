import mysql.connector
from mysql.connector import Error
import pandas as pd
import streamlit as st
from typing import Dict, List, Optional, Any
import logging

class DatabaseManager:
    def __init__(self):
        self.connection_config = {
            'host': '182.53.230.50',
            'port': 33306,
            'database': 'ntdatabase',
            'user': 'mysqldb',
            'password': 'h5DHtZ5TtssikBd',
            'charset': 'utf8mb4',
            'autocommit': True,
            'connection_timeout': 10
        }
        self.connection = None
    
    def get_connection(self):
        """Get database connection with error handling"""
        try:
            if self.connection is None or not self.connection.is_connected():
                self.connection = mysql.connector.connect(**self.connection_config)
            return self.connection
        except Error as e:
            st.error(f"Database connection error: {e}")
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
        """Get list of all tables with their information including last update"""
        try:
            conn = self.get_connection()
            if not conn:
                return []
            
            cursor = conn.cursor(dictionary=True)
            
            # Get basic table info from INFORMATION_SCHEMA
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
            
            try:
                cursor.execute(query, (self.connection_config['database'],))
                tables_info = cursor.fetchall()
                
                # Enhance with actual timestamp data from each table
                enhanced_tables = []
                for table_info in tables_info:
                    table_name = table_info['TABLE_NAME']
                    
                    # Try to get actual last timestamp from the table
                    actual_timestamp = self._get_actual_last_timestamp(table_name)
                    
                    # Use actual timestamp if available, otherwise keep original
                    if actual_timestamp:
                        table_info['ACTUAL_LAST_UPDATE'] = actual_timestamp
                        table_info['UPDATE_TIME'] = actual_timestamp
                    
                    enhanced_tables.append(table_info)
                
                cursor.close()
                return enhanced_tables
                
            except Error as e:
                cursor.close()
                return self._get_basic_table_info()
            
        except Error as e:
            return self._get_basic_table_info()
    
    def _get_actual_last_timestamp(self, table_name: str) -> Optional[str]:
        """Get the actual last timestamp from table data"""
        try:
            conn = self.get_connection()
            if not conn:
                return None
            
            if not self._is_valid_table_name(table_name):
                return None
            
            cursor = conn.cursor()
            
            # Common timestamp column names to check
            timestamp_columns = ['timestamp', 'created_at', 'updated_at', 'date', 'datetime', 'time']
            
            # Get all columns for this table
            cursor.execute(f"DESCRIBE `{table_name}`")
            columns = cursor.fetchall()
            
            # Find timestamp-like columns
            found_timestamp_col = None
            for col in columns:
                col_name = col[0].lower()
                col_type = col[1].lower()
                
                # Check if it's a timestamp/datetime column
                if ('timestamp' in col_type or 'datetime' in col_type or 
                    any(ts_name in col_name for ts_name in timestamp_columns)):
                    found_timestamp_col = col[0]
                    break
            
            if found_timestamp_col:
                # Get the latest timestamp from this column
                cursor.execute(f"SELECT MAX(`{found_timestamp_col}`) FROM `{table_name}`")
                result = cursor.fetchone()
                if result and result[0]:
                    cursor.close()
                    return str(result[0])
            
            cursor.close()
            return None
            
        except Exception as e:
            return None
    
    def _get_basic_table_info(self) -> List[Dict[str, Any]]:
        """Fallback method to get basic table info"""
        try:
            conn = self.get_connection()
            if not conn:
                return []
            
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES")
            table_names = [table[0] for table in cursor.fetchall()]
            cursor.close()
            
            tables_info = []
            for table_name in table_names:
                try:
                    # Get row count
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                    row_count = cursor.fetchone()[0]
                    cursor.close()
                    
                    tables_info.append({
                        'TABLE_NAME': table_name,
                        'TABLE_ROWS': row_count,
                        'DATA_LENGTH': None,
                        'INDEX_LENGTH': None,
                        'CREATE_TIME': None,
                        'UPDATE_TIME': None
                    })
                except:
                    # Skip problematic tables
                    continue
            
            return tables_info
            
        except Error as e:
            return []
    
    def get_table_preview(self, table_name: str, limit: int = 5) -> pd.DataFrame:
        """Get preview of table data (last N rows)"""
        try:
            conn = self.get_connection()
            if not conn:
                return pd.DataFrame()
            
            # Sanitize table name to prevent SQL injection
            if not self._is_valid_table_name(table_name):
                raise ValueError("Invalid table name")
            
            query = f"SELECT * FROM `{table_name}` ORDER BY 1 DESC LIMIT %s"
            df = pd.read_sql(query, conn, params=[limit])
            return df
        except Error as e:
            st.error(f"Error fetching table preview: {e}")
            return pd.DataFrame()
        except Exception as e:
            st.error(f"Unexpected error: {e}")
            return pd.DataFrame()
    
    def get_table_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Get table column information"""
        try:
            conn = self.get_connection()
            if not conn:
                return []
            
            if not self._is_valid_table_name(table_name):
                raise ValueError("Invalid table name")
            
            cursor = conn.cursor(dictionary=True)
            query = """
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, 
                   CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
            """
            cursor.execute(query, (self.connection_config['database'], table_name))
            columns = cursor.fetchall()
            cursor.close()
            return columns
        except Error as e:
            st.error(f"Error fetching table columns: {e}")
            return []
    
    def import_data(self, table_name: str, df: pd.DataFrame, column_mapping: Dict[str, str]) -> Dict[str, Any]:
        """Import data from DataFrame to database table"""
        try:
            conn = self.get_connection()
            if not conn:
                return {'success': False, 'error': 'No database connection'}
            
            if not self._is_valid_table_name(table_name):
                return {'success': False, 'error': 'Invalid table name'}
            
            # Prepare data based on mapping
            mapped_df = pd.DataFrame()
            for file_col, db_col in column_mapping.items():
                if file_col in df.columns:
                    mapped_df[db_col] = df[file_col]
            
            if mapped_df.empty:
                return {'success': False, 'error': 'No data to import after mapping'}
            
            # Clean data - handle NaN values
            mapped_df = mapped_df.fillna('')
            
            # Get table columns to ensure we're only inserting valid columns
            table_columns = self.get_table_columns(table_name)
            valid_columns = [col['COLUMN_NAME'] for col in table_columns]
            
            # Filter mapped_df to only include valid columns
            final_df = mapped_df[[col for col in mapped_df.columns if col in valid_columns]]
            
            if final_df.empty:
                return {'success': False, 'error': 'No valid columns found for import'}
            
            # Create INSERT query
            columns_str = '`, `'.join(final_df.columns)
            placeholders = ', '.join(['%s'] * len(final_df.columns))
            query = f"INSERT INTO `{table_name}` (`{columns_str}`) VALUES ({placeholders})"
            
            cursor = conn.cursor()
            
            # Convert DataFrame to list of tuples for executemany
            data_tuples = [tuple(row) for row in final_df.values]
            
            # Execute the insert
            cursor.executemany(query, data_tuples)
            rows_affected = cursor.rowcount
            
            cursor.close()
            conn.commit()
            
            return {
                'success': True,
                'rows_affected': rows_affected,
                'message': f'Successfully imported {rows_affected} rows'
            }
            
        except Error as e:
            return {'success': False, 'error': f'Database error: {str(e)}'}
        except Exception as e:
            return {'success': False, 'error': f'Unexpected error: {str(e)}'}
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """Execute a SELECT query and return results as DataFrame"""
        try:
            conn = self.get_connection()
            if not conn:
                return pd.DataFrame()
            
            if params:
                df = pd.read_sql(query, conn, params=params)
            else:
                df = pd.read_sql(query, conn)
            
            return df
        except Error as e:
            st.error(f"Query execution error: {e}")
            return pd.DataFrame()
    
    def get_tables(self) -> List[str]:
        """Get list of all tables in the database (for backward compatibility)"""
        try:
            tables_info = self.get_tables_with_info()
            return [table['TABLE_NAME'] for table in tables_info]
        except Exception as e:
            logging.error(f"Error in get_tables: {e}")
            return []
        """Get detailed table information"""
        try:
            conn = self.get_connection()
            if not conn:
                return {}
            
            if not self._is_valid_table_name(table_name):
                return {}
            
            cursor = conn.cursor(dictionary=True)
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) as row_count FROM `{table_name}`")
            row_count = cursor.fetchone()['row_count']
            
            # Get table creation info
            cursor.execute(f"SHOW CREATE TABLE `{table_name}`")
            create_info = cursor.fetchone()
            
            cursor.close()
            
            return {
                'row_count': row_count,
                'create_statement': create_info['Create Table'] if create_info else '',
                'columns': self.get_table_columns(table_name)
            }
        except Error as e:
            st.error(f"Error getting table info: {e}")
            return {}
    
    def _is_valid_table_name(self, table_name: str) -> bool:
        """Validate table name to prevent SQL injection"""
        if not table_name or not isinstance(table_name, str):
            return False
        
        # Check if table name contains only allowed characters
        import re
        pattern = r'^[a-zA-Z_][a-zA-Z0-9_]*$'
        return bool(re.match(pattern, table_name))
    
    def close_connection(self):
        """Close database connection"""
        try:
            if self.connection and self.connection.is_connected():
                self.connection.close()
        except Error as e:
            logging.error(f"Error closing connection: {e}")
    
    def __del__(self):
        """Destructor to ensure connection is closed"""
        self.close_connection()
