import pandas as pd
import streamlit as st
from typing import Optional
import io
import logging

class FileProcessor:
    """Handle file upload and processing for different formats"""
    
    def __init__(self):
        self.supported_formats = ['.csv', '.xlsx', '.xls']
        self.max_file_size = 50 * 1024 * 1024  # 50MB
    
    def process_file(self, uploaded_file) -> Optional[pd.DataFrame]:
        """
        Process uploaded file and return DataFrame
        
        Args:
            uploaded_file: Streamlit UploadedFile object
            
        Returns:
            pandas.DataFrame or None if processing fails
        """
        try:
            # Check file size
            if uploaded_file.size > self.max_file_size:
                st.error(f"File size ({uploaded_file.size / (1024*1024):.1f}MB) exceeds maximum allowed size (50MB)")
                return None
            
            # Get file extension
            file_extension = self._get_file_extension(uploaded_file.name)
            
            if file_extension not in self.supported_formats:
                st.error(f"Unsupported file format: {file_extension}")
                return None
            
            # Process based on file type
            if file_extension == '.csv':
                return self._process_csv(uploaded_file)
            elif file_extension in ['.xlsx', '.xls']:
                return self._process_excel(uploaded_file)
            else:
                st.error(f"Unsupported file format: {file_extension}")
                return None
                
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")
            logging.error(f"File processing error: {e}")
            return None
    
    def _process_csv(self, uploaded_file) -> Optional[pd.DataFrame]:
        """Process CSV file"""
        try:
            # Try different encodings
            encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252']
            
            for encoding in encodings:
                try:
                    # Reset file pointer
                    uploaded_file.seek(0)
                    
                    # Try to read with current encoding
                    df = pd.read_csv(
                        uploaded_file,
                        encoding=encoding,
                        na_values=['', 'NULL', 'null', 'N/A', 'n/a', 'NA', 'na'],
                        keep_default_na=True
                    )
                    
                    # Validate data
                    if df.empty:
                        st.warning("CSV file is empty")
                        return None
                    
                    # Clean column names
                    df.columns = df.columns.str.strip()
                    
                    # Show encoding success message
                    st.success(f"✅ CSV file loaded successfully with {encoding} encoding")
                    
                    return self._validate_and_clean_dataframe(df)
                    
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    if encoding == encodings[-1]:  # Last encoding attempt
                        raise e
                    continue
            
            st.error("Could not read CSV file with any supported encoding")
            return None
            
        except Exception as e:
            st.error(f"Error processing CSV file: {str(e)}")
            return None
    
    def _process_excel(self, uploaded_file) -> Optional[pd.DataFrame]:
        """Process Excel file (.xlsx, .xls)"""
        try:
            # Reset file pointer
            uploaded_file.seek(0)
            
            # Read Excel file
            excel_file = pd.ExcelFile(uploaded_file)
            
            # Get sheet names
            sheet_names = excel_file.sheet_names
            
            if len(sheet_names) == 1:
                # Single sheet - read directly
                df = pd.read_excel(
                    uploaded_file,
                    sheet_name=0,
                    na_values=['', 'NULL', 'null', 'N/A', 'n/a', 'NA', 'na']
                )
                st.success(f"✅ Excel file loaded successfully (Sheet: {sheet_names[0]})")
            else:
                # Multiple sheets - let user choose
                selected_sheet = st.selectbox(
                    "📋 Select Excel Sheet:",
                    options=sheet_names,
                    help=f"This Excel file contains {len(sheet_names)} sheets"
                )
                
                df = pd.read_excel(
                    uploaded_file,
                    sheet_name=selected_sheet,
                    na_values=['', 'NULL', 'null', 'N/A', 'n/a', 'NA', 'na']
                )
                st.success(f"✅ Excel sheet '{selected_sheet}' loaded successfully")
            
            # Validate data
            if df.empty:
                st.warning("Excel sheet is empty")
                return None
            
            # Clean column names
            df.columns = df.columns.str.strip()
            
            return self._validate_and_clean_dataframe(df)
            
        except Exception as e:
            st.error(f"Error processing Excel file: {str(e)}")
            return None
    
    def _validate_and_clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and clean DataFrame"""
        try:
            # Remove completely empty rows
            df = df.dropna(how='all')
            
            # Remove completely empty columns
            df = df.dropna(axis=1, how='all')
            
            # Clean column names - remove special characters and spaces
            df.columns = df.columns.str.replace(r'[^\w\s]', '', regex=True)
            df.columns = df.columns.str.replace(r'\s+', '_', regex=True)
            df.columns = df.columns.str.strip('_')
            
            # Ensure no duplicate column names
            df.columns = self._ensure_unique_columns(df.columns.tolist())
            
            # Convert object columns that look like numbers
            df = self._smart_type_conversion(df)
            
            # Show data info
            st.info(f"📊 Data cleaned: {len(df)} rows, {len(df.columns)} columns")
            
            return df
            
        except Exception as e:
            st.error(f"Error validating DataFrame: {str(e)}")
            return df  # Return original if cleaning fails
    
    def _smart_type_conversion(self, df: pd.DataFrame) -> pd.DataFrame:
        """Intelligently convert column types"""
        try:
            for col in df.columns:
                # Skip if already numeric
                if pd.api.types.is_numeric_dtype(df[col]):
                    continue
                
                # Try to convert to numeric if it looks like numbers
                if df[col].dtype == 'object':
                    # Remove common non-numeric characters and try conversion
                    temp_series = df[col].astype(str).str.replace(',', '').str.replace('$', '').str.strip()
                    
                    # Try to convert to numeric
                    numeric_series = pd.to_numeric(temp_series, errors='coerce')
                    
                    # If most values can be converted to numeric, use it
                    non_null_count = df[col].notna().sum()
                    numeric_count = numeric_series.notna().sum()
                    
                    if non_null_count > 0 and (numeric_count / non_null_count) > 0.8:
                        df[col] = numeric_series
                        
            return df
            
        except Exception as e:
            logging.warning(f"Type conversion warning: {e}")
            return df
    
    def _ensure_unique_columns(self, columns: list) -> list:
        """Ensure column names are unique"""
        seen = {}
        unique_columns = []
        
        for col in columns:
            if col in seen:
                seen[col] += 1
                unique_columns.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 0
                unique_columns.append(col)
        
        return unique_columns
    
    def _get_file_extension(self, filename: str) -> str:
        """Get file extension from filename"""
        import os
        return os.path.splitext(filename.lower())[1]
    
    def get_file_info(self, uploaded_file) -> dict:
        """Get information about uploaded file"""
        try:
            return {
                'name': uploaded_file.name,
                'size': uploaded_file.size,
                'type': uploaded_file.type,
                'size_mb': round(uploaded_file.size / (1024 * 1024), 2)
            }
        except Exception as e:
            logging.error(f"Error getting file info: {e}")
            return {}
    
    def preview_file_content(self, uploaded_file, max_rows: int = 10) -> Optional[pd.DataFrame]:
        """Get a quick preview of file content without full processing"""
        try:
            file_extension = self._get_file_extension(uploaded_file.name)
            
            # Reset file pointer
            uploaded_file.seek(0)
            
            if file_extension == '.csv':
                # Quick CSV preview
                df = pd.read_csv(uploaded_file, nrows=max_rows)
                return df
            elif file_extension in ['.xlsx', '.xls']:
                # Quick Excel preview
                df = pd.read_excel(uploaded_file, nrows=max_rows)
                return df
            
        except Exception as e:
            logging.error(f"Preview error: {e}")
        
        return None
