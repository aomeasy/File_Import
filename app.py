import streamlit as st
import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
from datetime import datetime
import time
import base64
from functools import lru_cache
import threading
from concurrent.futures import ThreadPoolExecutor

# Import modules with error handling
try:
    from database import DatabaseManager
except ImportError as e:
    st.error(f"Cannot import DatabaseManager: {e}")
    st.stop()

try:
    from file_processor import FileProcessor
except ImportError as e:
    st.error(f"Cannot import FileProcessor: {e}")
    st.stop()

# Configure page
st.set_page_config(
    page_title="Data Management Hub",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ===== CACHING FUNCTIONS =====
@st.cache_data(ttl=300)
def get_cached_tables_info():
    """Cache table information to avoid repeated DB queries"""
    try:
        db_manager = DatabaseManager()
        return db_manager.get_tables_with_info()
    except Exception as e:
        st.error(f"Error getting tables info: {e}")
        return []

@st.cache_data(ttl=300)
def get_cached_table_columns(table_name):
    """Cache table columns to avoid repeated queries"""
    try:
        db_manager = DatabaseManager()
        return db_manager.get_table_columns(table_name)
    except Exception as e:
        return []

@st.cache_data(ttl=60)
def get_cached_table_preview(table_name, limit=5):
    """Cache table preview with smaller limit"""
    try:
        # ใช้ instance ที่มีอยู่ใน session_state
        if 'db_manager' in st.session_state:
            return st.session_state.db_manager.get_table_preview(table_name, limit)
        else:
            db_manager = DatabaseManager()
            return db_manager.get_table_preview(table_name, limit)
    except Exception as e:
        st.error(f"Preview error: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_stored_procedures():
    """Get list of stored procedures from database"""
    try:
        db_manager = DatabaseManager()
        query = """
        SELECT 
            ROUTINE_NAME,
            ROUTINE_TYPE,
            DTD_IDENTIFIER as RETURNS,
            CREATED,
            LAST_ALTERED,
            ROUTINE_COMMENT
        FROM INFORMATION_SCHEMA.ROUTINES
        WHERE ROUTINE_SCHEMA = %s
        ORDER BY ROUTINE_NAME
        """
        df = db_manager.execute_query(query, (db_manager.connection_config['database'],))
        return df.to_dict('records') if not df.empty else []
    except Exception as e:
        st.error(f"Error getting procedures: {e}")
        return []

def get_procedure_parameters(procedure_name):
    """Get parameters for a stored procedure"""
    try:
        db_manager = DatabaseManager()
        query = """
        SELECT 
            PARAMETER_NAME,
            PARAMETER_MODE,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION
        FROM INFORMATION_SCHEMA.PARAMETERS
        WHERE SPECIFIC_SCHEMA = %s 
        AND SPECIFIC_NAME = %s
        AND PARAMETER_NAME IS NOT NULL
        ORDER BY ORDINAL_POSITION
        """
        df = db_manager.execute_query(query, (db_manager.connection_config['database'], procedure_name))
        return df.to_dict('records') if not df.empty else []
    except Exception as e:
        st.error(f"Error getting parameters: {e}")
        return []

def execute_procedure(procedure_name, parameters=None):
    """Execute a stored procedure with its own short-lived connection"""
    try:
        db_manager = DatabaseManager()

        # ✅ เปิด connection ใหม่ เฉพาะงานนี้เท่านั้น (แยกจาก session_state.db_manager)
        conn = mysql.connector.connect(
            host=db_manager.connection_config['host'],
            port=db_manager.connection_config.get('port', 3306),
            database=db_manager.connection_config['database'],
            user=db_manager.connection_config['user'],
            password=db_manager.connection_config['password'],
            charset=db_manager.connection_config.get('charset', 'utf8mb4'),
            autocommit=False,          # ใช้ทรานแซกชันของตัวเอง
            connection_timeout=10
        )

        cursor = conn.cursor()

        if parameters:
            placeholders = ', '.join(['%s'] * len(parameters))
            cursor.execute(f"CALL {procedure_name}({placeholders})", parameters)
        else:
            cursor.execute(f"CALL {procedure_name}()")

        # ดึงผลลัพธ์ (ถ้ามี)
        results = []
        try:
            for result in cursor.stored_results():
                results.append(result.fetchall())
        except Exception:
            pass

        conn.commit()                 # ✅ จบทรานแซกชันของตัวเอง
        cursor.close()
        conn.close()                  # ✅ ปิดคอนเน็กชันทันที

        return {'success': True, 'message': f'Procedure {procedure_name} executed successfully', 'results': results}

    except Error as e:
        # 🔁 ถ้ามีเออเรอร์ให้ rollback และปิดคอนเน็กชัน
        try:
            conn.rollback()
            cursor.close()
            conn.close()
        except Exception:
            pass
        return {'success': False, 'error': str(e)}
    except Exception as e:
        try:
            conn.rollback()
            cursor.close()
            conn.close()
        except Exception:
            pass
        return {'success': False, 'error': str(e)}


# ===== CSS STYLING =====
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 8px;
        margin-bottom: 1rem;
        text-align: center;
        color: white;
    }
    
    .metric-card {
        background: #f0f2f6;
        padding: 1rem;
        border-radius: 8px;
        text-align: center;
        margin: 0.5rem 0;
        border: 1px solid #e0e0e0;
    }
    
    .status-success {
        background: #d4edda;
        padding: 0.5rem;
        border-radius: 4px;
        color: #155724;
        margin: 0.5rem 0;
    }
    
    .status-error {
        background: #f8d7da;
        padding: 0.5rem;
        border-radius: 4px;
        color: #721c24;
        margin: 0.5rem 0;
    }
    
    .file-info {
        background: white;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #9CAF88;
        box-shadow: 0 2px 8px rgba(139, 69, 19, 0.1);
        margin: 1rem 0;
    }
    
    .header-match {
        background: #d4edda;
        color: #155724;
        padding: 0.25rem 0.5rem;
        border-radius: 4px;
        margin: 0.1rem;
        display: inline-block;
        font-size: 0.85rem;
        font-weight: bold;
    }
    
    .header-no-match {
        background: #f8d7da;
        color: #721c24;
        padding: 0.25rem 0.5rem;
        border-radius: 4px;
        margin: 0.1rem;
        display: inline-block;
        font-size: 0.85rem;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# ===== FILE MERGER CLASS =====
class FileMerger:
    def __init__(self):
        self.uploaded_files = []
        self.processed_data = {}
        self.merged_df = None
        self.header_mapping = {}
        
    def process_uploaded_files(self, files):
        """Process uploaded files and extract data"""
        processed = {}
        
        for file in files:
            file_info = {
                'name': file.name,
                'size': file.size,
                'type': self.get_file_type(file.name)
            }
            
            try:
                if file_info['type'] == 'csv':
                    df = pd.read_csv(file)
                    file_info['sheets'] = ['Sheet1']
                    file_info['data'] = {'Sheet1': df}
                    
                elif file_info['type'] == 'excel':
                    excel_file = pd.ExcelFile(file)
                    file_info['sheets'] = excel_file.sheet_names
                    file_info['data'] = {}
                    
                    for sheet in excel_file.sheet_names:
                        df = pd.read_excel(file, sheet_name=sheet)
                        file_info['data'][sheet] = df
                        
                processed[file.name] = file_info
                
            except Exception as e:
                st.error(f"Error processing {file.name}: {str(e)}")
                
        return processed
    
    def get_file_type(self, filename):
        """Determine file type from filename"""
        if filename.lower().endswith('.csv'):
            return 'csv'
        elif filename.lower().endswith(('.xlsx', '.xls')):
            return 'excel'
        return 'unknown'
    
    def analyze_headers(self, processed_data, selected_sheets, selected_files):
        """Analyze headers across all selected sheets"""
        all_headers = set()
        file_headers = {}
        
        for filename, file_info in processed_data.items():
            if selected_files.get(filename, True):
                sheet_name = selected_sheets.get(filename, file_info['sheets'][0])
                if sheet_name in file_info['data']:
                    df = file_info['data'][sheet_name]
                    headers = list(df.columns)
                    file_headers[filename] = headers
                    all_headers.update(headers)
        
        all_headers_list = list(all_headers)
        has_mismatch = False
        
        if len(file_headers) > 1:
            reference_headers = set(next(iter(file_headers.values())))
            for filename, headers in file_headers.items():
                if set(headers) != reference_headers:
                    has_mismatch = True
                    break
                    
        return all_headers_list, has_mismatch, file_headers
    
    def merge_files(self, processed_data, selected_sheets, selected_files, header_mapping=None, excluded_headers=None):
        """Merge all files into a single DataFrame"""
        merged_dfs = []
        
        for filename, file_info in processed_data.items():
            if selected_files.get(filename, True):
                sheet_name = selected_sheets.get(filename, file_info['sheets'][0])
                if sheet_name in file_info['data']:
                    df = file_info['data'][sheet_name].copy()
                    
                    if excluded_headers and filename in excluded_headers:
                        columns_to_keep = [col for col in df.columns if col not in excluded_headers[filename]]
                        df = df[columns_to_keep]
                    
                    if header_mapping and filename in header_mapping:
                        df.rename(columns=header_mapping[filename], inplace=True)
                    
                    df['_source_file'] = filename
                    merged_dfs.append(df)
        
        if merged_dfs:
            return pd.concat(merged_dfs, ignore_index=True, sort=False)
        return pd.DataFrame()

# ===== TAB 1: IMPORT DATA =====
def render_import_tab():
    """Render the Import tab"""
    
    # ===== STATS SECTION AT TOP =====
    st.subheader("📊 Quick Stats")
    
    col_stat1, col_stat2, col_stat3 = st.columns(3)
    
    with col_stat1:
        try:
            tables_info = get_cached_tables_info()
            tables = [table['TABLE_NAME'] for table in tables_info] if tables_info else []
            st.metric("📁 Total Tables", len(tables))
        except:
            st.metric("📁 Total Tables", "N/A")
    
    with col_stat2:
        if 'connection_status' in st.session_state and st.session_state.connection_status:
            st.metric("🔌 Database Status", "Connected", delta="Online", delta_color="normal")
        else:
            st.metric("🔌 Database Status", "Disconnected", delta="Offline", delta_color="inverse")
    
    with col_stat3:
        if st.button("🔄 Refresh All", use_container_width=True, key="refresh_import_top"):
            st.cache_data.clear()
            st.rerun()
    
    st.divider()
    
    # ===== MAIN IMPORT SECTION (FULL WIDTH) =====
    st.header("📁 File Import to Database")
    
    if 'db_manager' not in st.session_state:
        st.session_state.db_manager = DatabaseManager()
    if 'file_processor' not in st.session_state:
        st.session_state.file_processor = FileProcessor()
    
    try:
        tables_info = get_cached_tables_info()
        tables = [table['TABLE_NAME'] for table in tables_info] if tables_info else []
    except Exception as e:
        st.warning(f"Could not get table info: {e}")
        tables = []
        tables_info = []
    
    selected_table = st.selectbox(
        "🎯 Select Target Table",
        options=[""] + tables,
        help="Choose the table where you want to import your data"
    )
    
    if selected_table:
        if tables_info:
            table_details = next((t for t in tables_info if t.get('TABLE_NAME') == selected_table), None)
            
            if table_details:
                col1_info, col2_info, col3_info = st.columns(3)
                
                with col1_info:
                    row_count = table_details.get('TABLE_ROWS', 0) or 0
                    st.metric("📊 Rows", f"{row_count:,}")
                
                with col2_info:
                    update_time = table_details.get('UPDATE_TIME')
                    if update_time:
                        try:
                            if isinstance(update_time, str):
                                last_update = update_time[:10]
                            else:
                                last_update = update_time.strftime("%Y-%m-%d")
                            st.metric("🕒 Updated", last_update)
                        except:
                            st.metric("🕒 Updated", "Unknown")
                
                with col3_info:
                    data_length = table_details.get('DATA_LENGTH', 0) or 0
                    if data_length > 0:
                        size_mb = data_length / (1024 * 1024)
                        st.metric("💾 Size", f"{size_mb:.0f} MB")
        
        st.subheader(f"👀 Preview: {selected_table}")
        
        if st.button("🔄 Show Preview", type="secondary"):
            try:
                with st.spinner("Loading preview..."):
                    preview_data = get_cached_table_preview(selected_table, 5)
                
                if not preview_data.empty:
                    st.dataframe(preview_data, use_container_width=True, hide_index=True)
                    st.success(f"📊 Showing last 5 rows from {len(preview_data.columns)} columns")
                else:
                    st.warning("📭 Table is empty or preview unavailable")
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
        
        st.subheader("📤 Upload File")
        uploaded_file = st.file_uploader(
            "Choose a file to import",
            type=['csv', 'xlsx', 'xls'],
            help="Max size: 200MB",
            key="import_uploader"
        )
        
        if uploaded_file:
            st.markdown(f"""
            <div class="file-info">
                <h4>📄 {uploaded_file.name}</h4>
                <p><strong>Size:</strong> {uploaded_file.size / 1024:.2f} KB</p>
                <p><strong>Type:</strong> {uploaded_file.type}</p>
            </div>
            """, unsafe_allow_html=True)
            
            try:
                # Read file
                with st.spinner("Reading file..."):
                    if uploaded_file.name.endswith('.csv'):
                        df = pd.read_csv(uploaded_file)
                    else:
                        df = pd.read_excel(uploaded_file)
                
                st.success(f"✅ File loaded: {len(df)} rows, {len(df.columns)} columns")
                
                # Preview data
                st.subheader("📋 Data Preview")
                st.dataframe(df.head(10), use_container_width=True)
                
                # Column mapping
                st.subheader("🔗 Column Mapping")
                
                table_columns = get_cached_table_columns(selected_table)
                
                if not table_columns:
                    st.error("Cannot get table columns")
                    return
                
                db_column_names = [col['COLUMN_NAME'] for col in table_columns]
                file_columns = list(df.columns)
                
                st.info(f"**File Columns:** {len(file_columns)} | **Table Columns:** {len(db_column_names)}")
                
                # Auto-mapping
                column_mapping = {}
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write("**File Column**")
                with col2:
                    st.write("**→ Database Column**")
                
                for file_col in file_columns:
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.text(file_col)
                    
                    with col2:
                        # Auto-match if column names are similar
                        default_index = 0
                        if file_col in db_column_names:
                            default_index = db_column_names.index(file_col)
                        
                        selected_db_col = st.selectbox(
                            f"Map {file_col}",
                            options=["-- Skip --"] + db_column_names,
                            index=default_index + 1 if file_col in db_column_names else 0,
                            key=f"mapping_{file_col}",
                            label_visibility="collapsed"
                        )
                        
                        if selected_db_col != "-- Skip --":
                            column_mapping[file_col] = selected_db_col
                
                # Show mapping summary
                if column_mapping:
                    st.success(f"✅ Mapped {len(column_mapping)} columns")
                    
                    with st.expander("View Mapping Details"):
                        for file_col, db_col in column_mapping.items():
                            st.write(f"**{file_col}** → **{db_col}**")
                else:
                    st.warning("⚠️ No columns mapped")
                
                # Import button
                st.divider()
                
                col1, col2, col3 = st.columns([1, 1, 2])
                
                with col1:
                    if st.button("🚀 Import Data", type="primary", disabled=len(column_mapping) == 0):
                        if not column_mapping:
                            st.error("Please map at least one column")
                        else:
                            # สร้าง DatabaseManager ใหม่เลย ไม่ใช้อันเก่า
                            from database import DatabaseManager
                            fresh_db = DatabaseManager()
                            
                            with st.spinner(f"Importing {len(df)} rows..."):
                                result = fresh_db.import_data(
                                    selected_table,
                                    df,
                                    column_mapping
                                )
                            
                            # ปิด connection
                            fresh_db.close_connection()
                            
                            if result['success']:
                                st.success(f"✅ {result['message']}")
                                st.balloons()
                                st.cache_data.clear()
                                st.metric("Rows Imported", result['rows_affected'])
                            else:
                                st.error(f"❌ Import failed: {result['error']}")
                
                with col2:
                    if st.button("🔄 Reset", type="secondary"):
                        st.rerun()
            
            except Exception as e:
                st.error(f"❌ Error processing file: {str(e)}")
                st.exception(e)

# ===== TAB 2: RUN PROCEDURES =====
def render_procedures_tab():
    """Render the Procedures tab"""
    st.header("⚙️ Database Procedures & Updates")

    # 🔒 กดปุ่มเพื่อเปิดใช้งานเท่านั้น (ป้องกันรันทุกครั้งที่แอป rerun)
    enabled = st.toggle("Enable this tab (load from DB)", value=False, help="Turn on only when you want to work with procedures")
    if not enabled:
        st.info("This tab is idle. Turn on the toggle to load procedures.")
        return
    
    if 'db_manager' not in st.session_state:
        st.session_state.db_manager = DatabaseManager()
    
    if 'execution_history' not in st.session_state:
        st.session_state.execution_history = []
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("🔧 Stored Procedures")
        
        if st.button("🔄 Refresh Procedures List", type="secondary", key="refresh_procedures"):
            st.cache_data.clear()
            st.rerun()
        
        procedures = get_stored_procedures()
        
        if procedures:
            st.info(f"Found {len(procedures)} stored procedures")
            
            search_query = st.text_input("🔍 Search procedures", placeholder="Type to filter...", key="search_proc")
            
            if search_query:
                filtered_procedures = [p for p in procedures if search_query.lower() in p['ROUTINE_NAME'].lower()]
            else:
                filtered_procedures = procedures
            
            for proc in filtered_procedures:
                with st.expander(f"📦 {proc['ROUTINE_NAME']} ({proc['ROUTINE_TYPE']})"):
                    col_info, col_exec = st.columns([1, 1])
                    
                    with col_info:
                        st.write(f"**Type:** {proc['ROUTINE_TYPE']}")
                        if proc.get('ROUTINE_COMMENT'):
                            st.write(f"**Description:** {proc['ROUTINE_COMMENT']}")
                        if proc.get('CREATED'):
                            st.write(f"**Created:** {proc['CREATED']}")
                    
                    with col_exec:
                        params = get_procedure_parameters(proc['ROUTINE_NAME'])
                        
                        if params:
                            st.write(f"**Parameters:** {len(params)}")
                            param_values = []
                            
                            for param in params:
                                param_name = param['PARAMETER_NAME']
                                param_type = param['DATA_TYPE']
                                
                                if param_type in ['int', 'bigint', 'smallint']:
                                    val = st.number_input(
                                        f"{param_name} ({param_type})",
                                        value=0,
                                        key=f"param_{proc['ROUTINE_NAME']}_{param_name}"
                                    )
                                elif param_type in ['decimal', 'float', 'double']:
                                    val = st.number_input(
                                        f"{param_name} ({param_type})",
                                        value=0.0,
                                        format="%.2f",
                                        key=f"param_{proc['ROUTINE_NAME']}_{param_name}"
                                    )
                                elif param_type in ['date', 'datetime', 'timestamp']:
                                    val = st.date_input(
                                        f"{param_name} ({param_type})",
                                        key=f"param_{proc['ROUTINE_NAME']}_{param_name}"
                                    )
                                    val = val.strftime("%Y-%m-%d")
                                else:
                                    val = st.text_input(
                                        f"{param_name} ({param_type})",
                                        key=f"param_{proc['ROUTINE_NAME']}_{param_name}"
                                    )
                                
                                param_values.append(val)
                        else:
                            st.info("No parameters required")
                            param_values = None
                    
                    st.divider()
                    
                    col_btn1, col_btn2 = st.columns(2)
                    
                    with col_btn1:
                        if st.button(f"▶️ Execute", key=f"exec_{proc['ROUTINE_NAME']}", type="primary", use_container_width=True):
                            with st.spinner(f"Executing {proc['ROUTINE_NAME']}..."):
                                result = execute_procedure(proc['ROUTINE_NAME'], param_values if params else None)
                            
                            if result['success']:
                                st.success(f"✅ {result['message']}")
                                
                                if result.get('results'):
                                    for idx, res in enumerate(result['results']):
                                        st.write(f"**Result Set {idx + 1}:**")
                                        df_result = pd.DataFrame(res)
                                        st.dataframe(df_result, use_container_width=True)
                                        
                                        # Download buttons
                                        csv_data = df_result.to_csv(index=False)
                                        st.download_button(
                                            "📥 Download CSV",
                                            csv_data,
                                            f"{proc['ROUTINE_NAME']}_result_{idx+1}.csv",
                                            "text/csv",
                                            key=f"download_csv_{proc['ROUTINE_NAME']}_{idx}"
                                        )
                                
                                if result.get('rows_affected'):
                                    st.info(f"Rows affected: {result['rows_affected']}")
                                
                                if result.get('warnings'):
                                    with st.expander("⚠️ Warnings"):
                                        for warning in result['warnings']:
                                            st.warning(f"{warning[0]}: {warning[2]}")
                                
                                # Add to history
                                st.session_state.execution_history.append({
                                    'procedure': proc['ROUTINE_NAME'],
                                    'status': 'success',
                                    'timestamp': datetime.now()
                                })
                            else:
                                st.error(f"❌ Execution failed")
                                
                                if result.get('error_details'):
                                    details = result['error_details']
                                    st.error(f"**Error:** {details.get('msg')}")
                                    if details.get('errno'):
                                        st.caption(f"Error Code: {details['errno']}")
                                    if details.get('sqlstate'):
                                        st.caption(f"SQL State: {details['sqlstate']}")
                                else:
                                    st.error(result.get('error', 'Unknown error'))
                                
                                # Add to history
                                st.session_state.execution_history.append({
                                    'procedure': proc['ROUTINE_NAME'],
                                    'status': 'failed',
                                    'timestamp': datetime.now()
                                })
        
        else:
            st.warning("⚠️ No stored procedures found in database")
    
    with col2:
        st.subheader("📊 Quick Stats")
        
        if procedures:
            st.metric("Total Procedures", len(procedures))
        
        if st.session_state.execution_history:
            success_count = sum(1 for h in st.session_state.execution_history if h['status'] == 'success')
            failed_count = len(st.session_state.execution_history) - success_count
            
            st.metric("Executions", len(st.session_state.execution_history))
            
            col_s, col_f = st.columns(2)
            with col_s:
                st.metric("✅ Success", success_count)
            with col_f:
                st.metric("❌ Failed", failed_count)
        
        st.divider()
        
        if st.button("🗑️ Clear History", use_container_width=True):
            st.session_state.execution_history = []
            st.rerun()

# ===== TAB 3: FILE MERGER =====
def render_merger_tab():
    """Render the File Merger tab with download format selection"""
    st.header("📁 File Merger")
    st.write("รวมไฟล์ CSV และ Excel หลายไฟล์เข้าด้วยกัน")
    
    # Initialize merger
    if 'merger' not in st.session_state:
        st.session_state.merger = FileMerger()
    if 'merger_processed_data' not in st.session_state:
        st.session_state.merger_processed_data = {}
    if 'merger_merged_df' not in st.session_state:
        st.session_state.merger_merged_df = None
    if 'merger_selected_files' not in st.session_state:
        st.session_state.merger_selected_files = {}
    
    merger = st.session_state.merger
    
    # File upload section
    st.subheader("📤 อัปโหลดไฟล์")
    uploaded_files = st.file_uploader(
        "เลือกไฟล์ CSV หรือ Excel",
        type=['csv', 'xlsx', 'xls'],
        accept_multiple_files=True,
        help="รองรับไฟล์ CSV และ Excel หลายไฟล์",
        key="merger_uploader"
    )
    
    if uploaded_files:
        if len(uploaded_files) != len(st.session_state.get('merger_last_uploaded', [])):
            with st.spinner("กำลังประมวลผลไฟล์..."):
                st.session_state.merger_processed_data = merger.process_uploaded_files(uploaded_files)
                st.session_state.merger_last_uploaded = uploaded_files
                st.session_state.merger_merged_df = None
                st.session_state.merger_selected_files = {f.name: True for f in uploaded_files}
    
    # ===== MAIN CONTENT AFTER UPLOAD =====
    if st.session_state.merger_processed_data:
        # File selection
        if len(st.session_state.merger_processed_data) > 1:
            st.subheader("🎯 เลือกไฟล์สำหรับการรวม")
            
            cols = st.columns(min(len(st.session_state.merger_processed_data), 3))
            
            for i, (filename, file_info) in enumerate(st.session_state.merger_processed_data.items()):
                with cols[i % 3]:
                    selected = st.checkbox(
                        filename,
                        value=st.session_state.merger_selected_files.get(filename, True),
                        key=f"merger_select_{filename}",
                        help=f"ขนาด: {file_info['size']/1024:.1f} KB"
                    )
                    st.session_state.merger_selected_files[filename] = selected
            
            selected_count = sum(st.session_state.merger_selected_files.values())
            
            if selected_count == 0:
                st.error("⚠️ กรุณาเลือกไฟล์อย่างน้อย 1 ไฟล์")
                return
        else:
            filename = list(st.session_state.merger_processed_data.keys())[0]
            st.session_state.merger_selected_files = {filename: True}
        
        # File information
        st.subheader("📋 ไฟล์ที่อัปโหลด")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            selected_sheets = {}
            
            for filename, file_info in st.session_state.merger_processed_data.items():
                is_selected = st.session_state.merger_selected_files.get(filename, True)
                
                with st.expander(f"{'✅' if is_selected else '❌'} {filename}", expanded=is_selected):
                    col_info, col_sheet = st.columns([2, 1])
                    
                    with col_info:
                        st.markdown(f"""
                        **ขนาด:** {file_info['size']/1024:.2f} KB  
                        **ประเภท:** {file_info['type'].upper()}  
                        **จำนวน Sheets:** {len(file_info['sheets'])}
                        """)
                    
                    with col_sheet:
                        if len(file_info['sheets']) > 1:
                            selected_sheet = st.selectbox(
                                "เลือก Sheet:",
                                file_info['sheets'],
                                key=f"merger_sheet_{filename}",
                                disabled=not is_selected
                            )
                            selected_sheets[filename] = selected_sheet
                        else:
                            selected_sheets[filename] = file_info['sheets'][0]
                            st.info(f"Sheet: {file_info['sheets'][0]}")
                    
                    if is_selected:
                        sheet_name = selected_sheets[filename]
                        if sheet_name in file_info['data']:
                            df = file_info['data'][sheet_name]
                            st.write(f"**Preview ({len(df)} แถว, {len(df.columns)} คอลัมน์):**")
                            st.dataframe(df.head(5), use_container_width=True)
        
        with col2:
            # Statistics
            selected_files_data = {k: v for k, v in st.session_state.merger_processed_data.items() 
                                 if st.session_state.merger_selected_files.get(k, True)}
            
            total_files = len(selected_files_data)
            total_records = sum([
                len(file_info['data'][selected_sheets.get(filename, file_info['sheets'][0])]) 
                for filename, file_info in selected_files_data.items()
                if selected_sheets.get(filename, file_info['sheets'][0]) in file_info['data']
            ]) if selected_files_data else 0
            
            st.markdown(f"""
            <div class="metric-card">
                <h3>📊 สถิติ</h3>
                <p><strong>ไฟล์ที่เลือก:</strong> {total_files}</p>
                <p><strong>จำนวนแถวรวม:</strong> {total_records:,}</p>
            </div>
            """, unsafe_allow_html=True)
        
        # Header analysis
        st.header("🔍 การวิเคราะห์ Headers")
        
        all_headers, has_mismatch, file_headers = merger.analyze_headers(
            st.session_state.merger_processed_data,
            selected_sheets,
            st.session_state.merger_selected_files
        )
        
        if has_mismatch and len(file_headers) > 1:
            st.warning("⚠️ พบความไม่สอดคล้องของ Headers")
            
            # Show comparison
            for filename, headers in file_headers.items():
                with st.expander(f"Headers ของ {filename}"):
                    st.write(f"**จำนวน:** {len(headers)} headers")
                    st.write(", ".join(headers))
            
            st.info("💡 คุณสามารถรวมไฟล์ได้ทันที Headers ที่ไม่ตรงกันจะเป็นค่าว่าง")
        
        elif len(file_headers) > 1:
            st.success("✅ Headers ทั้งหมดสอดคล้องกัน")
        
        # Merge button
        st.header("⚙️ การรวมไฟล์")
        
        if st.button("🚀 เริ่มรวมไฟล์", type="primary", use_container_width=True, key="merge_files_btn"):
            with st.spinner("กำลังรวมไฟล์..."):
                merged_df = merger.merge_files(
                    st.session_state.merger_processed_data,
                    selected_sheets,
                    st.session_state.merger_selected_files
                )
                
                st.session_state.merger_merged_df = merged_df
                st.success(f"✅ รวมไฟล์สำเร็จ! {len(merged_df):,} แถว")
        
        # Show results
        if st.session_state.merger_merged_df is not None:
            st.header("📊 ผลลัพธ์การรวมไฟล์")
            
            merged_df = st.session_state.merger_merged_df
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("จำนวนแถวรวม", f"{len(merged_df):,}")
            with col2:
                st.metric("จำนวนคอลัมน์", len(merged_df.columns))
            with col3:
                st.metric("ไฟล์ที่รวม", sum(st.session_state.merger_selected_files.values()))
            
            st.subheader("ตัวอย่างข้อมูล")
            st.dataframe(merged_df.head(100), use_container_width=True)
            
            # ===== DOWNLOAD SECTION =====
            st.header("⬇️ ดาวน์โหลด")
            
            col1, col2 = st.columns([1, 2])
            
            with col1:
                download_format = st.radio(
                    "เลือกรูปแบบไฟล์:",
                    options=["CSV", "Excel (XLSX)"],
                    index=0,
                    key="download_format"
                )
            
            with col2:
                if download_format == "CSV":
                    filename = f"merged_file_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    csv_data = merged_df.to_csv(index=False, encoding='utf-8-sig')
                    file_size = len(csv_data.encode('utf-8')) / 1024
                    
                    st.info(f"📄 CSV | ขนาด: {file_size:.2f} KB")
                    
                    st.download_button(
                        label="📥 ดาวน์โหลดไฟล์ CSV",
                        data=csv_data,
                        file_name=filename,
                        mime="text/csv",
                        type="primary",
                        use_container_width=True,
                        key="download_merged_csv"
                    )
                
                else:
                    filename = f"merged_file_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                    
                    from io import BytesIO
                    output = BytesIO()
                    
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        merged_df.to_excel(writer, index=False, sheet_name='Merged Data')
                        
                        workbook = writer.book
                        worksheet = writer.sheets['Merged Data']
                        
                        for column in worksheet.columns:
                            max_length = 0
                            column_letter = column[0].column_letter
                            
                            for cell in column:
                                try:
                                    if len(str(cell.value)) > max_length:
                                        max_length = len(str(cell.value))
                                except:
                                    pass
                            
                            adjusted_width = min(max_length + 2, 50)
                            worksheet.column_dimensions[column_letter].width = adjusted_width
                        
                        worksheet.auto_filter.ref = worksheet.dimensions
                    
                    excel_data = output.getvalue()
                    file_size = len(excel_data) / 1024
                    
                    st.info(f"📊 Excel | ขนาด: {file_size:.2f} KB")
                    
                    st.download_button(
                        label="📥 ดาวน์โหลดไฟล์ Excel",
                        data=excel_data,
                        file_name=filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        type="primary",
                        use_container_width=True,
                        key="download_merged_excel"
                    )
    
    else:
        st.info("👆 กรุณาอัปโหลดไฟล์เพื่อเริ่มต้นใช้งาน")

# ===== MAIN APPLICATION =====
def main():
    try:
        # Header
        st.markdown("""
        <div class="main-header">
            <h1>🚀 Data Management Hub</h1>
            <p>Complete data management system with import, procedures, and file merger</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Initialize components
        if 'db_manager' not in st.session_state:
            try:
                st.session_state.db_manager = DatabaseManager()
            except Exception as e:
                st.error(f"Failed to initialize DatabaseManager: {e}")
                return
        
        if 'file_processor' not in st.session_state:
            try:
                st.session_state.file_processor = FileProcessor()
            except Exception as e:
                st.error(f"Failed to initialize FileProcessor: {e}")
                return

        # Sidebar configuration
        with st.sidebar:
            st.header("⚙️ Configuration")
            
            if 'connection_status' not in st.session_state:
                try:
                    st.session_state.connection_status = st.session_state.db_manager.test_connection()
                except Exception as e:
                    st.session_state.connection_status = False
            
            if st.session_state.connection_status:
                st.markdown('<div class="status-success">✅ Database Connected</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="status-error">❌ Database Connection Failed</div>', unsafe_allow_html=True)
            
            if st.button("🔄 Refresh", key="refresh_sidebar"):
                st.cache_data.clear()
                st.rerun()
            
            try:
                tables_info = get_cached_tables_info()
                tables = [table['TABLE_NAME'] for table in tables_info] if tables_info else []
            except Exception as e:
                tables = []
                tables_info = []

            st.write(f"📊 Available Tables: {len(tables)}")
        
        # ===== TABS NAVIGATION =====
        tab1, tab2, tab3 = st.tabs(["📁 Import Data", "⚙️ Run Procedures", "🔗 File Merger"])
        
        with tab1:
            render_import_tab()
        
        with tab2:
            render_procedures_tab()
        
        with tab3:
            render_merger_tab()
    
    except Exception as e:
        st.error(f"Application error: {e}")

if __name__ == "__main__":
    main()
