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
        db_manager = DatabaseManager()
        query = f"SELECT * FROM {table_name} ORDER BY id DESC LIMIT {limit}"
        return db_manager.execute_query(query)
    except Exception as e:
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
    """Execute a stored procedure"""
    try:
        db_manager = DatabaseManager()
        result = db_manager.execute_stored_procedure(procedure_name, parameters)
        return result
    except Exception as e:
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
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.header("📁 File Import")
        
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
                # Import logic (keeping original code)
                pass
    
    with col2:
        st.header("📊 Stats")
        
        try:
            tables_info = get_cached_tables_info()
            tables = [table['TABLE_NAME'] for table in tables_info] if tables_info else []
            
            if tables:
                st.markdown(f"""
                <div class="metric-card">
                    <h3>{len(tables)}</h3>
                    <p>Tables</p>
                </div>
                """, unsafe_allow_html=True)
        except:
            pass
        
        st.subheader("⚡ Actions")
        if st.button("🔄 Refresh All", use_container_width=True, key="refresh_import"):
            st.cache_data.clear()
            st.rerun()

# ===== TAB 2: RUN PROCEDURES =====
def render_procedures_tab():
    """Render the Procedures tab"""
    st.header("⚙️ Database Procedures & Updates")
    
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
                    # Procedure execution logic (keeping original code)
                    pass
        
        else:
            st.warning("⚠️ No stored procedures found in database")
    
    with col2:
        st.subheader("📊 Quick Stats")
        # Stats logic (keeping original code)
        pass

# ===== TAB 3: FILE MERGER (COMPLETE VERSION) =====
def render_merger_tab():
    """Render the File Merger tab with full header analysis"""
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
            st.session_state.merger_processed_data = merger.process_uploaded_files(uploaded_files)
            st.session_state.merger_last_uploaded = uploaded_files
            st.session_state.merger_merged_df = None
            st.session_state.merger_selected_files = {f.name: True for f in uploaded_files}
    
    # Main merger content
    if st.session_state.merger_processed_data:
        # File selection
        if len(st.session_state.merger_processed_data) > 1:
            st.subheader("🎯 เลือกไฟล์สำหรับการรวม")
            
            cols = st.columns(min(len(st.session_state.merger_processed_data), 3))
            
            for i, (filename, file_info) in enumerate(st.session_state.merger_processed_data.items()):
                with cols[i % 3]:
                    selected = st.checkbox(
                        f"{filename}",
                        value=st.session_state.merger_selected_files.get(filename, True),
                        key=f"merger_select_{filename}",
                        help=f"ขนาด: {file_info['size']/1024:.1f} KB"
                    )
                    st.session_state.merger_selected_files[filename] = selected
            
            selected_count = sum(st.session_state.merger_selected_files.values())
            
            if selected_count == 0:
                st.error("กรุณาเลือกไฟล์อย่างน้อย 1 ไฟล์")
                return
            elif selected_count < len(st.session_state.merger_processed_data):
                st.info(f"เลือกแล้ว {selected_count} จาก {len(st.session_state.merger_processed_data)} ไฟล์")
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
                        css_class = "file-info" if is_selected else "file-info disabled"
                        status_text = "เลือกสำหรับการรวม" if is_selected else "ไม่รวมในการประมวลผล"
                        st.markdown(f"""
                        <div class="{css_class}">
                            <strong>สถานะ:</strong> {status_text}<br>
                            <strong>ขนาด:</strong> {file_info['size']/1024:.2f} KB<br>
                            <strong>ประเภท:</strong> {file_info['type'].upper()}<br>
                            <strong>จำนวน Sheets:</strong> {len(file_info['sheets'])}
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col_sheet:
                        if len(file_info['sheets']) > 1:
                            selected_sheet = st.selectbox(
                                "เลือก Sheet:",
                                file_info['sheets'],
                                key=f"merger_sheet_{filename}",
                                index=0,
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
                            st.dataframe(df.head(3), use_container_width=True)
                    else:
                        st.markdown("*ไฟล์นี้จะไม่ถูกรวมในการประมวลผล*")
        
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
            
            excluded_files = len(st.session_state.merger_processed_data) - total_files
            
            st.markdown(f"""
            <div class="metric-card">
                <h3>📊 สถิติ</h3>
                <p><strong>ไฟล์ที่เลือก:</strong> {total_files}</p>
                <p><strong>ไฟล์ที่ไม่เลือก:</strong> {excluded_files}</p>
                <p><strong>จำนวนแถวรวม:</strong> {total_records:,}</p>
            </div>
            """, unsafe_allow_html=True)
        
        # ===== HEADER ANALYSIS SECTION =====
        if any(st.session_state.merger_selected_files.values()):
            st.header("🔍 การวิเคราะห์ Headers")
            
            all_headers, has_mismatch, file_headers = merger.analyze_headers(
                st.session_state.merger_processed_data,
                selected_sheets,
                st.session_state.merger_selected_files
            )
            
            if has_mismatch and len(file_headers) > 1:
                st.warning("พบความไม่สอดคล้องของ Headers - กรุณาตรวจสอบและปรับแต่ง")
                
                # Show header comparison
                st.subheader("เปรียบเทียบ Headers (สีเขียว = มีในไฟล์อื่น, สีแดง = ไม่มีในไฟล์อื่น)")
                
                # Helper function to check if header exists in other files
                def get_header_match_status(header, all_file_headers, current_filename):
                    other_files = [f for f in all_file_headers.keys() if f != current_filename]
                    if not other_files:
                        return "single_file"
                    exists_in_others = any(header in all_file_headers[f] for f in other_files)
                    return "match" if exists_in_others else "no_match"
                
                for filename, headers in file_headers.items():
                    with st.expander(f"Headers ของ {filename} ({len(headers)} headers)"):
                        # Display headers with color coding
                        header_html = "<div style='display: flex; flex-wrap: wrap; gap: 5px; margin: 10px 0;'>"
                        
                        for header in headers:
                            match_status = get_header_match_status(header, file_headers, filename)
                            
                            if match_status == "match":
                                css_class = "header-match"
                                icon = ""
                            elif match_status == "no_match":
                                css_class = "header-no-match"
                                icon = ""
                            else:
                                css_class = "header-match"
                                icon = ""
                            
                            header_html += f'<span class="{css_class}">{icon} {header}</span>'
                        
                        header_html += "</div>"
                        st.markdown(header_html, unsafe_allow_html=True)
                        
                        # Show statistics
                        matched_headers = [h for h in headers if get_header_match_status(h, file_headers, filename) == "match"]
                        unmatched_headers = [h for h in headers if get_header_match_status(h, file_headers, filename) == "no_match"]
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            st.success(f"Headers ที่มีในไฟล์อื่น: {len(matched_headers)}")
                        with col2:
                            if unmatched_headers:
                                st.error(f"Headers ที่ไม่มีในไฟล์อื่น: {len(unmatched_headers)}")
                            else:
                                st.success("ทุก Headers มีในไฟล์อื่น")
                
                # Header mapping interface
                st.subheader("🔧 ปรับแต่ง Headers สำหรับการรวมไฟล์")
                
                st.info("""
                **วิธีใช้งาน:**
                1. ดูตัวอย่างข้อมูลแต่ละไฟล์
                2. เลือกว่า Header ไหนจะใช้ หรือลบทิ้ง
                3. จับคู่ Headers ที่มีความหมายเหมือนกัน
                4. **Headers สีแดงคือไม่มีในไฟล์อื่น** - ควรพิจารณาจับคู่หรือลบ
                """)
                
                header_mapping = {}
                excluded_headers = {}
                
                for filename, headers in file_headers.items():
                    st.markdown("---")
                    
                    # File header with match statistics
                    matched_count = len([h for h in headers if get_header_match_status(h, file_headers, filename) == "match"])
                    unmatched_count = len(headers) - matched_count
                    
                    st.markdown(f"### {filename}")
                    
                    if unmatched_count > 0:
                        st.warning(f"มี {unmatched_count} headers ที่ไม่ตรงกับไฟล์อื่น (แสดงเป็นสีแดง)")
                    else:
                        st.success("ทุก headers ตรงกับไฟล์อื่น")
                    
                    # Get sample data
                    sheet_name = selected_sheets.get(filename, st.session_state.merger_processed_data[filename]['sheets'][0])
                    sample_df = st.session_state.merger_processed_data[filename]['data'][sheet_name].head(5)
                    
                    # Show sample data
                    with st.expander("ดูตัวอย่างข้อมูล 5 แถวแรก", expanded=False):
                        st.dataframe(sample_df, use_container_width=True)
                    
                    st.write("**จัดการ Headers:**")
                    
                    file_mapping = {}
                    file_excluded = []
                    
                    for i, header in enumerate(headers):
                        match_status = get_header_match_status(header, file_headers, filename)
                        
                        with st.container():
                            col1, col2, col3 = st.columns([2, 2, 3])
                            
                            with col1:
                                # Show header with color coding
                                if match_status == "match":
                                    st.markdown(f"**`{header}`**")
                                    st.caption("มีในไฟล์อื่น")
                                elif match_status == "no_match":
                                    st.markdown(f"**`{header}`**")
                                    st.caption("ไม่มีในไฟล์อื่น - ควรพิจารณา")
                                else:
                                    st.markdown(f"**`{header}`**")
                                    st.caption("ไฟล์เดียว")
                                
                                # Show sample values
                                if header in sample_df.columns:
                                    sample_values = sample_df[header].dropna().head(3).tolist()
                                    if sample_values:
                                        st.caption(f"ตัวอย่าง: {', '.join(str(v)[:15] for v in sample_values)}")
                            
                            with col2:
                                action = st.selectbox(
                                    "การดำเนินการ:",
                                    ["ใช้งาน", "ลบทิ้ง"],
                                    key=f"merger_action_{filename}_{i}",
                                    index=0,
                                    label_visibility="collapsed"
                                )
                            
                            with col3:
                                if action == "ใช้งาน":
                                    mapping_options = [f"ใช้ชื่อเดิม: {header}"]
                                    
                                    matching_headers = [h for h in all_headers if h != header]
                                    for other_header in sorted(matching_headers):
                                        mapping_options.append(f"จับคู่กับ: {other_header}")
                                    
                                    mapping_options.append("สร้างชื่อใหม่")
                                    
                                    if match_status == "no_match" and len(matching_headers) > 0:
                                        st.info("แนะนำ: header นี้ไม่มีในไฟล์อื่น")
                                    
                                    selected_mapping = st.selectbox(
                                        "เลือกการจับคู่:",
                                        mapping_options,
                                        key=f"merger_map_{filename}_{i}",
                                        index=0,
                                        label_visibility="collapsed"
                                    )
                                    
                                    if selected_mapping.startswith("จับคู่กับ:"):
                                        mapped_header = selected_mapping.replace("จับคู่กับ: ", "")
                                        file_mapping[header] = mapped_header
                                        st.success(f"จับคู่: {header} → {mapped_header}")
                                    elif selected_mapping == "สร้างชื่อใหม่":
                                        custom_header = st.text_input(
                                            "พิมพ์ชื่อใหม่:",
                                            value=header,
                                            key=f"merger_custom_{filename}_{i}",
                                            label_visibility="collapsed"
                                        )
                                        if custom_header and custom_header != header:
                                            file_mapping[header] = custom_header
                                            st.success(f"เปลี่ยนชื่อ: {header} → {custom_header}")
                                    else:
                                        st.info("ใช้ชื่อเดิม")
                                else:
                                    file_excluded.append(header)
                                    st.error("Header นี้จะถูกลบออก")
                    
                    if file_mapping:
                        header_mapping[filename] = file_mapping
                    if file_excluded:
                        excluded_headers[filename] = file_excluded
                
                # Store mappings
                st.session_state.merger_header_mapping = header_mapping
                st.session_state.merger_excluded_headers = excluded_headers
                
            elif len(file_headers) > 1:
                st.success("Headers ทั้งหมดสอดคล้องกัน - พร้อมสำหรับการรวมไฟล์")
                st.session_state.merger_header_mapping = {}
                st.session_state.merger_excluded_headers = {}
            else:
                st.info("มีเพียงไฟล์เดียวที่เลือก - ไม่ต้องการการปรับแต่ง Headers")
                st.session_state.merger_header_mapping = {}
                st.session_state.merger_excluded_headers = {}
        
        # Merge button
        if any(st.session_state.merger_selected_files.values()):
            st.header("⚙️ การรวมไฟล์")
            
            selected_files_list = [f for f, selected in st.session_state.merger_selected_files.items() if selected]
            
            col1, col2 = st.columns(2)
            with col1:
                st.write("**ไฟล์ที่จะรวม:**")
                for f in selected_files_list:
                    st.write(f"• {f}")
            
            if st.button("🚀 เริ่มรวมไฟล์", type="primary", use_container_width=True, key="merge_files_btn"):
                with st.spinner("กำลังรวมไฟล์..."):
                    merged_df = merger.merge_files(
                        st.session_state.merger_processed_data,
                        selected_sheets,
                        st.session_state.merger_selected_files,
                        st.session_state.get('merger_header_mapping', {}),
                        st.session_state.get('merger_excluded_headers', {})
                    )
                    
                    st.session_state.merger_merged_df = merged_df
                    
                    selected_count = sum(st.session_state.merger_selected_files.values())
                    st.success(f"รวมไฟล์สำเร็จ! รวม {selected_count} ไฟล์ ได้รับ {len(merged_df):,} แถว")
        
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
            
            # Download
            st.header("⬇️ ดาวน์โหลด")
            
            filename = f"merged_file_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            csv_data = merged_df.to_csv(index=False)
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.download_button(
                    label="📥 ดาวน์โหลดไฟล์ CSV",
                    data=csv_data,
                    file_name=filename,
                    mime="text/csv",
                    type="primary",
                    use_container_width=True,
                    key="download_merged"
                )
            
            with col2:
                file_size = len(csv_data.encode('utf-8')) / 1024
                st.info(f"ขนาดไฟล์: {file_size:.2f} KB")
    
    else:
        st.info("กรุณาอัปโหลดไฟล์เพื่อเริ่มต้นใช้งาน")
        
        # Feature showcase
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("""
            ### รองรับหลายรูปแบบ
            - ไฟล์ CSV
            - Excel (.xlsx, .xls)
            - หลาย Sheet
            - **เลือกไฟล์ที่ต้องการ**
            """)
        
        with col2:
            st.markdown("""
            ### ตรวจสอบอัตโนมัติ
            - เช็ค Header consistency
            - **แสดงสี Headers ที่ไม่ match**
            - ตัวอย่างข้อมูล
            """)
        
        with col3:
            st.markdown("""
            ### ปรับแต่งได้
            - **เลือก/ไม่เลือกไฟล์**
            - Mapping Headers
            - เลือก/ลบ Headers
            - ดาวน์โหลดผลลัพธ์
            """)

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
