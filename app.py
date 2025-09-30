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

# ===== TAB 3: FILE MERGER WITH DOWNLOAD FORMAT SELECTION =====
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
            st.session_state.merger_processed_data = merger.process_uploaded_files(uploaded_files)
            st.session_state.merger_last_uploaded = uploaded_files
            st.session_state.merger_merged_df = None
            st.session_state.merger_selected_files = {f.name: True for f in uploaded_files}
    
    # Main merger content
    if st.session_state.merger_processed_data:
        # ... (keep all existing code for file selection, header analysis, etc.)
        # ... (ใส่โค้ดส่วนอื่นที่มีอยู่แล้วตามเดิม)
        
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
            
            # ===== DOWNLOAD SECTION WITH FORMAT SELECTION =====
            st.header("⬇️ ดาวน์โหลด")
            
            col1, col2 = st.columns([1, 2])
            
            with col1:
                # Format selection
                download_format = st.radio(
                    "เลือกรูปแบบไฟล์:",
                    options=["CSV", "Excel (XLSX)"],
                    index=0,
                    key="download_format",
                    help="เลือกรูปแบบที่ต้องการดาวน์โหลด"
                )
            
            with col2:
                if download_format == "CSV":
                    # CSV Download
                    filename = f"merged_file_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    csv_data = merged_df.to_csv(index=False, encoding='utf-8-sig')
                    file_size = len(csv_data.encode('utf-8')) / 1024
                    
                    st.info(f"📄 รูปแบบ: CSV | ขนาด: {file_size:.2f} KB")
                    
                    st.download_button(
                        label="📥 ดาวน์โหลดไฟล์ CSV",
                        data=csv_data,
                        file_name=filename,
                        mime="text/csv",
                        type="primary",
                        use_container_width=True,
                        key="download_merged_csv"
                    )
                    
                    st.caption("✓ รองรับภาษาไทย (UTF-8)")
                    st.caption("✓ เปิดได้ด้วย Excel, Google Sheets")
                    st.caption("✓ ขนาดไฟล์เล็ก")
                
                else:  # Excel
                    # Excel Download
                    filename = f"merged_file_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                    
                    # Create Excel file in memory
                    from io import BytesIO
                    
                    output = BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        merged_df.to_excel(writer, index=False, sheet_name='Merged Data')
                        
                        # Get workbook and worksheet
                        workbook = writer.book
                        worksheet = writer.sheets['Merged Data']
                        
                        # Auto-adjust column width
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
                        
                        # Add filters
                        worksheet.auto_filter.ref = worksheet.dimensions
                    
                    excel_data = output.getvalue()
                    file_size = len(excel_data) / 1024
                    
                    st.info(f"📊 รูปแบบ: Excel | ขนาด: {file_size:.2f} KB")
                    
                    st.download_button(
                        label="📥 ดาวน์โหลดไฟล์ Excel",
                        data=excel_data,
                        file_name=filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        type="primary",
                        use_container_width=True,
                        key="download_merged_excel"
                    )
                    
                    st.caption("✓ รองรับภาษาไทย")
                    st.caption("✓ มี Auto-filter")
                    st.caption("✓ ปรับความกว้างคอลัมน์อัตโนมัติ")
            
            # File format comparison
            with st.expander("ℹ️ เปรียบเทียบรูปแบบไฟล์", expanded=False):
                comparison_df = pd.DataFrame({
                    'คุณสมบัติ': [
                        'ขนาดไฟล์',
                        'ความเร็วในการเปิด',
                        'รองรับภาษาไทย',
                        'สูตรและการจัด format',
                        'เปิดด้วยโปรแกรม',
                        'ความเหมาะสม'
                    ],
                    'CSV': [
                        'เล็กกว่า',
                        'เร็วกว่า',
                        'รองรับ (UTF-8)',
                        'ไม่รองรับ',
                        'Excel, Text Editor, Database',
                        'ข้อมูลขนาดใหญ่, นำเข้าระบบ'
                    ],
                    'Excel (XLSX)': [
                        'ใหญ่กว่า',
                        'ช้ากว่า',
                        'รองรับ',
                        'รองรับเต็มรูปแบบ',
                        'Excel, Google Sheets',
                        'รายงาน, การนำเสนอ, วิเคราะห์'
                    ]
                })
                
                st.dataframe(comparison_df, use_container_width=True, hide_index=True)
            
            # Data distribution chart
            if '_source_file' in merged_df.columns:
                st.subheader("📈 การกระจายข้อมูลตามไฟล์ต้นทาง")
                
                import plotly.express as px
                
                source_counts = merged_df['_source_file'].value_counts()
                
                fig = px.pie(
                    values=source_counts.values,
                    names=source_counts.index,
                    title="สัดส่วนข้อมูลจากแต่ละไฟล์"
                )
                fig.update_traces(
                    textposition='inside',
                    textinfo='percent+label'
                )
                fig.update_layout(
                    showlegend=True,
                    height=400
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Source file statistics table
                st.subheader("📋 สถิติรายละเอียดตามไฟล์")
                stats_df = pd.DataFrame({
                    'ไฟล์': source_counts.index,
                    'จำนวนแถว': source_counts.values,
                    'สัดส่วน (%)': (source_counts.values / len(merged_df) * 100).round(2)
                })
                st.dataframe(stats_df, use_container_width=True, hide_index=True)
    
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
            - เลือกไฟล์ที่ต้องการ
            """)
        
        with col2:
            st.markdown("""
            ### ตรวจสอบอัตโนมัติ
            - เช็ค Header consistency
            - แสดงสี Headers ที่ไม่ match
            - ตัวอย่างข้อมูล
            """)
        
        with col3:
            st.markdown("""
            ### ดาวน์โหลดได้ 2 รูปแบบ
            - **CSV** - เล็ก เร็ว
            - **Excel** - มี format
            - รองรับภาษาไทย
            - Auto-adjust columns
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
