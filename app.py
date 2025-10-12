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
from io import BytesIO  # ✅ เพิ่มเพื่อใช้รีเซ็ต pointer และอ่านเป็น bytes

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

# ---- session defaults (safe) ----
for k, v in {
    'favorites': [],
    'loaded_procedures': [],
    'last_proc_filter': "",
    'last_proc_exact': False,
    'execution_history': [],
    # event flags
    'PROC_RUN_EVENT': None,       # {'name': str, 'params': list|None}
    'PROC_ADD_FAV_EVENT': None,   # {'name': str}
}.items():
    st.session_state.setdefault(k, v)

# ===== CACHING FUNCTIONS =====
@st.cache_data(ttl=300)
def get_cached_tables_info():
    try:
        db_manager = DatabaseManager()
        return db_manager.get_tables_with_info()
    except Exception as e:
        st.error(f"Error getting tables info: {e}")
        return []

@st.cache_data(ttl=300)
def get_cached_table_columns(table_name):
    try:
        db_manager = DatabaseManager()
        return db_manager.get_table_columns(table_name)
    except Exception as e:
        return []

@st.cache_data(ttl=60)
def get_cached_table_preview(table_name, limit=5):
    try:
        if 'db_manager' in st.session_state:
            return st.session_state.db_manager.get_table_preview(table_name, limit)
    except Exception as e:
        st.error(f"Preview error: {str(e)}")
    # fallback
    try:
        db_manager = DatabaseManager()
        return db_manager.get_table_preview(table_name, limit)
    except Exception:
        return pd.DataFrame()

# ---------- Stored procedures (filter/load) ----------
@st.cache_data(ttl=300)
def get_stored_procedures(name_filter: str = "", limit: int = 50):
    """Get list of stored procedures from database with optional wildcard filter and limit"""
    try:
        db_manager = DatabaseManager()
        base_sql = """
        SELECT 
            ROUTINE_NAME,
            ROUTINE_TYPE,
            DTD_IDENTIFIER as RETURNS,
            CREATED,
            LAST_ALTERED,
            ROUTINE_COMMENT
        FROM INFORMATION_SCHEMA.ROUTINES
        WHERE ROUTINE_SCHEMA = %s
        """
        params = [db_manager.connection_config['database']]
        base_sql += " AND ROUTINE_NAME LIKE %s"
        params.append(name_filter if name_filter else "%")
        base_sql += " ORDER BY ROUTINE_NAME LIMIT %s"
        params.append(limit)
        df = db_manager.execute_query(base_sql, tuple(params))
        return df.to_dict('records') if (df is not None and not df.empty) else []
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
        return df.to_dict('records') if (df is not None and not df.empty) else []
    except Exception as e:
        st.error(f"Error getting parameters: {e}")
        return []

def execute_procedure(procedure_name, parameters=None):
    """Original execute (kept, not used by the new UI flow but preserved to not break other logic)"""
    conn = None
    cursor = None
    results = []
    try:
        db_manager = DatabaseManager()
        conn = mysql.connector.connect(
            host=db_manager.connection_config['host'],
            port=db_manager.connection_config.get('port', 3306),
            database=db_manager.connection_config['database'],
            user=db_manager.connection_config['user'],
            password=db_manager.connection_config['password'],
            charset=db_manager.connection_config.get('charset', 'utf8mb4'),
            autocommit=False,
            connection_timeout=10,
        )
        cursor = conn.cursor(buffered=True, dictionary=True)
        args = parameters if parameters is not None else []
        cursor.callproc(procedure_name, args)
        for rs in cursor.stored_results():
            rows = rs.fetchall()
            results.append(rows)
        try:
            while cursor.nextset():
                pass
        except mysql.connector.Error:
            pass
        conn.commit()
        return {'success': True,'message': f'Procedure {procedure_name} executed successfully','results': results}
    except mysql.connector.Error as e:
        if conn:
            try: conn.rollback()
            except Exception: pass
        return {
            'success': False,
            'error': str(e),
            'error_details': {
                'errno': getattr(e, 'errno', None),
                'sqlstate': getattr(e, 'sqlstate', None),
                'msg': getattr(e, 'msg', str(e))
            }
        }
    except Exception as e:
        if conn:
            try: conn.rollback()
            except Exception: pass
        return {'success': False, 'error': str(e)}
    finally:
        try:
            if cursor: cursor.close()
        except Exception: pass
        try:
            if conn: conn.close()
        except Exception: pass

# ---------- NEW: execute with visible progress ----------
def execute_procedure_with_progress(procedure_name, parameters=None, fetch_chunk=1000):
    conn = None
    cursor = None
    results = []
    progress = st.progress(0)
    status = st.empty()
    try:
        status.info("Connecting to database...")
        progress.progress(5)
        db_manager = DatabaseManager()
        conn = mysql.connector.connect(
            host=db_manager.connection_config['host'],
            port=db_manager.connection_config.get('port', 3306),
            database=db_manager.connection_config['database'],
            user=db_manager.connection_config['user'],
            password=db_manager.connection_config['password'],
            charset=db_manager.connection_config.get('charset', 'utf8mb4'),
            autocommit=False,
            connection_timeout=10,
        )
        cursor = conn.cursor(buffered=True, dictionary=True)
        status.info(f"Calling procedure: {procedure_name}")
        progress.progress(15)
        args = parameters if parameters is not None else []
        cursor.callproc(procedure_name, args)
        stage_end  = 90
        # fetch result sets
        for rs in cursor.stored_results():
            status.info("Fetching result set...")
            rows_acc = []
            while True:
                rows = rs.fetchmany(size=fetch_chunk)
                if not rows:
                    break
                rows_acc.extend(rows)
                progress.progress(min(stage_end, progress_value_bump(step=5)))
            results.append(rows_acc)
        # clear pending sets (ok packets)
        try:
            while cursor.nextset():
                pass
        except mysql.connector.Error:
            pass
        status.info("Committing transaction...")
        progress.progress(95)
        conn.commit()
        progress.progress(100)
        status.success(f"Procedure {procedure_name} executed successfully")
        return {'success': True,'message': f'Procedure {procedure_name} executed successfully','results': results}
    except mysql.connector.Error as e:
        if conn:
            try: conn.rollback()
            except Exception: pass
        status.error(f"MySQL error: {getattr(e, 'msg', str(e))}")
        return {
            'success': False,
            'error': str(e),
            'error_details': {
                'errno': getattr(e, 'errno', None),
                'sqlstate': getattr(e, 'sqlstate', None),
                'msg': getattr(e, 'msg', str(e))
            }
        }
    except Exception as e:
        if conn:
            try: conn.rollback()
            except Exception: pass
        status.error(f"Error: {str(e)}")
        return {'success': False, 'error': str(e)}
    finally:
        try:
            if cursor: cursor.close()
        except Exception: pass
        try:
            if conn: conn.close()
        except Exception: pass

def progress_value_bump(step=5):
    """Track progress position in session since Streamlit bar doesn't expose current value."""
    if 'proc_progress_value' not in st.session_state:
        st.session_state['proc_progress_value'] = 20
    st.session_state['proc_progress_value'] = min(100, st.session_state['proc_progress_value'] + step)
    return st.session_state['proc_progress_value']

# ---------- NEW: common renderer for execution result ----------
def render_exec_result(proc_name: str, result: dict):
    if result.get('success'):
        st.success(f"✅ {result['message']}")
        if result.get('results'):
            for idx, res in enumerate(result['results']):
                st.write(f"**Result Set {idx + 1}:**")
                df_result = pd.DataFrame(res)
                st.dataframe(df_result, use_container_width=True)
                csv_data = df_result.to_csv(index=False)
                st.download_button(
                    "📥 Download CSV",
                    csv_data,
                    f"{proc_name}_result_{idx+1}.csv",
                    "text/csv",
                    key=f"download_csv_{proc_name}_{idx}"
                )
        if result.get('rows_affected'):
            st.info(f"Rows affected: {result.get('rows_affected')}")
        if result.get('warnings'):
            with st.expander("⚠️ Warnings"):
                for warning in result['warnings']:
                    st.warning(f"{warning[0]}: {warning[2]}")
        st.session_state.execution_history.append({'procedure': proc_name,'status': 'success','timestamp': datetime.now()})
    else:
        st.error("❌ Execution failed")
        if result.get('error_details'):
            details = result['error_details']
            st.error(f"**Error:** {details.get('msg')}")
            if details.get('errno'):
                st.caption(f"Error Code: {details['errno']}")
            if details.get('sqlstate'):
                st.caption(f"SQL State: {details['sqlstate']}")
        else:
            st.error(result.get('error', 'Unknown error'))
        st.session_state.execution_history.append({'procedure': proc_name,'status': 'failed','timestamp': datetime.now()})

# ---------- NEW: favorites helpers ----------
def add_favorite(name: str):
    favs = set(st.session_state.get('favorites', []))
    favs.add(name)
    st.session_state['favorites'] = list(favs)

def remove_favorite(name: str):
    favs = set(st.session_state.get('favorites', []))
    favs.discard(name)
    st.session_state['favorites'] = list(favs)

def render_favorites_block():
    st.subheader("⭐ Favorites")
    favs = st.session_state.get('favorites', [])
    if not favs:
        st.caption("ยังไม่มีรายการโปรด")
        return
    for name in favs:
        c1, c2, c3 = st.columns([2,1,1])
        with c1:
            st.write(name)
        with c2:
            if st.button("▶️ Run", key=f"fav_run_{name}"):
                st.session_state['PROC_RUN_EVENT'] = {'name': name, 'params': None}
        with c3:
            if st.button("🗑️ Remove", key=f"fav_del_{name}"):
                remove_favorite(name)
                st.rerun()

# ===== CSS STYLING =====
st.markdown("""
<style>
    .main-header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 1rem; border-radius: 8px; margin-bottom: 1rem; text-align: center; color: white; }
    .metric-card { background: #f0f2f6; padding: 1rem; border-radius: 8px; text-align: center; margin: 0.5rem 0; border: 1px solid #e0e0e0; }
    .status-success { background: #d4edda; padding: 0.5rem; border-radius: 4px; color: #155724; margin: 0.5rem 0; }
    .status-error { background: #f8d7da; padding: 0.5rem; border-radius: 4px; color: #721c24; margin: 0.5rem 0; }
    .file-info { background: white; padding: 1rem; border-radius: 8px; border-left: 4px solid #9CAF88; box-shadow: 0 2px 8px rgba(139, 69, 19, 0.1); margin: 1rem 0; }
    .header-match { background: #d4edda; color: #155724; padding: 0.25rem 0.5rem; border-radius: 4px; margin: 0.1rem; display: inline-block; font-size: 0.85rem; font-weight: bold; }
    .header-no-match { background: #f8d7da; color: #721c24; padding: 0.25rem 0.5rem; border-radius: 4px; margin: 0.1rem; display: inline-block; font-size: 0.85rem; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# ===== Utility: Safe CSV reader (ใหม่) =====
def read_csv_safely(file_or_bytes, *, sep=None):
    """อ่าน CSV โดยลองหลาย encoding ที่พบบ่อยในไฟล์ไทย และกันบรรทัด/อักขระเสียโดยไม่ให้ล้ม"""
    if hasattr(file_or_bytes, "getvalue"):
        raw = file_or_bytes.getvalue()
    elif hasattr(file_or_bytes, "read"):
        raw = file_or_bytes.read()
    else:
        raw = file_or_bytes  # assume bytes

    encodings_try = ['utf-8-sig', 'cp874', 'tis-620', 'iso-8859-11', 'utf-16', 'utf-16le', 'utf-16be', 'latin1']
    last_err = None
    for enc in encodings_try:
        try:
            buf = BytesIO(raw)
            df = pd.read_csv(
                buf,
                encoding=enc,
                encoding_errors='replace',
                engine='python',
                sep=sep,                 # sniff ถ้า None
                on_bad_lines='skip',     # ข้ามบรรทัดพิกล
                dtype=str                # กัน type เพี้ยน
            )
            df.attrs['__encoding__'] = enc
            return df
        except Exception as e:
            last_err = e
            continue
    raise last_err or Exception("Cannot decode CSV with known Thai encodings")

# ===== FILE MERGER CLASS =====
class FileMerger:
    def __init__(self):
        self.uploaded_files = []
        self.processed_data = {}
        self.merged_df = None
        self.header_mapping = {}

    def process_uploaded_files(self, files):
        processed = {}
        for file in files:
            file_info = {'name': file.name, 'size': file.size, 'type': self.get_file_type(file.name)}
            try:
                if file_info['type'] == 'csv':
                    # ✅ ใช้ตัวอ่านแบบปลอดภัย
                    df = read_csv_safely(file)
                    file_info['succeeded_encoding'] = getattr(df.attrs, '__encoding__', 'unknown')
                    file_info['sheets'] = ['Sheet1']
                    file_info['data'] = {'Sheet1': df}

                elif file_info['type'] == 'excel':
                    # Excel ปกติไม่ติด encoding
                    excel_file = pd.ExcelFile(file)
                    file_info['sheets'] = excel_file.sheet_names
                    file_info['data'] = {
                        sheet: pd.read_excel(excel_file, sheet_name=sheet)
                        for sheet in excel_file.sheet_names
                    }

                processed[file.name] = file_info

            except Exception as e:
                st.error(f"Error processing {file.name}: {str(e)}")
        return processed

    def get_file_type(self, filename):
        if filename.lower().endswith('.csv'): return 'csv'
        elif filename.lower().endswith(('.xlsx', '.xls')): return 'excel'
        return 'unknown'

    def analyze_headers(self, processed_data, selected_sheets, selected_files):
        all_headers = set(); file_headers = {}; has_mismatch = False
        for filename, file_info in processed_data.items():
            if selected_files.get(filename, True):
                sheet_name = selected_sheets.get(filename, file_info['sheets'][0])
                if sheet_name in file_info['data']:
                    df = file_info['data'][sheet_name]
                    headers = list(df.columns)
                    file_headers[filename] = headers
                    all_headers.update(headers)
        if len(file_headers) > 1:
            reference_headers = set(next(iter(file_headers.values())))
            for _, headers in file_headers.items():
                if set(headers) != reference_headers:
                    has_mismatch = True; break
        return list(all_headers), has_mismatch, file_headers

    def merge_files(self, processed_data, selected_sheets, selected_files, header_mapping=None, excluded_headers=None):
        merged_dfs = []
        for filename, file_info in processed_data.items():
            if selected_files.get(filename, True):
                sheet_name = selected_sheets.get(filename, file_info['sheets'][0])
                if sheet_name in file_info['data']:
                    df = file_info['data'][sheet_name].copy()
                    if excluded_headers and filename in excluded_headers:
                        columns_to_keep = [c for c in df.columns if c not in excluded_headers[filename]]
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
            st.cache_data.clear(); st.rerun()

    st.divider()
    st.header("📁 File Import to Database")

    if 'db_manager' not in st.session_state:
        st.session_state.db_manager = DatabaseManager()
    if 'file_processor' not in st.session_state:
        st.session_state.file_processor = FileProcessor()

    try:
        tables_info = get_cached_tables_info()
        tables = [table['TABLE_NAME'] for table in tables_info] if tables_info else []
    except Exception as e:
        st.warning(f"Could not get table info: {e}"); tables = []; tables_info = []

    selected_table = st.selectbox("🎯 Select Target Table", options=[""] + tables, help="Choose the table where you want to import your data")
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
        uploaded_file = st.file_uploader("Choose a file to import", type=['csv', 'xlsx', 'xls'], help="Max size: 200MB", key="import_uploader")
        if uploaded_file:
            st.markdown(f"""
            <div class="file-info">
                <h4>📄 {uploaded_file.name}</h4>
                <p><strong>Size:</strong> {uploaded_file.size / 1024:.2f} KB</p>
                <p><strong>Type:</strong> {uploaded_file.type}</p>
            </div>
            """, unsafe_allow_html=True)
            try:
                with st.spinner("Reading file..."):
                    if uploaded_file.name.endswith('.csv'):
                        # ✅ ใช้อ่านแบบปลอดภัยเพื่อกัน error utf-8
                        df = read_csv_safely(uploaded_file)
                    else:
                        df = pd.read_excel(uploaded_file)

                st.success(f"✅ File loaded: {len(df)} rows, {len(df.columns)} columns")
                st.caption(f"Encoding: {getattr(df.attrs, '__encoding__', 'auto') if uploaded_file.name.endswith('.csv') else 'n/a'}")

                st.subheader("📋 Data Preview")
                st.dataframe(df.head(10), use_container_width=True)

                st.subheader("🔗 Column Mapping")
                table_columns = get_cached_table_columns(selected_table)
                if not table_columns:
                    st.error("Cannot get table columns"); return
                db_column_names = [col['COLUMN_NAME'] for col in table_columns]
                file_columns = list(df.columns)
                st.info(f"**File Columns:** {len(file_columns)} | **Table Columns:** {len(db_column_names)}")

                column_mapping = {}
                col1, col2 = st.columns(2)
                with col1: st.write("**File Column**")
                with col2: st.write("**→ Database Column**")
                for file_col in file_columns:
                    c1, c2 = st.columns(2)
                    with c1: st.text(file_col)
                    with c2:
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

                if column_mapping:
                    st.success(f"✅ Mapped {len(column_mapping)} columns")
                    with st.expander("View Mapping Details"):
                        for file_col, db_col in column_mapping.items():
                            st.write(f"**{file_col}** → **{db_col}**")
                else:
                    st.warning("⚠️ No columns mapped")

                st.divider()
                c1, c2, _ = st.columns([1,1,2])
                with c1:
                    if st.button("🚀 Import Data", type="primary", disabled=len(column_mapping)==0):
                        if not column_mapping:
                            st.error("Please map at least one column")
                        else:
                            fresh_db = DatabaseManager()
                            with st.spinner(f"Importing {len(df)} rows..."):
                                result = fresh_db.import_data(selected_table, df, column_mapping)
                            fresh_db.close_connection()
                            if result.get('success'):
                                st.success(f"✅ {result['message']}")
                                st.balloons()
                                st.cache_data.clear()
                                st.metric("Rows Imported", result.get('rows_affected', 0))
                            else:
                                st.error(f"❌ Import failed: {result.get('error')}")
                with c2:
                    if st.button("🔄 Reset", type="secondary"):
                        st.rerun()
            except Exception as e:
                st.error(f"❌ Error processing file: {str(e)}")
                st.exception(e)

# ===== TAB 2: RUN PROCEDURES (with event flags) =====
def render_procedures_tab():
    st.header("⚙️ Database Procedures & Updates")
    enabled = st.toggle("Enable this tab (load from DB)", value=False, help="Turn on only when you want to work with procedures")
    if not enabled:
        st.info("This tab is idle. Turn on the toggle to load procedures.")
        return

    if 'db_manager' not in st.session_state:
        st.session_state.db_manager = DatabaseManager()

    # Favorites on top
    render_favorites_block()
    st.divider()

    st.subheader("🔎 Search / Load Procedures (Lazy-load)")
    c1, c2, c3, c4, c5 = st.columns([2,1,1,1,1])
    with c1:
        name_filter = st.text_input(
            "Procedure name (supports % wildcard)",
            value=st.session_state.get('last_proc_filter', ""),
            placeholder="เช่น %R06% หรือระบุชื่อเต็ม"
        )
    with c2:
        limit = st.number_input("Limit", min_value=1, max_value=500, value=50, step=10, help="จำกัดจำนวนผลลัพธ์")
    with c3:
        exact_only = st.checkbox("Exact name", value=st.session_state.get('last_proc_exact', False), help="ติ๊กหากต้องการชื่อตรง")
    with c4:
        do_load = st.button("📥 Load", type="primary", use_container_width=True)
    with c5:
        do_clear_loaded = st.button("🧹 Clear Loaded", use_container_width=True)

    if do_clear_loaded:
        st.session_state.loaded_procedures = []
        st.session_state['last_proc_filter'] = ""
        st.session_state['last_proc_exact'] = False
        st.toast("Cleared loaded procedures")

    if do_load:
        pattern = name_filter or "%"
        if exact_only and name_filter:
            pattern = name_filter
        procs = get_stored_procedures(pattern, limit)
        st.session_state.loaded_procedures = procs
        st.session_state['last_proc_filter'] = name_filter
        st.session_state['last_proc_exact'] = exact_only
        if procs:
            st.success(f"Loaded {len(procs)} procedure(s)")
        else:
            st.warning("No procedures matched your filter.")

    # Always use the list persisted in session
    procedures = st.session_state.loaded_procedures

    # Quick Run
    with st.expander("⚡ Quick Run (run by name directly)"):
        quick_name = st.text_input("Procedure name to run (exact)", key="quick_run_name")
        if st.button("▶️ Run Now", key="quick_run_btn", type="primary"):
            if quick_name.strip():
                st.session_state['PROC_RUN_EVENT'] = {'name': quick_name.strip(), 'params': None}
            else:
                st.error("กรุณาระบุชื่อ procedure ให้ถูกต้อง")

    st.divider()
    col1, col2 = st.columns([2, 1])

    # List & buttons
    with col1:
        st.subheader("🔧 Stored Procedures")
        if procedures:
            st.info(f"Found {len(procedures)} stored procedures (loaded)")
            search_query = st.text_input("Filter in results (client-side)", placeholder="พิมพ์คัดกรองผลที่โหลดมา", key="search_proc_client")
            filtered_procedures = [p for p in procedures if (search_query.lower() in p['ROUTINE_NAME'].lower())] if search_query else procedures

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
                                param_name = param['PARAMETER_NAME']; param_type = param['DATA_TYPE']
                                if param_type in ['int', 'bigint', 'smallint']:
                                    val = st.number_input(f"{param_name} ({param_type})", value=0, key=f"param_{proc['ROUTINE_NAME']}_{param_name}")
                                elif param_type in ['decimal', 'float', 'double']:
                                    val = st.number_input(f"{param_name} ({param_type})", value=0.0, format="%.2f", key=f"param_{proc['ROUTINE_NAME']}_{param_name}")
                                elif param_type in ['date', 'datetime', 'timestamp']:
                                    v = st.date_input(f"{param_name} ({param_type})", key=f"param_{proc['ROUTINE_NAME']}_{param_name}")
                                    val = v.strftime("%Y-%m-%d")
                                else:
                                    val = st.text_input(f"{param_name} ({param_type})", key=f"param_{proc['ROUTINE_NAME']}_{param_name}")
                                param_values.append(val)
                        else:
                            st.info("No parameters required")
                            param_values = None

                    st.divider()
                    col_btns = st.columns([1,1,1])
                    # Set events instead of executing inside loop
                    with col_btns[0]:
                        if st.button("▶️ Execute", key=f"exec_{proc['ROUTINE_NAME']}", type="primary", use_container_width=True):
                            st.session_state['PROC_RUN_EVENT'] = {'name': proc['ROUTINE_NAME'], 'params': (param_values if params else None)}
                    with col_btns[1]:
                        if st.button("⭐ Add to Favorites", key=f"fav_{proc['ROUTINE_NAME']}"):
                            st.session_state['PROC_ADD_FAV_EVENT'] = {'name': proc['ROUTINE_NAME']}
                    with col_btns[2]:
                        if st.button("🔄 Refresh Params", key=f"ref_params_{proc['ROUTINE_NAME']}"):
                            st.rerun()
        else:
            st.warning("⚠️ No procedures loaded. ใส่ชื่อแล้วกด Load ก่อน")

        # ===== Handle events after list render =====
        event_run = st.session_state.get('PROC_RUN_EVENT')
        if event_run:
            st.session_state['proc_progress_value'] = 20
            result = execute_procedure_with_progress(event_run['name'], event_run.get('params'))
            render_exec_result(event_run['name'], result)
            st.session_state['PROC_RUN_EVENT'] = None

        event_fav = st.session_state.get('PROC_ADD_FAV_EVENT')
        if event_fav:
            add_favorite(event_fav['name'])
            st.toast(f"Added {event_fav['name']} to Favorites")
            st.session_state['PROC_ADD_FAV_EVENT'] = None

    with col2:
        st.subheader("📊 Quick Stats")
        if procedures:
            st.metric("Total Procedures (loaded)", len(procedures))
        if st.session_state.execution_history:
            success_count = sum(1 for h in st.session_state.execution_history if h['status'] == 'success')
            failed_count = len(st.session_state.execution_history) - success_count
            st.metric("Executions", len(st.session_state.execution_history))
            c_s, c_f = st.columns(2)
            with c_s: st.metric("✅ Success", success_count)
            with c_f: st.metric("❌ Failed", failed_count)
        st.divider()
        if st.button("🗑️ Clear History", use_container_width=True):
            st.session_state.execution_history = []; st.rerun()
        if st.button("🔄 Clear Cache (procedures)", use_container_width=True):
            get_stored_procedures.clear()
            st.session_state.loaded_procedures = []
            st.toast("Cleared cached procedures & session list")

# ===== TAB 3: FILE MERGER =====
def render_merger_tab():
    st.header("📁 File Merger")
    st.write("รวมไฟล์ CSV และ Excel หลายไฟล์เข้าด้วยกัน")
    if 'merger' not in st.session_state:
        st.session_state.merger = FileMerger()
    if 'merger_processed_data' not in st.session_state:
        st.session_state.merger_processed_data = {}
    if 'merger_merged_df' not in st.session_state:
        st.session_state.merger_merged_df = None
    if 'merger_selected_files' not in st.session_state:
        st.session_state.merger_selected_files = {}
    merger = st.session_state.merger

    st.subheader("📤 อัปโหลดไฟล์")
    uploaded_files = st.file_uploader("เลือกไฟล์ CSV หรือ Excel", type=['csv', 'xlsx', 'xls'], accept_multiple_files=True, help="รองรับไฟล์ CSV และ Excel หลายไฟล์", key="merger_uploader")

    if uploaded_files:
        if len(uploaded_files) != len(st.session_state.get('merger_last_uploaded', [])):
            with st.spinner("กำลังประมวลผลไฟล์..."):
                st.session_state.merger_processed_data = merger.process_uploaded_files(uploaded_files)
                st.session_state.merger_last_uploaded = uploaded_files
                st.session_state.merger_merged_df = None
                st.session_state.merger_selected_files = {f.name: True for f in uploaded_files}

    if st.session_state.merger_processed_data:
        if len(st.session_state.merger_processed_data) > 1:
            st.subheader("🎯 เลือกไฟล์สำหรับการรวม")
            cols = st.columns(min(len(st.session_state.merger_processed_data), 3))
            for i, (filename, file_info) in enumerate(st.session_state.merger_processed_data.items()):
                with cols[i % 3]:
                    selected = st.checkbox(filename, value=st.session_state.merger_selected_files.get(filename, True), key=f"merger_select_{filename}", help=f"ขนาด: {file_info['size']/1024:.1f} KB")
                    st.session_state.merger_selected_files[filename] = selected
            selected_count = sum(st.session_state.merger_selected_files.values())
            if selected_count == 0:
                st.error("⚠️ กรุณาเลือกไฟล์อย่างน้อย 1 ไฟล์"); return
        else:
            filename = list(st.session_state.merger_processed_data.keys())[0]
            st.session_state.merger_selected_files = {filename: True}

        st.subheader("📋 ไฟล์ที่อัปโหลด")
        col1, col2 = st.columns([2, 1])
        with col1:
            selected_sheets = {}
            for filename, file_info in st.session_state.merger_processed_data.items():
                is_selected = st.session_state.merger_selected_files.get(filename, True)
                with st.expander(f"{'✅' if is_selected else '❌'} {filename}", expanded=is_selected):
                    col_info, col_sheet = st.columns([2, 1])
                    with col_info:
                        st.markdown(f"**ขนาด:** {file_info['size']/1024:.2f} KB  \n**ประเภท:** {file_info['type'].upper()}  \n**จำนวน Sheets:** {len(file_info['sheets'])}")
                        if 'succeeded_encoding' in file_info:
                            st.caption(f"Encoding: {file_info.get('succeeded_encoding','auto')}")
                    with col_sheet:
                        if len(file_info['sheets']) > 1:
                            selected_sheet = st.selectbox("เลือก Sheet:", file_info['sheets'], key=f"merger_sheet_{filename}", disabled=not is_selected)
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
            selected_files_data = {k: v for k, v in st.session_state.merger_processed_data.items() if st.session_state.merger_selected_files.get(k, True)}
            total_files = len(selected_files_data)
            total_records = sum([
                len(file_info['data'][selected_sheets.get(filename, file_info['sheets'][0])]) 
                for filename, file_info in selected_files_data.items()
                if selected_sheets.get(filename, file_info['sheets'][0]) in file_info['data']
            ]) if selected_files_data else 0
            st.markdown(f"""<div class="metric-card"><h3>📊 สถิติ</h3><p><strong>ไฟล์ที่เลือก:</strong> {total_files}</p><p><strong>จำนวนแถวรวม:</strong> {total_records:,}</p></div>""", unsafe_allow_html=True)

        st.header("🔍 การวิเคราะห์ Headers")
        all_headers, has_mismatch, file_headers = merger.analyze_headers(st.session_state.merger_processed_data, selected_sheets, st.session_state.merger_selected_files)
        if has_mismatch and len(file_headers) > 1:
            st.warning("⚠️ พบความไม่สอดคล้องของ Headers")
            for filename, headers in file_headers.items():
                with st.expander(f"Headers ของ {filename}"):
                    st.write(f"**จำนวน:** {len(headers)} headers")
                    st.write(", ".join(headers))
            st.info("💡 คุณสามารถรวมไฟล์ได้ทันที Headers ที่ไม่ตรงกันจะเป็นค่าว่าง")
        elif len(file_headers) > 1:
            st.success("✅ Headers ทั้งหมดสอดคล้องกัน")

        st.header("⚙️ การรวมไฟล์")
        if st.button("🚀 เริ่มรวมไฟล์", type="primary", use_container_width=True, key="merge_files_btn"):
            with st.spinner("กำลังรวมไฟล์..."):
                merged_df = merger.merge_files(st.session_state.merger_processed_data, selected_sheets, st.session_state.merger_selected_files)
                st.session_state.merger_merged_df = merged_df
                st.success(f"✅ รวมไฟล์สำเร็จ! {len(merged_df):,} แถว")

        if st.session_state.merger_merged_df is not None:
            st.header("📊 ผลลัพธ์การรวมไฟล์")
            merged_df = st.session_state.merger_merged_df
            c1, c2, c3 = st.columns(3)
            with c1: st.metric("จำนวนแถวรวม", f"{len(merged_df):,}")
            with c2: st.metric("จำนวนคอลัมน์", len(merged_df.columns))
            with c3: st.metric("ไฟล์ที่รวม", sum(st.session_state.merger_selected_files.values()))
            st.subheader("ตัวอย่างข้อมูล")
            st.dataframe(merged_df.head(100), use_container_width=True)

            st.header("⬇️ ดาวน์โหลด")
            d1, d2 = st.columns([1,2])
            with d1:
                download_format = st.radio("เลือกรูปแบบไฟล์:", options=["CSV", "Excel (XLSX)"], index=0, key="download_format")
            with d2:
                if download_format == "CSV":
                    filename = f"merged_file_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    csv_data = merged_df.to_csv(index=False, encoding='utf-8-sig')
                    file_size = len(csv_data.encode('utf-8')) / 1024
                    st.info(f"📄 CSV | ขนาด: {file_size:.2f} KB")
                    st.download_button(label="📥 ดาวน์โหลดไฟล์ CSV", data=csv_data, file_name=filename, mime="text/csv", type="primary", use_container_width=True, key="download_merged_csv")
                else:
                    filename = f"merged_file_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                    output = BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        merged_df.to_excel(writer, index=False, sheet_name='Merged Data')
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
                    st.download_button(label="📥 ดาวน์โหลดไฟล์ Excel", data=excel_data, file_name=filename, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary", use_container_width=True, key="download_merged_excel")
    else:
        st.info("👆 กรุณาอัปโหลดไฟล์เพื่อเริ่มต้นใช้งาน")

# ===== MAIN APPLICATION =====
def main():
    try:
        st.markdown("""
        <div class="main-header">
            <h1>🚀 Data Management Hub</h1>
            <p>Complete data management system with import, procedures, and file merger</p>
        </div>
        """, unsafe_allow_html=True)

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

        with st.sidebar:
            st.header("⚙️ Configuration")
            if 'connection_status' not in st.session_state:
                try:
                    st.session_state.connection_status = st.session_state.db_manager.test_connection()
                except Exception:
                    st.session_state.connection_status = False

            if st.session_state.connection_status:
                st.markdown('<div class="status-success">✅ Database Connected</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="status-error">❌ Database Connection Failed</div>', unsafe_allow_html=True)

            if st.button("🔄 Refresh", key="refresh_sidebar"):
                st.cache_data.clear(); st.rerun()

            try:
                tables_info = get_cached_tables_info()
                tables = [table['TABLE_NAME'] for table in tables_info] if tables_info else []
            except Exception:
                tables = []; tables_info = []

            st.write(f"📊 Available Tables: {len(tables)}")

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
