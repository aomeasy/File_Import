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
import chardet 


try:
    from ocr_module import EnhancedThaiDocumentOCR
    OCR_AVAILABLE = True
except Exception as e:
    OCR_AVAILABLE = False
    st.warning(f"⚠️ OCR module could not be loaded: {e}")

                
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
    page_title="Database Management Hub",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)
 

st.markdown("""
<style>
/* ===== Global Styling ===== */
html, body, [class*="stAppViewContainer"] {
    font-family: 'Sarabun', sans-serif !important;
    color: #222;
}
[data-testid="stSidebar"] {
    background: linear-gradient(180deg,#f9fafc 0%,#eef1f9 100%);
    border-right: 1px solid #e0e0e0;
    padding: 1.2rem;
}
[data-testid="stSidebar"] h3, [data-testid="stSidebar"] h4 {
    color: #3b3b98;
}
button[kind="primary"] {
    border-radius: 10px !important;
}
.status-success {
    background:#e6f4ea;
    color:#137333;
    padding:6px 10px;
    border-radius:6px;
}
.status-error {
    background:#fce8e6;
    color:#c5221f;
    padding:6px 10px;
    border-radius:6px;
}
</style>
""", unsafe_allow_html=True)

# ---- session defaults (safe) ----
for k, v in {
    'favorites': [],
    'loaded_procedures': [],
    'last_proc_filter': "",
    'last_proc_exact': False,
    'execution_history': [],
    'PROC_RUN_EVENT': None,
    'PROC_ADD_FAV_EVENT': None,
    # ✅ เพิ่มใหม่
    'AI_RUN_TRIGGERED': False,
    'AI_PROC_NAME': None,
    'AI_CONFIDENCE': 0.0,
    'AI_USERNAME': None,
    'AI_SOURCE_TABLE': None,
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
        # 🔹 Smart search: auto add wildcard if user doesn't type it
        if name_filter and '%' not in name_filter:
            name_filter = f"%{name_filter}%"
        base_sql += " AND ROUTINE_NAME LIKE %s"
        params.append(name_filter if name_filter else "%")
        base_sql += " ORDER BY ROUTINE_NAME LIMIT %s"
        params.append(limit)
        df = db_manager.execute_query(base_sql, tuple(params))
        return df.to_dict('records') if (df is not None and not df.empty) else []
    except Exception as e:
        st.error(f"Error getting procedures: {e}")
        return []

def show_loading_overlay():
    """แสดงหน้าจอครอบทั้งหมดพร้อมข้อความ Loading"""
    st.markdown("""
        <style>
        /* ====== Overlay ปิดการกดทั้งหมด ====== */
        .overlay-blocker {
            position: fixed;
            top: 0; left: 0;
            width: 100%;
            height: 100%;
            background: rgba(255,255,255,0.8);
            z-index: 9999;
            display: flex;
            align-items: center;
            justify-content: center;
            flex-direction: column;
            font-size: 20px;
            color: #333;
        }
        .loader {
            border: 6px solid #f3f3f3;
            border-top: 6px solid #3498db;
            border-radius: 50%;
            width: 50px;
            height: 50px;
            animation: spin 1s linear infinite;
            margin-bottom: 16px;
        }
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        </style>
        <div class="overlay-blocker">
            <div class="loader"></div>
            <div><b>กำลังประมวลผล โปรดรอสักครู่...</b></div>
        </div>
    """, unsafe_allow_html=True)

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
        progress.progress(95)
        conn.commit()
        progress.progress(100)
        status.success(f"Commit done.")
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
        if proc_name == "update_Broadband_daily":
            st.markdown(
                """
                <div style="
                    background-color:#fff8e1;
                    border-left:5px solid #ff9800;
                    padding:10px 15px;
                    border-radius:6px;
                    margin-top:10px;
                    ">
                    🔄 <b>Procedure <code>update_Broadband_daily</code> executed successfully.</b><br>
                    กรุณา <a href="https://lookerstudio.google.com/reporting/1483b6e3-3477-4906-8966-ec276423ec27"
                    target="_blank" style="color:#d32f2f; font-weight:bold; text-decoration:underline;">
                    คลิกที่นี่เพื่อ Refresh Dashboard</a> ใน Looker Studio
                </div>
                """,
                unsafe_allow_html=True
            )



    # ---------- RESULT SET ----------
    if result.get('results'):
        for idx, res in enumerate(result['results'], start=1):
            df_result = pd.DataFrame(res)

            result_title = f"Result Set {idx}"
            base_filename = f"{proc_name}_result_{idx}"

            # หาชื่อหัวข้อจากค่าฟิลด์แรก
            if len(df_result) > 0 and len(df_result.columns) > 0:
                try:
               
                        first_column_name = df_result.columns[0]
        
                        # ใช้แค่ชื่อคอลัมน์ ไม่เอาค่าข้อมูล
                        result_title = first_column_name  # <-- แก้ตรงนี้
                        
                        # สำหรับชื่อไฟล์ยังใช้ชื่อ procedure + index
                        base_filename = f"{proc_name}_result_{idx}"
                except:
                    pass

            st.write(f"**{result_title}**")

            df_display = df_result.copy()
            df_display.index = range(1, len(df_display) + 1)
            st.dataframe(df_display, use_container_width=True)

            unique_id = f"{proc_name}_{idx}_{id(result)}"

            csv_data = df_result.to_csv(index=False).encode('utf-8-sig')

            from io import BytesIO
            excel_buffer = BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                df_result.to_excel(writer, index=False, sheet_name='Result')
            excel_data = excel_buffer.getvalue()

            col1, col2 = st.columns(2)

            with col1:
                st.download_button(
                    label="📄 Download CSV",
                    data=csv_data,
                    file_name=f"{base_filename}.csv",
                    mime="text/csv",
                    key=f"csv_{unique_id}",
                    use_container_width=True
                )

            with col2:
                st.download_button(
                    label="📊 Download Excel",
                    data=excel_data,
                    file_name=f"{base_filename}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"excel_{unique_id}",
                    use_container_width=True
                )

# ---------- DOWNLOAD ALL (Excel multi-sheet) ----------
        if len(result['results']) > 1:
            from io import BytesIO
            all_excel_buffer = BytesIO()
            with pd.ExcelWriter(all_excel_buffer, engine='openpyxl') as writer:
                for s_idx, s_res in enumerate(result['results'], start=1):
                    df_sheet = pd.DataFrame(s_res)
                    sheet_name = f"Result_{s_idx}"
                    if len(df_sheet.columns) > 0:
                        try:
                            sheet_name = str(df_sheet.columns[0])[:31]  # Excel sheet name max 31 chars
                        except:
                            pass
                    df_sheet.to_excel(writer, index=False, sheet_name=sheet_name)
            all_excel_data = all_excel_buffer.getvalue()

            st.download_button(
                label="📦 Download All (Excel)",
                data=all_excel_data,
                file_name=f"{proc_name}_all_results.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"excel_all_{proc_name}_{id(result)}",
                use_container_width=True
            )

# ---------- END DOWNLOAD ALL (Excel multi-sheet) ----------


        
        if result.get('rows_affected'):
            st.info(f"Rows affected: {result.get('rows_affected')}")

        if result.get('warnings'):
            with st.expander("⚠️ Warnings"):
                for warning in result['warnings']:
                    st.warning(f"{warning[0]}: {warning[2]}")

        # add success history
        if not any(h.get('procedure') == proc_name and
                   h.get('timestamp') and
                   (datetime.now() - h['timestamp']).seconds < 2
                   for h in st.session_state.execution_history):
            st.session_state.execution_history.append({
                'procedure': proc_name,
                'status': 'success',
                'timestamp': datetime.now()
            })

        return  # <-- สำคัญ: มี results แล้วไม่ต้องไปเข้า error

    # ---------- NO RESULT BUT SUCCESS → OK ----------
    if result.get('success') and not result.get('results'):
        return  # <-- ป้องกันไม่ให้ถูกตีเป็น error

    # ---------- ERROR ----------
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

    # add failed history
    if not any(h.get('procedure') == proc_name and
               h.get('timestamp') and
               (datetime.now() - h['timestamp']).seconds < 2
               for h in st.session_state.execution_history):
        st.session_state.execution_history.append({
            'procedure': proc_name,
            'status': 'failed',
            'timestamp': datetime.now()
        })
     
  
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
 
    # 🔹 ตรวจจับ encoding เบื้องต้นด้วย chardet
    detected = chardet.detect(raw)
    primary_enc = detected.get("encoding") or "utf-8"
    confidence = detected.get("confidence", 0)

    encodings_try = [primary_enc, 'utf-8-sig', 'cp874', 'tis-620', 'iso-8859-11', 
                     'utf-16', 'utf-16le', 'utf-16be', 'latin1']
    last_err = None

    # 🔹 ลองอ่านด้วย encoding ต่าง ๆ
    for enc in encodings_try:
        try:
            buf = BytesIO(raw)
            df = pd.read_csv(
                buf,
                encoding=enc,
                encoding_errors='replace',
                engine='python',
                sep=sep,
                on_bad_lines='skip',
                dtype=str
            )
            df.attrs['__encoding__'] = enc
            return df
        except Exception as e:
            last_err = e
            continue

    raise last_err or Exception("❌ Cannot decode CSV with known Thai encodings")
   

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
            file_info = {
                'name': file.name,
                'size': file.size,
                'type': self.get_file_type(file.name)
            }
    
            try:
                if file_info['type'] == 'csv':
                    raw_data = file.read()
                    detected = chardet.detect(raw_data)
                    encoding = detected.get("encoding", "utf-8") or "utf-8"
    
                    try:
                        df = pd.read_csv(BytesIO(raw_data), dtype=str, encoding=encoding, keep_default_na=False)
                        file_info['succeeded_encoding'] = encoding
                    except UnicodeDecodeError:
                        df = pd.read_csv(BytesIO(raw_data), dtype=str, encoding='latin-1', keep_default_na=False)
                        file_info['succeeded_encoding'] = 'latin-1'
    
                    file_info['sheets'] = ['Sheet1']
                    file_info['data'] = {'Sheet1': df}
    
                elif file_info['type'] == 'excel':
                    try:
                        excel_file = pd.ExcelFile(file)
                        file_info['sheets'] = excel_file.sheet_names
                        file_info['data'] = {
                            sheet: pd.read_excel(excel_file, sheet_name=sheet, dtype=str, keep_default_na=False)
                            for sheet in excel_file.sheet_names
                        }
                    except Exception as e:
                        st.warning(f"⚠️ Excel file '{file.name}' ไม่สามารถอ่านได้: {e}")
                        file_info['sheets'] = []
                        file_info['data'] = {}
                        file_info['error'] = str(e)
    
                # 🔥 ป้องกันชื่อไฟล์ซ้ำแบบ Windows → สร้างชื่อใหม่
                base_name = file.name
                new_key = base_name
                counter = 1
    
                while new_key in processed:
                    name, ext = base_name.rsplit(".", 1)
                    new_key = f"{name} ({counter}).{ext}"
                    counter += 1
    
                processed[new_key] = file_info
                file_info["display_name"] = new_key
    
            except Exception as e:
                st.error(f"Error processing {file.name}: {str(e)}")
    
        return processed

         
     


    def get_file_type(self, filename):
        if filename.lower().endswith('.csv'): return 'csv'
        elif filename.lower().endswith(('.xlsx', '.xls')): return 'excel'
        return 'unknown'
 

    def analyze_headers(self, processed_data, selected_sheets, selected_files):
        """
        วิเคราะห์ headers จากทุกไฟล์ที่เลือก
        รองรับโหมดรวม sheet
        """
        all_headers = set()
        file_headers = {}
        has_mismatch = False
        
        for filename, file_info in processed_data.items():
            if not selected_files.get(filename, True):
                continue
            
            sheet_name = selected_sheets.get(filename, file_info['sheets'][0])
            
            # ⭐ กรณีรวมทุก sheet
            if sheet_name == "ALL_SHEETS":
                for s in file_info['sheets']:
                    if s in file_info['data']:
                        headers = file_info['data'][s].columns.tolist()
                        all_headers.update(headers)
                        file_headers[f"{filename} ({s})"] = headers
            else:
                # กรณีเลือก 1 sheet (เดิม)
                if sheet_name in file_info['data']:
                    headers = file_info['data'][sheet_name].columns.tolist()
                    all_headers.update(headers)
                    file_headers[filename] = headers
        
        # ✅ FIX: ต้อง return ค่า (คุณลืม return)
        # ตรวจสอบว่ามี header ไม่ตรงกันหรือไม่
        if len(file_headers) > 1:
            header_sets = [set(h) for h in file_headers.values()]
            first_set = header_sets[0]
            has_mismatch = not all(s == first_set for s in header_sets)
        
        return all_headers, has_mismatch, file_headers
    
    
    def merge_files(self, processed_data, selected_sheets, selected_files, 
                    sheet_mode=None, uploaded_files_cache=None,  # ✅ FIX: เพิ่ม parameters เหล่านี้
                    header_mapping=None, excluded_headers=None):
        """
        รวมไฟล์ทั้งหมด รองรับโหมดรวม sheet
        """
        dataframes = []  # ✅ FIX: คุณลืมประกาศตัวแปรนี้
        processor = FileProcessor()  # ✅ FIX: ประกาศข้างนอก loop จะดีกว่า
        
        for filename, file_info in processed_data.items():
            if not selected_files.get(filename, True):
                continue
            
            sheet_name = selected_sheets.get(filename, file_info['sheets'][0])
            
            # ⭐ กรณีรวมทุก sheet
            if sheet_name == "ALL_SHEETS":
                # ✅ FIX: ดึง uploaded file จาก cache
                if uploaded_files_cache and filename in uploaded_files_cache:
                    uploaded_file = uploaded_files_cache[filename]
                    merged_sheet_df = processor.merge_all_sheets(uploaded_file)
                    
                    if merged_sheet_df is not None:
                        # ✅ เพิ่มคอลัมน์ระบุไฟล์ต้นทาง
                        merged_sheet_df['_source_file'] = filename
                        
                        # ✅ FIX: ต้องตรวจสอบและใช้ header_mapping ถ้ามี (logic เดิมของคุณ)
                        if header_mapping:
                            merged_sheet_df = merged_sheet_df.rename(columns=header_mapping)
                        
                        if excluded_headers:
                            merged_sheet_df = merged_sheet_df.drop(
                                columns=[col for col in excluded_headers if col in merged_sheet_df.columns],
                                errors='ignore'
                            )
                        
                        dataframes.append(merged_sheet_df)
                else:
                    # ถ้าไม่มี cache ให้ใช้ข้อมูลที่ประมวลผลไว้แล้ว
                    # รวม sheets จาก file_info['data']
                    sheet_dfs = []
                    for s in file_info['sheets']:
                        if s in file_info['data']:
                            df = file_info['data'][s].copy()
                            df['_source_sheet'] = s  # ✅ เพิ่มคอลัมน์ระบุ sheet
                            sheet_dfs.append(df)
                    
                    if sheet_dfs:
                        combined_df = pd.concat(sheet_dfs, ignore_index=True)
                        combined_df['_source_file'] = filename
                        
                        if header_mapping:
                            combined_df = combined_df.rename(columns=header_mapping)
                        
                        if excluded_headers:
                            combined_df = combined_df.drop(
                                columns=[col for col in excluded_headers if col in combined_df.columns],
                                errors='ignore'
                            )
                        
                        dataframes.append(combined_df)
            else:
                # กรณีเลือก 1 sheet (เดิม)
                if sheet_name in file_info['data']:
                    df = file_info['data'][sheet_name].copy()
                    df['_source_file'] = filename
                    
                    # ✅ FIX: ต้องใช้ header_mapping และ excluded_headers (logic เดิมของคุณ)
                    if header_mapping:
                        df = df.rename(columns=header_mapping)
                    
                    if excluded_headers:
                        df = df.drop(
                            columns=[col for col in excluded_headers if col in df.columns],
                            errors='ignore'
                        )
                    
                    dataframes.append(df)
        
        # ✅ FIX: รวม dataframes ทั้งหมดและ return
        if not dataframes:
            st.warning("ไม่มีข้อมูลที่จะรวม")
            return pd.DataFrame()
        
        merged_df = pd.concat(dataframes, ignore_index=True)
        return merged_df

st.markdown("""
<style>
/* ===== Force full width for inputs and buttons ===== */

/* สำหรับ text_input / password_input / selectbox */
div.stTextInput, div.stPasswordInput, div.stSelectbox, div.stFileUploader {
    width: 100% !important;
}

/* สำหรับปุ่มทั้งหมด */
div.stButton > button {
    width: 100% !important;
    display: block;
    text-align: center;
}

/* ปรับความกว้างของ columns ภายในคอนเทนเนอร์ import section */
section.main div.block-container {
    max-width: 100% !important;
    padding-right: 2rem;
    padding-left: 2rem;
}
</style>
""", unsafe_allow_html=True)

# ============================================================
# 🧹 ฟังก์ชันทำความสะอาดข้อมูลก่อน Import 
# ============================================================

def clean_dataframe_for_import(df, table_columns, column_mapping):
    """
    ทำความสะอาดข้อมูลก่อน import เข้า database
    - แปลงค่าว่างเป็น None สำหรับฟิลด์ตัวเลข
    - ตัด whitespace
    - แปลง type ให้เหมาะสม
    
    Args:
        df: DataFrame ที่จะ import
        table_columns: list of dict จาก get_cached_table_columns()
        column_mapping: dict mapping จาก file column -> db column
    
    Returns:
        DataFrame ที่ทำความสะอาดแล้ว
    """
    import pandas as pd
    import numpy as np
    
    df_clean = df.copy()
    
    # สร้าง mapping ของ column types จาก database
    col_types = {}
    for col_info in table_columns:
        col_name = col_info['COLUMN_NAME']
        data_type = col_info['DATA_TYPE'].lower()
        is_nullable = col_info.get('IS_NULLABLE', 'YES') == 'YES'
        
        col_types[col_name] = {
            'type': data_type,
            'nullable': is_nullable
        }
    
    # ทำความสะอาดเฉพาะ columns ที่จะ import
    for file_col, db_col in column_mapping.items():
        if file_col not in df_clean.columns or db_col not in col_types:
            continue
        
        db_type = col_types[db_col]['type']
        is_nullable = col_types[db_col]['nullable']
        
        # 1. ตัด whitespace
        if df_clean[file_col].dtype == 'object':
            df_clean[file_col] = df_clean[file_col].astype(str).str.strip()
        
        # 2. แปลงค่าว่าง/NaN เป็น None สำหรับฟิลด์ตัวเลข
        if db_type in ['int', 'bigint', 'smallint', 'tinyint', 'integer']:
            # แทนที่ค่าว่าง '' เป็น None
            df_clean[file_col] = df_clean[file_col].replace(['', 'nan', 'NaN', 'NULL', 'null', 'None'], None)
            
            # ถ้าฟิลด์ไม่ยอมรับ NULL และมีค่าว่าง → ใส่ 0
            if not is_nullable:
                df_clean[file_col] = df_clean[file_col].fillna(0)
            
            # แปลงเป็นตัวเลข (ถ้าไม่ได้ใส่ None)
            df_clean[file_col] = pd.to_numeric(df_clean[file_col], errors='coerce')
        
        elif db_type in ['float', 'double', 'decimal', 'numeric']:
            df_clean[file_col] = df_clean[file_col].replace(['', 'nan', 'NaN', 'NULL', 'null', 'None'], None)
            
            if not is_nullable:
                df_clean[file_col] = df_clean[file_col].fillna(0.0)
            
            df_clean[file_col] = pd.to_numeric(df_clean[file_col], errors='coerce')
        
        elif db_type in ['date', 'datetime', 'timestamp']:
            df_clean[file_col] = df_clean[file_col].replace(['', 'nan', 'NaN', 'NULL', 'null', 'None'], None)
            
            # แปลงเป็น datetime (ถ้าไม่ได้ใส่ None)
            df_clean[file_col] = pd.to_datetime(df_clean[file_col], errors='coerce')
        
        else:
            # ฟิลด์ text: แปลงค่าว่างเป็น None หรือ ''
            df_clean[file_col] = df_clean[file_col].replace(['nan', 'NaN', 'NULL', 'null', 'None'], '')
            
            if is_nullable:
                df_clean[file_col] = df_clean[file_col].replace('', None)
    
    return df_clean


# ===== TAB 1: IMPORT DATA =====
def render_import_tab():
    # ✅ ตรวจสอบ force_reset ก่อนทำอะไร
    if st.session_state.get('force_reset', False):
        with st.spinner("🔁 กำลังโหลดหน้าใหม่..."):
            time.sleep(0.3)
            st.cache_data.clear()
            # ล้างทุกอย่างยกเว้น db_manager 
            keys_to_delete = [k for k in st.session_state.keys() if k != 'db_manager']
            for key in keys_to_delete:
                del st.session_state[key]
            
            # ✅ ล้าง query params (บังคับ refresh)
            try:
                st.query_params.clear()
            except:
                pass
            
            # ✅ Rerun ครั้งที่ 2 เพื่อให้หน้าโหลดสะอาด
            st.rerun()
            
      
    st.subheader("📊 Quick Stats")
    col_stat1, col_stat2, col_stat3 = st.columns(3)
    with col_stat1:
        try:
            tables_info = get_cached_tables_info()
            tables = [table['TABLE_NAME'] for table in tables_info] if tables_info else []
            # 🛡️ ซ่อนตารางระบบที่ไม่ต้องให้ user เห็น
            HIDDEN_TABLES = ["user_permissions","sn"]
            tables = [t for t in tables if t not in HIDDEN_TABLES]
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
    st.header("📁 File Import to Database")

    # === Database & Processor setup ===
    if 'db_manager' not in st.session_state:
        st.session_state.db_manager = DatabaseManager()
    if 'file_processor' not in st.session_state:
        st.session_state.file_processor = FileProcessor()

    try:
        tables_info = get_cached_tables_info()
        tables = [table['TABLE_NAME'] for table in tables_info] if tables_info else []
        # 🛡️ ซ่อนตารางระบบที่ไม่ต้องให้ user เห็น
        HIDDEN_TABLES = ["user_permissions", "mysql", "performance_schema", "sys"]
        tables = [t for t in tables if t not in HIDDEN_TABLES]
    except Exception as e:
        st.warning(f"Could not get table info: {e}")
        tables = []
        tables_info = []

    selected_table = st.selectbox("🎯 Select Target Table", options=[""] + tables, help="Choose the table where you want to import your data")

    # ✅ แสดงคำอธิบายเล็ก ๆ เฉพาะเมื่อเลือกตาราง Broadband_daily
    if selected_table == "Broadband_daily":
        st.markdown(
            "<p style='color: #6c757d; font-size: 13px; margin-top: -10px;'>"
            "สำหรับ Update Ticket จากระบบ <b>SCOMS</b> และ <b>TTS</b> เพื่อจัดทำรายงาน Daily report"
            "</p>",
            unsafe_allow_html=True
        )
 
    if selected_table: 
        # ===== Show Table Info =====
        if tables_info:
            table_details = next((t for t in tables_info if t.get('TABLE_NAME') == selected_table), None)
            if table_details:
                col1_info, col2_info, col3_info = st.columns(3)
                with col1_info:
                    # ✅ พยายามใช้ COUNT(*) ก่อน ถ้า fail ให้ fallback เป็น TABLE_ROWS
                    try:
                        db = st.session_state.get('db_manager') or DatabaseManager()
                        conn = db.get_connection()
                        cursor = conn.cursor()
                        cursor.execute(f"SELECT COUNT(*) FROM {selected_table}")
                        exact_count = cursor.fetchone()[0]
                        cursor.close()
                        conn.close()
                        st.metric("📊 Rows", f"{exact_count:,}")
                    except Exception:
                        # fallback ไปใช้ค่าประมาณจาก INFORMATION_SCHEMA.TABLES
                        row_count = table_details.get('TABLE_ROWS', 0) or 0
                        st.metric("📊 Rows (est.)", f"{row_count:,}")
 
                with col2_info:
                    # ✅ พยายามดึง MAX(timestamp) จากตารางจริง
                    try:
                        db = st.session_state.get('db_manager') or DatabaseManager()
                        conn = db.get_connection()
                        cursor = conn.cursor()
                        # ตรวจหาคอลัมน์ timestamp ที่มีอยู่ในตาราง
                        cursor.execute("""
                            SELECT COLUMN_NAME 
                            FROM INFORMATION_SCHEMA.COLUMNS 
                            WHERE TABLE_SCHEMA = DATABASE() 
                            AND TABLE_NAME = %s
                            AND COLUMN_NAME IN ('timestamp', 'last_update', 'updated_at', 'update_time');
                        """, (selected_table,))
                        col = cursor.fetchone()
                        if col:
                            col_name = col[0]
                            cursor.execute(f"SELECT MAX({col_name}) FROM {selected_table}")
                            last_update_val = cursor.fetchone()[0]
                            if last_update_val:
                                # ✅ แสดงวันที่ + เวลาเต็ม
                                if isinstance(last_update_val, str):
                                    last_update = last_update_val[:19]
                                else:
                                    last_update = last_update_val.strftime("%Y-%m-%d %H:%M:%S")
                                st.metric("🕒 Updated", last_update)
                            else:
                                st.metric("🕒 Updated", "No data")
                        else:
                            # ถ้าไม่พบคอลัมน์ timestamp -> fallback ไปใช้ UPDATE_TIME เดิม
                            update_time = table_details.get('UPDATE_TIME')
                            if update_time:
                                if isinstance(update_time, str):
                                    last_update = update_time[:19]
                                else:
                                    last_update = update_time.strftime("%Y-%m-%d %H:%M:%S")
                                st.metric("🕒 Updated", last_update)
                            else:
                                st.metric("🕒 Updated", "Unknown")
                        cursor.close()
                        conn.close()
                    except Exception as e:
                        st.metric("🕒 Updated", "Unknown")
                        st.caption(f"⚠️ timestamp check failed: {e}")
    
                with col3_info:
                    data_length = table_details.get('DATA_LENGTH', 0) or 0
                    if data_length > 0:
                        size_mb = data_length / (1024 * 1024)
                        st.metric("💾 Size", f"{size_mb:.0f} MB")


        # ===== Show Preview Button (แก้ไข: แสดง 5 record ล่าสุดตาม timestamp) =====
#        st.subheader(f"👀 Preview: {selected_table}")
#        if st.button("🔄 Show Preview", type="secondary"):
#            try:
#                with st.spinner("Loading preview..."):
#                    db = st.session_state.get('db_manager') or DatabaseManager()
#                    conn = db.get_connection()
#                    cursor = conn.cursor()
#                    
#                    # ตรวจหาคอลัมน์ timestamp
#                    cursor.execute("""
#                        SELECT COLUMN_NAME 
#                        FROM INFORMATION_SCHEMA.COLUMNS 
#                        WHERE TABLE_SCHEMA = DATABASE() 
#                        AND TABLE_NAME = %s
#                        AND COLUMN_NAME IN ('timestamp', 'last_update', 'updated_at', 'update_time')
#                        ORDER BY COLUMN_NAME
#                        LIMIT 1;
#                    """, (selected_table,))
#                    
#                    timestamp_col = cursor.fetchone()
#                    
#                    if timestamp_col:
#                        # มี timestamp column -> เรียงตาม timestamp
#                        ts_name = timestamp_col[0]
#                        query = f"SELECT * FROM {selected_table} ORDER BY {ts_name} DESC LIMIT 5"
#                    else:
#                        # ไม่มี timestamp -> ใช้วิธีเดิม
#                        query = f"SELECT * FROM {selected_table} ORDER BY 1 DESC LIMIT 5"
#                    
#                    preview_data = pd.read_sql(query, conn)
#                    cursor.close()
#                    conn.close()
#                
#                if not preview_data.empty:
#                    st.dataframe(preview_data, use_container_width=True, hide_index=True)
#                    st.success(f"📊 Showing last 5 rows from {len(preview_data.columns)} columns")
#                else:
#                    st.warning("📭 Table is empty or preview unavailable")
#            except Exception as e:
#                st.error(f"❌ Error: {str(e)}")

        # ===== Upload File (รองรับหลายไฟล์) =====
        st.subheader("📤 Upload File")
        
        uploaded_files = st.file_uploader(
            "Choose files to import", 
            type=['csv', 'xlsx', 'xls'], 
            help="Max size: 200MB per file",
            key="import_uploader",
            accept_multiple_files=True  # ✅ รองรับหลายไฟล์
        )
        
        if uploaded_files:
            # ===== แสดงข้อมูลไฟล์ที่อัพโหลด =====
            st.markdown(f"""
            <div class="file-info">
                <h4>📄 จำนวนไฟล์ที่อัพโหลด: {len(uploaded_files)} ไฟล์</h4>
            </div>
            """, unsafe_allow_html=True)
            
            try:
                with st.spinner("Reading files..."):
                    # ===== อ่านไฟล์ทั้งหมดเก็บใน list =====
                    df_list = []
                    file_info_list = []
                    
                    for uploaded_file in uploaded_files:
                        # อ่านไฟล์แต่ละไฟล์
                        if uploaded_file.name.endswith('.csv'):
                            uploaded_file.seek(0)
                            df_temp = read_csv_safely(uploaded_file)
                            encoding_used = df_temp.attrs.get('__encoding__', 'unknown')
                        else:
                            try:
                                # ✅ ลองอ่าน Excel ปกติ (.xlsx)
                                df_temp = pd.read_excel(uploaded_file, engine='openpyxl')
                                encoding_used = 'n/a'
                            except Exception:
                                try:
                                    # ✅ ลองอ่าน Excel เก่า (.xls)
                                    df_temp = pd.read_excel(uploaded_file, engine='xlrd')
                                    encoding_used = 'n/a'
                                except Exception as e:
                                    uploaded_file.seek(0)
                                    raw_start = uploaded_file.read(2048)
                                    uploaded_file.seek(0)
                                    text_sample = raw_start.decode(errors="ignore").lower()
                                    
                                    if "<table" in text_sample:
                                        # ✅ HTML-based .xls
                                        import chardet
                                        detected = chardet.detect(raw_start)
                                        encoding_used = detected.get("encoding", "utf-8")
                                        html_text = uploaded_file.read().decode(encoding_used, errors="replace")
                                        tables = pd.read_html(html_text)
                                        df_temp = tables[0] if tables else pd.DataFrame()
                                        df_temp.attrs["__encoding__"] = encoding_used
                                    else:
                                        try:
                                            # ✅ ลองอ่านด้วย UTF-8 ก่อน
                                            df_temp = pd.read_csv(uploaded_file, encoding='utf-8', on_bad_lines='skip')
                                            encoding_used = 'utf-8'



                                        except UnicodeDecodeError:
                                            import chardet
                                            uploaded_file.seek(0)
                                            raw_data = uploaded_file.read(4096)
                                            detected = chardet.detect(raw_data)
                                            detected_enc = detected.get("encoding", "latin1") or "latin1"
                                            uploaded_file.seek(0)
                                            try:
                                                # ✅ ลองอ่านด้วย encoding ที่ตรวจเจอ
                                                df_temp = pd.read_csv(uploaded_file, encoding=detected_enc, on_bad_lines='skip')
                                                encoding_used = detected_enc
                                            except Exception as e_csv:
                                                uploaded_file.seek(0)
                                                st.warning(f"⚠️ Primary parser failed ({detected_enc}): {e_csv}")
                                                # ✅ Fallback สุดท้าย: ใช้ Python engine เพื่อกัน Buffer overflow / malformed CSV
                                                for enc in ['windows-874', 'tis-620', 'iso-8859-11', 'latin1']:
                                                    try:
                                                        uploaded_file.seek(0)
                                                        df_temp = pd.read_csv(
                                                            uploaded_file,
                                                            encoding=enc,
                                                            on_bad_lines='skip',
                                                            engine='python',   # ✅ ใช้ parser ที่ทน format เพี้ยน
                                                            sep=None,          # ✅ ให้ pandas เดา delimiter เอง (, / ; / tab)
                                                            quoting=3,         # ✅ ปิด quote parsing ป้องกัน " เปิดไม่ปิด
                                                            dtype=str,
                                                            keep_default_na=False
                                                        )
                                                        encoding_used = enc
                                                        break
                                                    except Exception as e_fallback:
                                                        last_err = e_fallback
                                                        continue
                                                else:
                                                    raise Exception(f"❌ Cannot read CSV with any fallback encoding. Last error: {last_err}")
 

                                         
                                            
                        # ✅ ตรวจว่าคอลัมน์เป็นตัวเลข (แสดงว่า header ไม่ถูกอ่าน)
                        if all(isinstance(c, (int, float)) for c in df_temp.columns):
                            first_row = df_temp.iloc[0].tolist()
                            if any(pd.notnull(x) for x in first_row):
                                df_temp.columns = first_row
                                df_temp = df_temp.drop(df_temp.index[0]).reset_index(drop=True)
                        
                        # เก็บ DataFrame และข้อมูลไฟล์
                        df_list.append(df_temp)
                        file_info_list.append({
                            'name': uploaded_file.name,
                            'size': uploaded_file.size,
                            'type': uploaded_file.type,
                            'rows': len(df_temp),
                            'columns': len(df_temp.columns),
                            'encoding': encoding_used
                        })
                    
                    # ===== รวม DataFrame ทั้งหมดด้วย pd.concat() =====
                    df = pd.concat(df_list, ignore_index=True)
                    
                    # ===== แสดงข้อมูลแต่ละไฟล์ =====
                    st.markdown("### 📊 ข้อมูลแต่ละไฟล์")
                    for idx, info in enumerate(file_info_list, 1):
                        st.markdown(f"""
                        <div style="background-color:#f8f9fa; padding:10px; border-radius:5px; margin-bottom:10px;">
                            <strong>ไฟล์ที่ {idx}:</strong> {info['name']}<br>
                            <span style="color:#666;">
                                📏 Size: {info['size'] / 1024:.2f} KB | 
                                📝 Type: {info['type']} | 
                                📊 Rows: {info['rows']:,} | 
                                📋 Columns: {info['columns']} | 
                                🔤 Encoding: {info['encoding']}
                            </span>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    # ===== แสดงยอดรวมหลัง merge =====
                    st.success(f"✅ ไฟล์ทั้งหมด: {len(uploaded_files)} ไฟล์")
                    st.info(f"📊 **ยอดรวมหลัง Merge:** {len(df):,} rows × {len(df.columns)} columns")
                    
                    # ===== Data Preview =====
                    st.subheader("📋 Data Preview")
                    with st.expander("📋 Data Preview (คลิกเพื่อดูข้อมูลตัวอย่าง)", expanded=False):
                        st.dataframe(df.head(10), use_container_width=True)
                
                # ===== Column Mapping (ส่วนนี้ไม่เปลี่ยน) =====
                st.subheader("🔗 Column Mapping")
                table_columns = get_cached_table_columns(selected_table)
                
                if not table_columns:
                    st.error("Cannot get table columns")
                    return
                
                db_column_names = [col['COLUMN_NAME'] for col in table_columns]
                file_columns = list(df.columns)
                
                st.info(f"**File Columns:** {len(file_columns)} | **Table Columns:** {len(db_column_names)}")
                
                column_mapping = {}
                
                with st.expander("🔽 View/Hide Column Mapping", expanded=False):
                    cols = st.columns(2)
                    with cols[0]:
                        st.write("**File Column**")
                    with cols[1]:
                        st.write("**→ Database Column**")
                    
                    for file_col in file_columns:
                        c1, c2 = st.columns(2)
                        with c1:
                            st.text(file_col)
                        with c2:
                            default_index = 0
                            if file_col in db_column_names:
                                default_index = db_column_names.index(file_col)
                            
                            selected_db_col = st.selectbox(
                                f"Map {file_col}",
                                options=["🔴-- Skip --"] + db_column_names,
                                index=default_index + 1 if file_col in db_column_names else 0,
                                key=f"mapping_{file_col}",
                                label_visibility="collapsed"
                            )
                            
                            if selected_db_col != "🔴-- Skip --":
                                column_mapping[file_col] = selected_db_col
                
                if column_mapping:
                    st.success(f"✅ Mapped {len(column_mapping)} columns")

                    # ✅ เพิ่มตรงนี้: แปลงข้อมูลเป็น string ทั้งหมด
                    df_to_import = df[list(column_mapping.keys())].copy()
                    df_to_import = df_to_import.rename(columns=column_mapping)
                    
                    # แปลงทุก column เป็น string และจัดการ NaN
                    for col in df_to_import.columns:
                        df_to_import[col] = df_to_import[col].apply(
                            lambda x: None if pd.isna(x) else str(x)
                        )
                else:
                    st.warning("⚠️ No columns mapped")
                    df_to_import = None

 

                # ============================================================
                # 🔐 Authorization + แสดง Allowed Tables
                # ============================================================
                
                st.divider()
                
                # --- ช่องกรอก Secret Key ---
                secret_key = st.text_input(
                    "Secret Key to unlock import",
                    type="password",
                    placeholder="Enter your secret key",
                    key="import_secret_key"
                )
                
                user_perm = get_user_permission(secret_key)
                
                if not user_perm:
                    st.warning("🔑 Enter correct key to unlock Import Data button.", icon="🔒")
                    import_disabled = True
                else:
                    role = user_perm["role"]
                    allowed_tables = user_perm.get("allowed_tables", [])
                    
                    # ตรวจสอบว่ามีสิทธิ์ import table นี้หรือไม่
                    if role == "Admin" or selected_table in allowed_tables:
                        st.success(f"✅ Authorized as **{role}**")
                        import_disabled = False
                    else:
                        st.error(f"🚫 You are not allowed to import into `{selected_table}`.")
                        import_disabled = True
                    
                    # ============================================================
                    # 📋 แสดงรายการ Tables ที่มีสิทธิ์เข้าถึง
                    # ============================================================
                    
                    st.markdown("---")
                    
                    # ตรวจสอบว่า allowed_tables ว่างหรือไม่
                    if not allowed_tables or allowed_tables == [''] or allowed_tables == []:
                        # ถ้าว่างเปล่า = มีสิทธิ์ทุก table
                        st.markdown("""
                        <div style="background-color:#e8f5e9;border-left:6px solid #4caf50;
                                    padding:12px 18px;border-radius:8px;font-size:14px;">
                            <strong>🔓 Unlocked Tables:</strong><br>
                            <span style="color:#2e7d32;font-weight:bold;">All Tables</span>
                            <span style="color:#666;font-size:13px;"> (Full Access)</span>
                        </div>
                        """, unsafe_allow_html=True)
                    else:
                        # แสดงรายการ tables ที่มีสิทธิ์
                        tables_list = ", ".join([f"<code>{t}</code>" for t in allowed_tables])
                        table_count = len(allowed_tables)
                        
                        st.markdown(f"""
                        <div style="background-color:#e3f2fd;border-left:6px solid #2196f3;
                                    padding:12px 18px;border-radius:8px;font-size:14px;">
                            <strong>🔓 Unlocked Tables ({table_count}):</strong><br>
                            <span style="color:#1565c0;font-size:13px;line-height:1.8;">
                                {tables_list}
                            </span>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    st.markdown("<br>", unsafe_allow_html=True)
                    
                    # ============================================================
                    # 🚀 ปุ่ม Import Data (แก้ไข: ป้องกันการกดซ้ำ)
                    # ============================================================
                    
                    # ✅ สร้าง session_state เพื่อป้องกันการกดซ้ำ
                    if 'import_in_progress' not in st.session_state:
                        st.session_state.import_in_progress = False
                    
                    # ✅ disable ปุ่มถ้ากำลัง import อยู่
                    button_disabled = import_disabled or st.session_state.import_in_progress

                    if st.button("🚀 Import Data", type="primary", use_container_width=True, disabled=button_disabled):
                        if not column_mapping:
                            st.error("Please map at least one column")
                        else:
                            # ✅ ล็อกปุ่มทันทีเมื่อเริ่ม import
                            st.session_state.import_in_progress = True
                            st.session_state['current_import_user'] = secret_key.strip()
                            st.rerun()  # rerun เพื่อให้ปุ่ม disabled ทันที
                    
                    # ✅ ตรวจสอบว่าต้อง import จริงหรือไม่
                    if st.session_state.import_in_progress and column_mapping:
                        try:
                            # ============================================================
                            # 🧹 ทำความสะอาดข้อมูลก่อน import
                            # ============================================================
                            with st.spinner("🧹 Cleaning data..."):
                                # ดึง column info จาก database
                                table_columns = get_cached_table_columns(selected_table)
                                
                                # ทำความสะอาดข้อมูล (ส่ง column_mapping ด้วย)
                                df_clean = clean_dataframe_for_import(df, table_columns, column_mapping)

                                # ✅ เพิ่มตรงนี้: แปลงทุก column เป็น string
                                for col in df_clean.columns:
                                    df_clean[col] = df_clean[col].apply(
                                        lambda x: None if pd.isna(x) else str(x)
                                    )
                                # st.success("✅ Data cleaned successfully")
                                
                                # แสดงสถิติการทำความสะอาด
                                #null_count = df_clean.isnull().sum().sum()
                                #if null_count > 0:
                                #    st.info(f"ℹ️ Found {null_count} NULL values after cleaning (will be handled by database)")
                        
                            # ============================================================
                            # 🔹 บันทึก Log
                            # ============================================================
                            try:
                                username = secret_key.strip()
                                db = st.session_state.get('db_manager') or DatabaseManager()
                                conn = db.get_connection()
                                cursor = conn.cursor()
                                cursor.execute("""
                                    INSERT INTO activity_log (username, action, target, ip_address, details)
                                    VALUES (%s, %s, %s, %s, %s)
                                """, (
                                    username,
                                    "Import Data",
                                    selected_table,
                                    st.session_state.get('client_ip', 'unknown'),
                                    f"rows={len(df_clean)}"
                                ))
                                conn.commit()
                                cursor.close()
                                conn.close()
                            except Exception as log_err:
                                st.warning(f"⚠️ Failed to write activity log: {log_err}")
                            
                            # ============================================================
                            # 🔹 Import Data เข้าฐานข้อมูล (ใช้ df_clean แทน df)
                            # ============================================================
                            fresh_db = DatabaseManager()
                            with st.spinner(f"Importing {len(df_clean)} rows..."):
                                result = fresh_db.import_data(selected_table, df_clean, column_mapping)
                            fresh_db.close_connection()


                            # ============================================================
                            # ✅ ULTIMATE FIX: เก็บ Import State + Procedure Result
                            # ============================================================
                            
                            # ใส่ใน render_import_tab() หลังจาก Import สำเร็จ
                            
                            # ✅ เมื่อ Import สำเร็จ → เก็บ result ไว้ใน session
                            if result.get('success'):
                                st.success(f"✅ {result['message']}")
                                st.balloons()  
                                st.markdown("""
                                <a href="?" target="_self" style="display:inline-block;
                                   background-color:#0066cc;color:white;text-decoration:none;
                                   padding:12px 24px;border-radius:6px;text-align:center;
                                   font-weight:bold;width:100%;box-sizing:border-box;">
                                    🔄 โหลดหน้าใหม่  
                                </a>
                                """, unsafe_allow_html=True)
                              
                                # ✅ เก็บ import result ใน session state
                                st.session_state['last_import_success'] = {
                                    'table': selected_table,
                                    'message': result['message'],
                                    'rows_affected': result.get('rows_affected', 0),
                                    'timestamp': time.time(),
                                    'username': secret_key.strip()  # เก็บ username ไว้ใช้ใน procedure
                                }


                                # ============================================================
                                # 🔮 AI Recommendation (แสดงเฉพาะคำแนะนำ)
                                # ============================================================
                                st.divider()
                                st.subheader("💡 AI Recommendation")
                                
                                try:
                                    current_action = f"Import Data:{selected_table}"
                                    suggestion, freq, confidence = recommend_action(current_action) or (None, 0, 0)
                                    
                                    if suggestion:
                                        proc_name = suggestion.replace("Execute Procedure:", "").strip()
                                        
                                        # สีตามระดับความเชื่อมั่น
                                        if confidence >= 80:
                                            conf_color = "#2ecc71"
                                            emoji = "🟢"
                                            conf_text = "สูงมาก"
                                        elif confidence >= 50:
                                            conf_color = "#f1c40f"
                                            emoji = "🟡"
                                            conf_text = "ปานกลาง"
                                        else:
                                            conf_color = "#e74c3c"
                                            emoji = "🔴"
                                            conf_text = "ค่อนข้างต่ำ"
                                        
                                        # แสดงข้อความแนะนำ
                                        st.markdown(f"""
                                        <div style="background-color:#f8f9fb;border-left:6px solid {conf_color};
                                                    padding:12px 18px;border-radius:10px;font-size:15px;line-height:1.6;">
                                            <strong>🤖 Smart AI Operator:</strong><br>
                                            จากการวิเคราะห์พฤติกรรมการใช้งานย้อนหลัง ระบบคาดการณ์ว่า<br>
                                            <span style="color:#2d3436;"><b>Procedure <code>{proc_name}</code></b></span> 
                                            เป็นขั้นตอนถัดไปที่เหมาะสมสำหรับกระบวนการนี้<br>
                                            <span style="font-size:13.5px;color:#636e72;">
                                            อ้างอิงจากรูปแบบการทำงานเดิม <b>{freq}</b> ครั้ง 
                                            และมีระดับความเชื่อมั่น <b style="color:{conf_color};">{emoji} {confidence:.1f}% ({conf_text})</b>
                                            </span>
                                        </div>
                                        """, unsafe_allow_html=True)
                                        
                                        # Confidence Bar
                                        total_patterns = round(freq / (confidence / 100)) if confidence > 0 else freq
                                        freq_fmt = f"{freq:,}"
                                        total_fmt = f"{total_patterns:,}"
                                        conf_fmt = f"{confidence:.2f}"
                                        
                                        st.markdown(f"""
                                        <div style="background-color:#eaecef;border-radius:8px;margin-top:6px;">
                                          <div style="width:{confidence}%;background-color:{conf_color};
                                                      height:12px;border-radius:8px;"></div>
                                        </div>
                                        
                                        <div style="font-size:13px;color:#555;margin-top:6px;">
                                          <b style="color:{conf_color};">Confidence Level:</b>
                                          <span style="font-weight:bold;color:{conf_color};">{conf_fmt}%</span>
                                        </div>
                                        
                                        <div style="font-size:12.5px; color:#7f8c8d; margin-top:2px; font-family:Consolas, 'Courier New', monospace;">
                                          {freq_fmt} ÷ {total_fmt} × 100  =  <b>{conf_fmt}%</b>
                                        </div>
                                        """, unsafe_allow_html=True)
                                        
 
                                    
                                    else:
                                        # กรณีไม่มีข้อมูลเพียงพอ
                                        st.markdown("""
                                        <div style="background-color:#f8f9fb;border-left:6px solid #b2bec3;
                                                    padding:12px 18px;border-radius:10px;font-size:15px;line-height:1.6;">
                                            <strong>🤖 Smart AI Operator:</strong><br>
                                            ขณะนี้ระบบยังไม่มีข้อมูลเพียงพอสำหรับการวิเคราะห์ขั้นตอนถัดไป<br>
                                            กรุณาดำเนินการเพิ่มเติมเพื่อให้ระบบเรียนรู้ pattern ได้มากขึ้น
                                        </div>
                                        """, unsafe_allow_html=True)
                                        
                                        st.markdown("""
                                        <div style="background-color:#eaecef;border-radius:8px;margin-top:6px;">
                                          <div style="width:0%;background-color:#b2bec3;height:12px;border-radius:8px;"></div>
                                        </div>
                                        <div style="font-size:13px;color:#555;margin-top:2px;">
                                          Confidence Level: <b style="color:#b2bec3;">0.0%</b>
                                        </div>
                                        """, unsafe_allow_html=True)
                                
                                except Exception as e:
                                    st.warning(f"⚠️ Suggestion module error: {e}")
                            
                            else:
                                st.error(f"❌ Import failed: {result.get('error')}")
                        
                        except Exception as import_err:
                            st.error(f"❌ Import process error: {import_err}")
                            st.exception(import_err)
                        
                        finally:
                            # ✅ ปลดล็อกปุ่มหลังจาก import เสร็จ
                            st.session_state.import_in_progress = False
                    
                    
                    # ============================================================
                    # ⚙️ Quick Action Section (วางนอก Import Block)
                    # ============================================================
                    st.markdown("---")
                    
                    # ดึง import result จาก session
                    last_import = st.session_state.get('last_import_success')
                    
                    # ตรวจสอบว่า import สำเร็จและเป็น table Broadband_daily หรือไม่
                    if last_import and last_import.get('table') == 'Broadband_daily':
                        st.markdown("### ⚙️ Quick Action: Run Procedure")
                        
                        st.markdown("""
                        <div style="background-color:#fff3cd;border-left:6px solid #ffc107;
                                    padding:12px 18px;border-radius:8px;font-size:14px;margin-bottom:15px;">
                            <strong>💡 Suggested Next Step:</strong><br> 
                            หากท่านได้ดำเนินการนำเข้าข้อมูลจากระบบ <b>TTS</b> และ <b>SCOMS</b> เรียบร้อยแล้ว<br>
                            กรุณาดำเนินการกด<b>Quick Run<code style="background:#e8f4f8;padding:2px 8px;border-radius:4px;">
                            update_Broadband_daily</code></b><br> เพื่อปรับปรุงข้อมูลใน <b>Dashboard Daily Report</b> ให้เป็นปัจจุบัน
                        </div>
                        """, unsafe_allow_html=True)


                        st.markdown("""
                        <a href="?" target="_self" style="display:inline-block;
                        background-color:#0066cc;color:white;text-decoration:none;
                        padding:12px 24px;border-radius:6px;text-align:center;
                        font-weight:bold;width:100%;box-sizing:border-box;">
                        🔄 โหลดหน้าใหม่  
                        </a>
                        """, unsafe_allow_html=True)                      
                        
                        # แสดงข้อมูล import ที่เพิ่งทำ
                        import_time = time.strftime('%H:%M:%S', time.localtime(last_import['timestamp']))
                        st.caption(f"📊 Last import: **{last_import['rows_affected']:,} rows** at {import_time}")
                        
                        # สร้าง session keys
                        if 'update_and_result' not in st.session_state:
                            st.session_state.update_and_result = None
                        
                        # ============================================================
                        # Callback Function
                        # ============================================================
                        def execute_update_and_callback():
                            """Execute procedure โดยไม่ทำให้หน้าจอเด้ง"""
                            try:
                                db = DatabaseManager()
                                conn = db.get_connection()
                                cursor = conn.cursor()
                                
                                # นับข้อมูลก่อนรัน
                                cursor.execute("SELECT COUNT(*) FROM Broadband_daily")
                                before_count = cursor.fetchone()[0]
                                
                                # รัน procedure
                                cursor.callproc("update_Broadband_daily")
                                rows_affected = cursor.rowcount
                                
                                # Fetch result sets
                                try:
                                    for rs in cursor.stored_results():
                                        rs.fetchall()
                                except:
                                    pass
                                
                                # นับข้อมูลหลังรัน
                                cursor.execute("SELECT COUNT(*) FROM Broadband_daily")
                                after_count = cursor.fetchone()[0]
                                difference = after_count - before_count
                                
                                conn.commit()
                                
                                # บันทึก log
                                username = last_import.get('username', 'unknown')
                                cursor.execute("""
                                    INSERT INTO activity_log (username, action, target, ip_address, details)
                                    VALUES (%s, %s, %s, %s, %s)
                                """, (
                                    username,
                                    "Execute Procedure",
                                    "update_Broadband_daily",
                                    st.session_state.get('client_ip', 'unknown'),
                                    f"Auto-run after Broadband_daily import | Rows: {rows_affected} | Before: {before_count} | After: {after_count} | Diff: {difference:+d}"
                                ))
                                conn.commit()
                                
                                cursor.close()
                                conn.close()
                                
                                # เก็บผลลัพธ์
                                st.session_state.update_and_result = {
                                    "success": True,
                                    "rows_affected": rows_affected,
                                    "before_count": before_count,
                                    "after_count": after_count,
                                    "difference": difference,
                                    "timestamp": time.time()
                                }
                                
                            except Exception as e:
                                st.session_state.update_and_result = {
                                    "success": False,
                                    "error": str(e),
                                    "timestamp": time.time()
                                }
                        
                        # ============================================================
                        # UI Controls
                        # ============================================================
                        col_btn, col_clear = st.columns([4, 1])
                        # ✅ ป้องกันกดซ้ำ
                        if "run_proc_in_progress" not in st.session_state:
                            st.session_state.run_proc_in_progress = False
                        
                        button_disabled = st.session_state.run_proc_in_progress
                        
                        with col_btn:
                            if st.button(
                                "⚡ Quick Run For Update",
                                type="primary",
                                use_container_width=True,
                                key="btn_run_update_Broadband_daily",
                                disabled=button_disabled,
                                help="Execute update_Broadband_daily stored procedure"
                            ):
                                st.session_state.run_proc_in_progress = True
                                # execute_update_and_callback() 
                                show_loading_overlay()  # ✅ แสดง overlay ทันที
                                st.experimental_rerun()  # ✅ รีเฟรชเพื่อให้ overlay ค้างก่อนเริ่มทำงานจริง
                        # ============================================================
                        # ดำเนินการเมื่อ flag ถูกเปิด
                        # ============================================================
                        if st.session_state.run_proc_in_progress:
                            show_loading_overlay()
                            with st.spinner("⚙️ Running update_Broadband_daily..."):
                                time.sleep(0.3)  # ✅ หน่วงเล็กน้อยให้ overlay แสดง
                                execute_update_and_callback()
                                st.session_state.run_proc_in_progress = False
                                st.experimental_rerun()
                    
                        
                        with col_clear:
                            if st.button("✖️", use_container_width=True, key="btn_close_quick_action", help="Close this section"):
                                st.session_state.pop('last_import_success', None)
                                st.session_state.pop('update_and_result', None)
                                st.session_state.run_proc_in_progress = False  # ✅ รีเซ็ต flag
                                st.rerun()
                        
                        # ============================================================
                        # แสดงผลลัพธ์
                        # ============================================================
                        if st.session_state.update_and_result:
                            result = st.session_state.update_and_result
                            
                            st.markdown("---")
                            
                            if result.get("success"):
                                st.success("✅ Procedure update_Broadband_daily executed successfully!")
    
    
                                
                                if result['rows_affected'] > 0 or result['difference'] != 0:
                                    # st.info(f"ℹ️ Procedure processed {result['rows_affected']:,} rows")
                                    st.balloons()
                                    # ✅ เพิ่มส่วนนี้หลังจาก update สำเร็จ
                                    st.markdown("""
                                    <div style="margin-top:10px; padding:10px; border-left:4px solid #f39c12; background-color:#fffbea;">
                                        ⚠️ <b>กรุณารีเฟรชข้อมูลที่ Looker Studio</b><br>
                                        👉 <a href="https://lookerstudio.google.com/reporting/1483b6e3-3477-4906-8966-ec276423ec27" 
                                              target="_blank" 
                                              style="color:#0073e6; text-decoration:none; font-weight:bold;">
                                              เปิดลิงก์เพื่อรีเฟรชข้อมูลใน Dashboard</a>
                                    </div>
                                    """, unsafe_allow_html=True)

            
                                else:
                                    st.warning("⚠️ No rows affected (this might be normal)")
                                
                                # แสดงเวลา
                                exec_time = time.strftime('%H:%M:%S', time.localtime(result['timestamp']))
                                st.caption(f"🕐 Executed at: {exec_time}")
                            
                            else:
                                st.error("❌ Procedure execution failed")
                                st.error(result.get('error', 'Unknown error'))
                                
                                with st.expander("🔍 Error Details"):
                                    st.code(result.get('error', ''), language='text')
                    
                                               

            except Exception as e:
                st.error(f"❌ Error processing file: {str(e)}")
                st.exception(e)
 
def log_activity(username, action, target, details=None):
    """บันทึก Log ลงในฐานข้อมูล"""
    try:
        db = st.session_state.get('db_manager') or DatabaseManager()
        conn = db.get_connection()
        cursor = conn.cursor()
        ip = st.session_state.get('client_ip', 'unknown')
        sql = """
            INSERT INTO activity_log (username, action, target, ip_address, details)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (username, action, target, ip, str(details)))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        st.warning(f"⚠️ Failed to write activity log: {e}")

# ====== 🔮 AI Suggestion Section (Auto Procedure Recommendation) ======

def recommend_action(current_action):
    """แนะนำ Procedure ที่มักถูกรันหลัง Import พร้อมค่า Confidence (%)"""
    try:
        db = st.session_state.get('db_manager') or DatabaseManager()
        conn = db.get_connection()
        cursor = conn.cursor()

        # ดึง pattern ที่เกิดหลัง import (จำกัดเวลา 10 นาที)
        query = """
            SELECT next_action, COUNT(*) AS freq
            FROM (
                SELECT 
                    CONCAT(a.action, ':', a.target) AS prev_action,
                    CONCAT(b.action, ':', b.target) AS next_action
                FROM activity_log a
                JOIN activity_log b 
                  ON a.username = b.username
                 AND b.timestamp > a.timestamp
                 AND TIMESTAMPDIFF(MINUTE, a.timestamp, b.timestamp) <= 30
                WHERE a.action = 'Import Data'
            ) seq
            WHERE prev_action = %s
              AND (
                    next_action LIKE 'Run Procedure%%'
                 OR next_action LIKE 'Execute Procedure%%'
              )
            GROUP BY next_action
            ORDER BY freq DESC
            LIMIT 1;
        """
        cursor.execute(query, (current_action,))
        row = cursor.fetchone()

        # ดึงจำนวนครั้งทั้งหมดของการ Import ตารางนี้ เพื่อใช้คำนวณ %
        total_query = "SELECT COUNT(*) FROM activity_log WHERE CONCAT(action, ':', target) = %s"
        cursor.execute(total_query, (current_action,))
        total_imports = cursor.fetchone()[0] or 0

        cursor.close()
        conn.close()

        if row:
            next_action, freq = row
            confidence = (freq / total_imports * 100) if total_imports > 0 else 0
            return next_action, freq, confidence
    except Exception as e:
        st.warning(f"⚠️ AI suggestion failed: {e}")
    return None, None, 0
 
 
 
def log_activity(username, action, target, details=None):
    """บันทึก Log ลงในฐานข้อมูล"""
    try:
        db = st.session_state.get('db_manager') or DatabaseManager()
        conn = db.get_connection()
        cursor = conn.cursor()
        ip = st.session_state.get('client_ip', 'unknown')
        sql = """
            INSERT INTO activity_log (username, action, target, ip_address, details)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (username, action, target, ip, str(details)))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        st.warning(f"⚠️ Failed to write activity log: {e}")

# ====== 🔮 AI Suggestion Section (Auto Procedure Recommendation) ======

def recommend_action(current_action):
    """แนะนำ Procedure ที่มักถูกรันหลัง Import พร้อมค่า Confidence (%)"""
    try:
        db = st.session_state.get('db_manager') or DatabaseManager()
        conn = db.get_connection()
        cursor = conn.cursor()

        # ดึง pattern ที่เกิดหลัง import (จำกัดเวลา 10 นาที)
        query = """
            SELECT next_action, COUNT(*) AS freq
            FROM (
                SELECT 
                    CONCAT(a.action, ':', a.target) AS prev_action,
                    CONCAT(b.action, ':', b.target) AS next_action
                FROM activity_log a
                JOIN activity_log b 
                  ON a.username = b.username
                 AND b.timestamp > a.timestamp
                 AND TIMESTAMPDIFF(MINUTE, a.timestamp, b.timestamp) <= 30
                WHERE a.action = 'Import Data'
            ) seq
            WHERE prev_action = %s
              AND (
                    next_action LIKE 'Run Procedure%%'
                 OR next_action LIKE 'Execute Procedure%%'
              )
            GROUP BY next_action
            ORDER BY freq DESC
            LIMIT 1;
        """
        cursor.execute(query, (current_action,))
        row = cursor.fetchone()

        # ดึงจำนวนครั้งทั้งหมดของการ Import ตารางนี้ เพื่อใช้คำนวณ %
        total_query = "SELECT COUNT(*) FROM activity_log WHERE CONCAT(action, ':', target) = %s"
        cursor.execute(total_query, (current_action,))
        total_imports = cursor.fetchone()[0] or 0

        cursor.close()
        conn.close()

        if row:
            next_action, freq = row
            confidence = (freq / total_imports * 100) if total_imports > 0 else 0
            return next_action, freq, confidence
    except Exception as e:
        st.warning(f"⚠️ AI suggestion failed: {e}")
    return None, None, 0


# ===== TAB 2: RUN PROCEDURES (with event flags) =====
def render_procedures_tab():
    st.header("⚙️ Database Procedures & Updates")

    # ====== Enable / Disable Tab ======
    enabled = st.toggle("Enable this tab (load from DB)", value=False,
                        help="Turn on only when you want to work with procedures")
    if not enabled:
        st.info("This tab is idle. Turn on the toggle to load procedures.")
        return

    if 'db_manager' not in st.session_state:
        st.session_state.db_manager = DatabaseManager()

    # ====== SEARCH / LOAD ======
    st.subheader("🔎 Search / Load Procedures")

    # ✅ ใช้ form เพื่อให้ Enter trigger การ submit
    with st.form(key="proc_search_form", clear_on_submit=False):
        col_a, col_b = st.columns([3, 1])
        with col_a:
            name_filter = st.text_input(
                "Procedure name",
                value=st.session_state.get('last_proc_filter', ""),
                placeholder="พิมพ์ชื่อ procedure แล้วกด Enter เพื่อค้นหา"
            )

        # ✅ ทำให้ปุ่มอยู่แนวเดียวกับ textbox
        with col_b:
            st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)  # ดันปุ่มขึ้น
            do_load = st.form_submit_button("📥 Load", type="primary", use_container_width=True)
 

    # ====== LOAD ======
    if do_load:
        pattern = name_filter or "%"
        procs = get_stored_procedures(pattern, 200)  # ✅ fix limit to 200
        st.session_state.loaded_procedures = procs
        st.session_state['last_proc_filter'] = name_filter
        if procs:
            st.success(f"Loaded {len(procs)} procedure(s)")
        else:
            st.warning("No procedures matched your filter.")

    # ====== DISPLAY ======
    procedures = st.session_state.get("loaded_procedures", [])
    st.divider()

    # ====== SHOW PROCEDURES ======
    st.subheader("🔧 Stored Procedures")
    if not procedures:
        st.warning("⚠️ No procedures loaded. ใส่ชื่อแล้วกด Load ก่อน")
        return

    # ✅ เก็บ procedure ที่กำลังเปิดอยู่ (เพื่อคงสถานะเปิด)
    if 'expanded_proc' not in st.session_state:
        st.session_state['expanded_proc'] = None

    # ✅ แสดงผลรายการ procedure ที่โหลดมา
    for proc in procedures:
        proc_name = proc['ROUTINE_NAME']
        expanded = st.session_state['expanded_proc'] == proc_name

        with st.expander(f"📦 {proc_name}", expanded=expanded):

            # ⚠️ ข้อความเตือนพิเศษ (คง logic เดิม)
            if proc_name == "update_Broadband_daily":
                st.markdown(
                    "<span style='color:red;font-weight:bold;'>⚠️ ก่อน Run ให้ Import ข้อมูล Ticket ทั้ง TTS และ SCOMS ลง Broadband_daily ก่อน</span>",
                    unsafe_allow_html=True
                )

            # ===== AUTH SECTION =====
            st.markdown("#### 🔑 Authorization")
            key_col, status_col = st.columns([2, 1])
            with key_col:
                local_key = st.text_input(
                    f"Enter Secret Key (for execute permission)",
                    type="password",
                    placeholder="Enter key...",
                    key=f"key_{proc_name}",
                    on_change=lambda name=proc_name: st.session_state.update(expanded_proc=name)
                ).strip()


            with status_col:
                # ✅ จัดตำแหน่งให้อยู่กึ่งกลางแนวเดียวกับ textbox
                st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
            
                user_perm = get_user_permission(local_key) if local_key else None
                if not user_perm:
                    st.info("👁 Guest mode — execute locked")
                    execute_disabled = True
                    role = "Guest"
                else:
                    role = user_perm["role"]
                    allowed_procs = user_perm.get("allowed_procedures", [])
                    if role == "Admin" or proc_name in allowed_procs:
                        st.success(f"✅ Authorized as **{role}**")
                        execute_disabled = False
                    else:
                        st.error(f"🚫 Not allowed to execute `{proc_name}`")
                        execute_disabled = True
 
 

            # ===== EXECUTE BUTTON =====
            exec_col, note_col = st.columns([1, 3])
            with exec_col:
                # ✅ ตรวจสอบสถานะว่ากำลังรันอยู่หรือไม่
                is_running = st.session_state.get("proc_running", False)
                btn_label = "⏳ Running..." if is_running else "▶️ Execute"
            
                # ✅ ปุ่มจะถูก disable ถ้ากำลังรัน
                execute_disabled_final = execute_disabled or is_running
            
                if st.button(
                    btn_label,
                    key=f"exec_{proc_name}",
                    type="primary",
                    use_container_width=True,
                    disabled=execute_disabled_final,
                ):
                    # ✅ ตั้งสถานะกำลังรัน (จะอยู่จนกว่ารันเสร็จ)
                    st.session_state['proc_running'] = True
                    st.session_state['expanded_proc'] = proc_name  # คง panel เปิด
            
                    try:
                        db = st.session_state.get("db_manager") or DatabaseManager()
                        conn = db.get_connection()
                        cursor = conn.cursor()
                        cursor.execute(
                            """
                            INSERT INTO activity_log (username, action, target, ip_address, details)
                            VALUES (%s, %s, %s, %s, %s)
                            """,
                            (
                                local_key,
                                "Execute Procedure",
                                proc_name,
                                st.session_state.get("client_ip", "unknown"),
                                "{}",
                            ),
                        )
                        conn.commit()
                        cursor.close()
                        conn.close()
                    except Exception as log_err:
                        st.warning(f"⚠️ Failed to write log: {log_err}")
            
                    # ✅ เริ่ม event run
                    st.session_state["PROC_RUN_EVENT"] = {
                        "name": proc_name,
                        "params": None,
                    }
            
            with note_col:
                if st.session_state.get("proc_running"):
                    st.markdown(
                        "<span style='color:#0288d1;font-weight:bold;'>⏳ Procedure is running... กรุณารอจนกว่าจะเสร็จสิ้น</span>",
                        unsafe_allow_html=True,
                    )
                else:
                    st.caption("Only authorized users can execute this procedure.")
            
           
         
            # ===== EVENT HANDLING =====
            event_run = st.session_state.get('PROC_RUN_EVENT')
            if event_run and event_run.get('name') == proc_name:
                # ✅ ล็อกปุ่มไว้ตลอดระหว่าง run
                st.session_state['proc_running'] = True
                st.session_state['proc_progress_value'] = 20
            
                # ✅ สร้าง unique result key
                result_key = f"proc_result_{proc_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                
                # รัน procedure
                result = execute_procedure_with_progress(event_run['name'], event_run.get('params'))
                
                # เก็บผลลัพธ์
                st.session_state[result_key] = result
                st.session_state[f'latest_result_{proc_name}'] = result_key
                
                # ✅ ปลดล็อกปุ่มหลังรันเสร็จ
                st.session_state['proc_running'] = False
                st.session_state['PROC_RUN_EVENT'] = None
            
            # ✅ แสดงผลลัพธ์ล่าสุด (ถ้ามี)
            latest_key = st.session_state.get(f'latest_result_{proc_name}')
            if latest_key and latest_key in st.session_state:
                render_exec_result(proc_name, st.session_state[latest_key])
     
    # ===== RIGHT: STATS =====
    st.divider()
    st.subheader("📊 Quick Stats")
    
    if procedures:
        st.metric("Total Procedures (loaded)", len(procedures))
    
    if st.session_state.execution_history:
        success_count = sum(1 for h in st.session_state.execution_history if h['status'] == 'success')
        failed_count = len(st.session_state.execution_history) - success_count
        st.metric("Executions", len(st.session_state.execution_history))
    
        cols = st.columns(2)
        with cols[0]:
            st.metric("✅ Success", success_count)
        with cols[1]:
            st.metric("❌ Failed", failed_count)
    
    st.divider()
    
    if st.button("🧹 Clear History", use_container_width=True):
        st.session_state.execution_history = []
        st.rerun()
    
    if st.button("🗂️ Clear Cache (procedures)", use_container_width=True):
        get_stored_procedures.clear()
        st.session_state.loaded_procedures = []
        st.toast("Cleared cached procedures & session list")

# ===== TAB 3: FILE MERGER =====
 
def render_merger_tab():
    st.header("📁 File Merger")
    st.write("รวมไฟล์ CSV และ Excel หลายไฟล์เข้าด้วยกัน")
    
    # ===== Session State Initialization =====
    if 'merger' not in st.session_state:
        st.session_state.merger = FileMerger()
    if 'merger_processed_data' not in st.session_state:
        st.session_state.merger_processed_data = {}
    if 'merger_merged_df' not in st.session_state:
        st.session_state.merger_merged_df = None
    if 'merger_selected_files' not in st.session_state:
        st.session_state.merger_selected_files = {}
    # ⭐ NEW: เพิ่ม session state สำหรับโหมดรวม sheet
    if 'merger_sheet_mode' not in st.session_state:
        st.session_state.merger_sheet_mode = {}  # {filename: 'single' | 'all'}
    if 'merger_uploaded_files_cache' not in st.session_state:
        st.session_state.merger_uploaded_files_cache = {}  # เก็บ uploaded files
    
    merger = st.session_state.merger

    # ===== File Upload Section =====
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
                # ⭐ NEW: เก็บ uploaded files ไว้ใช้งานภายหลัง
                st.session_state.merger_uploaded_files_cache = {f.name: f for f in uploaded_files}

    # ===== File Selection Section =====
    if st.session_state.merger_processed_data:
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

        # ===== File Details Section =====
        st.subheader("📋 ไฟล์ที่อัปโหลด")
        cols = st.columns([2, 1])

        with cols[0]:
            selected_sheets = {}
        
            for idx, (filename, file_info) in enumerate(st.session_state.merger_processed_data.items(), start=1):
                is_selected = st.session_state.merger_selected_files.get(filename, True)
        
                expander_title = f"{'✅' if is_selected else '❌'} ไฟล์ #{idx}: {filename}"
        
                with st.expander(expander_title, expanded=False):
        
                    col_info, col_sheet = st.columns([2, 1])
        
                    # ===== File Info Column =====
                    with col_info:
                        st.markdown(
                            f"**ขนาด:** {file_info['size']/1024:.2f} KB"
                            f"  \n**ประเภท:** {file_info['type'].upper()}"
                            f"  \n**จำนวน Sheets:** {len(file_info['sheets'])}"
                        )
                        if 'succeeded_encoding' in file_info:
                            st.caption(f"Encoding: {file_info.get('succeeded_encoding','auto')}")
        
                    # ===== Sheet Selection Column (⭐ MODIFIED) =====
                    with col_sheet:
                        if len(file_info['sheets']) > 1:
                            # ⭐ NEW: เพิ่มตัวเลือกโหมดรวม sheet
                            merge_mode = st.radio(
                                "โหมดการเลือก:",
                                ["📄 เลือก 1 Sheet", "📚 รวมทุก Sheet"],
                                key=f"merger_mode_{filename}",
                                disabled=not is_selected,
                                horizontal=True
                            )
                            
                            # บันทึกโหมดใน session
                            st.session_state.merger_sheet_mode[filename] = 'all' if merge_mode == "📚 รวมทุก Sheet" else 'single'
                            
                            if merge_mode == "📄 เลือก 1 Sheet":
                                # โหมดเดิม: เลือก 1 sheet
                                selected_sheet = st.selectbox(
                                    "เลือก Sheet:",
                                    file_info['sheets'],
                                    key=f"merger_sheet_{filename}",
                                    disabled=not is_selected
                                )
                                selected_sheets[filename] = selected_sheet
                            else:
                                # ⭐ NEW: โหมดรวมทุก sheet
                                selected_sheets[filename] = "ALL_SHEETS"
                                st.info(f"✅ จะรวม {len(file_info['sheets'])} sheets")
                        else:
                            # ไฟล์มี 1 sheet เท่านั้น
                            selected_sheets[filename] = file_info['sheets'][0]
                            st.session_state.merger_sheet_mode[filename] = 'single'
                            st.info(f"Sheet: {file_info['sheets'][0]}")
        
                    # ===== Preview Section (⭐ MODIFIED) =====
                    if is_selected:
                        sheet_name = selected_sheets[filename]
                        
                        # ⭐ NEW: กรณีรวมทุก sheet
                        if sheet_name == "ALL_SHEETS":
                            total_rows = sum(len(file_info['data'][s]) for s in file_info['sheets'] if s in file_info['data'])
                            total_cols = len(file_info['data'][file_info['sheets'][0]].columns) if file_info['sheets'] and file_info['sheets'][0] in file_info['data'] else 0
                            
                            st.write(f"**Preview (รวม {len(file_info['sheets'])} sheets, {total_rows:,} แถว, {total_cols} คอลัมน์):**")
                            
                            # แสดงตัวอย่างแต่ละ sheet
                            for sheet in file_info['sheets'][:3]:  # แสดงแค่ 3 sheets แรก
                                if sheet in file_info['data']:
                                    df = file_info['data'][sheet]
                                    st.caption(f"📄 {sheet}: {len(df):,} แถว")
                                    st.dataframe(df.head(2), use_container_width=True)
                            
                            if len(file_info['sheets']) > 3:
                                st.caption(f"... และอีก {len(file_info['sheets']) - 3} sheets")
                        
                        # กรณีเลือก 1 sheet (เดิม)
                        elif sheet_name in file_info['data']:
                            df = file_info['data'][sheet_name]
                            st.write(f"**Preview ({len(df):,} แถว, {len(df.columns)} คอลัมน์):**")
                            st.dataframe(df.head(5), use_container_width=True)

        # ===== Statistics Section (⭐ MODIFIED) =====
        with cols[1]:
            selected_files_data = {
                k: v for k, v in st.session_state.merger_processed_data.items() 
                if st.session_state.merger_selected_files.get(k, True)
            }
            
            total_files = len(selected_files_data)
            
            # ⭐ MODIFIED: คำนวณแถวรวม รองรับโหมดรวม sheet
            total_records = 0
            total_sheets = 0
            
            for filename, file_info in selected_files_data.items():
                sheet_name = selected_sheets.get(filename, file_info['sheets'][0])
                
                if sheet_name == "ALL_SHEETS":
                    # ⭐ NEW: นับทุก sheet
                    for s in file_info['sheets']:
                        if s in file_info['data']:
                            total_records += len(file_info['data'][s])
                            total_sheets += 1
                elif sheet_name in file_info['data']:
                    # นับ 1 sheet (เดิม)
                    total_records += len(file_info['data'][sheet_name])
                    total_sheets += 1
            
            st.markdown(
                f"""<div class="metric-card">
                <h3>📊 สถิติ</h3>
                <p><strong>ไฟล์ที่เลือก:</strong> {total_files}</p>
                <p><strong>Sheets รวม:</strong> {total_sheets}</p>
                <p><strong>จำนวนแถวรวม:</strong> {total_records:,}</p>
                </div>""", 
                unsafe_allow_html=True
            )

        # ===== Header Analysis Section =====
        st.header("🔍 การวิเคราะห์ Headers")
        all_headers, has_mismatch, file_headers = merger.analyze_headers(
            st.session_state.merger_processed_data, 
            selected_sheets, 
            st.session_state.merger_selected_files
        )
        
        if has_mismatch and len(file_headers) > 1:
            st.warning("⚠️ พบความไม่สอดคล้องของ Headers")

            for filename, headers in file_headers.items():
                with st.expander(f"Headers ของ {filename}"):
                    st.write(f"**จำนวน:** {len(headers)} headers")
                    st.write(", ".join(map(str, headers)))
      
            st.info("💡 คุณสามารถรวมไฟล์ได้ทันที Headers ที่ไม่ตรงกันจะเป็นค่าว่าง")
        elif len(file_headers) > 1:
            st.success("✅ Headers ทั้งหมดสอดคล้องกัน")

        # ===== Merge Files Section =====
        st.header("⚙️ การรวมไฟล์")
        if st.button("🚀 เริ่มรวมไฟล์", type="primary", use_container_width=True, key="merge_files_btn"):
            with st.spinner("กำลังรวมไฟล์..."):
                merged_df = merger.merge_files(
                    st.session_state.merger_processed_data, 
                    selected_sheets, 
                    st.session_state.merger_selected_files,
                    # ⭐ NEW: ส่ง sheet_mode และ uploaded_files_cache เข้าไป
                    sheet_mode=st.session_state.merger_sheet_mode,
                    uploaded_files_cache=st.session_state.merger_uploaded_files_cache
                )
                st.session_state.merger_merged_df = merged_df
                st.success(f"✅ รวมไฟล์สำเร็จ! {len(merged_df):,} แถว")

        # ===== Duplicate Analysis Function =====
        def analyze_duplicates(df: pd.DataFrame):
            if df.empty:
                return pd.DataFrame(), 0
            dup_mask = df.duplicated(keep=False)
            dup_df = df[dup_mask].copy()
            return dup_df, dup_mask.sum()
        
        # ===== Results Section =====
        if st.session_state.merger_merged_df is not None:
            st.header("📊 ผลลัพธ์การรวมไฟล์")
        
            merged_df = st.session_state.merger_merged_df.copy()
        
            # วิเคราะห์ข้อมูลซ้ำ
            dup_df, dup_count = analyze_duplicates(merged_df)
        
            if dup_count > 0:
                st.warning(f"⚠️ พบข้อมูลซ้ำ {dup_count:,} แถว จากทั้งหมด {len(merged_df):,}")
                with st.expander("🔍 ดูตัวอย่างข้อมูลซ้ำ"):
                    st.dataframe(dup_df.head(10), use_container_width=True)
        
                action = st.radio(
                    "ต้องการจัดการข้อมูลซ้ำอย่างไร?",
                    ["❌ ลบข้อมูลซ้ำ", "➡️ ข้าม (คงไว้ทั้งหมด)"],
                    horizontal=True,
                    key="dup_action"
                )
        
                if action == "❌ ลบข้อมูลซ้ำ":
                    merged_df = merged_df.drop_duplicates(keep="first").reset_index(drop=True)
                    st.success(f"✅ ลบข้อมูลซ้ำแล้ว เหลือ {len(merged_df):,} แถว")
                else:
                    st.info("📎 เก็บข้อมูลทั้งหมดไว้โดยไม่ลบซ้ำ")
        
                # Highlight duplicates
                if action == "➡️ ข้าม (คงไว้ทั้งหมด)":
                    dup_mask = merged_df.duplicated(keep=False)
        
                    def highlight_duplicates(row):
                        idx = row.name
                        return ['background-color: #ffe6e6' if dup_mask.iloc[idx] else '' for _ in row]
        
                    st.dataframe(
                        merged_df.style.apply(highlight_duplicates, axis=1),
                        use_container_width=True
                    )
                else:
                    st.dataframe(merged_df.head(100), use_container_width=True)
            else:
                st.success("✅ ไม่พบข้อมูลซ้ำ")
                st.dataframe(merged_df.head(100), use_container_width=True)
        
            # บันทึกกลับเข้า session
            st.session_state.merger_merged_df = merged_df

            # ===== Download Section =====
            st.header("⬇️ ดาวน์โหลด")
            d1, d2 = st.columns([1, 2])
            
            with d1:
                download_format = st.radio(
                    "เลือกรูปแบบไฟล์:", 
                    options=["CSV", "Excel (XLSX)"], 
                    index=0, 
                    key="download_format"
                )
            
            with d2:
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
                    output = BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        merged_df.to_excel(writer, index=False, sheet_name='Merged Data')
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

import re
import time
from datetime import datetime
import streamlit as st
import pandas as pd
import mysql.connector


 
def render_data_editor_tab():
    from datetime import datetime
    import re
    
    # === DATABASE CONNECTION ===
    if 'db_manager' not in st.session_state:
        st.session_state.db_manager = DatabaseManager()
    db = st.session_state.db_manager

    # === TABLE SELECTION PANEL ===
    st.markdown("### 📂 Select Target Table")
    try:
        tables_info = get_cached_tables_info()
        tables = [t['TABLE_NAME'] for t in tables_info] if tables_info else []

        HIDDEN_TABLES = ["user_permissions", "sn"]
        tables = [t for t in tables if t not in HIDDEN_TABLES]
    except Exception as e:
        st.error(f"Cannot get tables: {e}")
        tables = []

    selected_table = st.selectbox("Select a table to view/edit", [""] + tables, key="table_selector")
    if not selected_table:
        st.info("👆 Please select a table to start.")
        return

    columns = [col['COLUMN_NAME'] for col in get_cached_table_columns(selected_table)]
    columns_lower = [c.lower() for c in columns]

    st.markdown("---")
    left, right = st.columns([1.2, 3])

    # ==========================================
    # 🔍 LEFT: SEARCH PANEL
    # ==========================================
    with left:
        st.markdown("#### 🔍 Smart Search")
        
        search_input = st.text_input(
            "Enter keywords or conditions",
            placeholder="เช่น service_type=FTTx , mm=สิงหาคม2025",
            key="view_search_input"
        )
        
        # ===== FILTER เฉพาะ TABLE Asset =====
        if selected_table == "Asset":
            st.markdown("#### 📅 Filter by Month / Year")
            
            try:
                minmax = db.execute_query("""
                    SELECT 
                        MIN(CAST(year AS UNSIGNED)) AS min_year,
                        MAX(CAST(year AS UNSIGNED)) AS max_year,
                        MIN(CAST(month AS UNSIGNED)) AS min_month,
                        MAX(CAST(month AS UNSIGNED)) AS max_month
                    FROM Asset
                    WHERE year REGEXP '^[0-9]+$' AND month REGEXP '^[0-9]+$';
                """)
            except:
                minmax = None
            
            if minmax is not None and not minmax.empty:
                row = minmax.iloc[0]
                min_year = int(row["min_year"] or 2020)
                max_year = int(row["max_year"] or min_year)
                min_month = int(row["min_month"] or 1)
                max_month = int(row["max_month"] or 12)
            else:
                min_year, max_year = 2020, 2025
                min_month, max_month = 1, 12
            
            col_m, col_y = st.columns(2)
            with col_m:
                asset_month = st.selectbox(
                    "Month",
                    options=list(range(1, 13)),
                    index=max_month - 1,
                    key="asset_month"
                )
            with col_y:
                asset_year = st.selectbox(
                    "Year",
                    options=list(range(min_year, max_year + 1)),
                    index=list(range(min_year, max_year + 1)).index(max_year),
                    key="asset_year"
                )
            
            # Filter ดำเนินการ (Status)
            try:
                status_rows = db.execute_query("""
                    SELECT DISTINCT `ดำเนินการ` AS status_value
                    FROM Asset
                    WHERE `ดำเนินการ` IS NOT NULL 
                      AND `ดำเนินการ` <> ''
                    ORDER BY `ดำเนินการ`
                """)
                
                if status_rows is not None and not status_rows.empty and len(status_rows) > 0:
                    status_options = ["All"] + status_rows["status_value"].tolist()
                else:
                    status_options = ["All"]
                    
            except Exception as e:
                st.warning(f"ไม่สามารถดึงข้อมูล Status: {e}")
                status_options = ["All"]
            
            asset_status = st.selectbox(
                "Filter by Status (ดำเนินการ)",
                options=status_options,
                index=0,
                key="asset_status_filter"
            )
        else:
            # ถ้าไม่ใช่ table Asset ให้ set ค่า default
            asset_status = "All"
            asset_month = None
            asset_year = None
        
        # Match Mode
        match_mode = st.radio("Match Mode", ["AND", "OR"], horizontal=True, index=1)
        
        row_limit_label = st.selectbox("Show rows", ["10", "50", "100", "500", "1000", "All"], index=0)
        row_limit = None if row_limit_label == "All" else int(row_limit_label)
        
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    # ==========================================
    # 📊 RIGHT: SQL QUERY BUILD & RESULTS
    # ==========================================
    with right:
        
        # ---- Build SQL ----
        query = f"SELECT * FROM `{selected_table}`"
        params = []
        
        # ======================================================
        # CASE 1: มี search input
        # ======================================================
        if search_input.strip():
            
            parts = [p.strip() for p in re.split('[,;]', search_input) if p.strip()]
            has_explicit = any('=' in p for p in parts)
            
            # --------------------------------------------------
            # CASE 1A: explicit condition เช่น month=5, type=FTTx
            # --------------------------------------------------
            if has_explicit:
                conditions = []
                joiner = f" {match_mode} "
                
                for cond in parts:
                    if '=' not in cond:
                        continue
                    key_, value_ = [x.strip() for x in cond.split("=", 1)]
                    
                    if key_.lower() in columns_lower:
                        col_real = columns[columns_lower.index(key_.lower())]
                        conditions.append(f"`{col_real}` LIKE %s")
                        params.append(f"%{value_}%")
                
                # เพิ่ม Asset month/year
                if selected_table == "Asset":
                    conditions.append("`month` LIKE %s")
                    params.append(f"%{asset_month}%")
                    
                    conditions.append("`year` LIKE %s")
                    params.append(f"%{asset_year}%")
                    
                    # Filter by ดำเนินการ (เฉพาะ explicit)
                    if asset_status != "All":
                        conditions.append("`ดำเนินการ` = %s")
                        params.append(asset_status)
                
                if conditions:
                    query += " WHERE " + joiner.join(conditions)
            
            # --------------------------------------------------
            # CASE 1B: plain keyword search เช่น "ขาย"
            # --------------------------------------------------
            else:
                like_clauses = f" {match_mode} ".join([f"`{col}` LIKE %s" for col in columns])
                query += f" WHERE ({like_clauses})"
                params = [f"%{search_input}%"] * len(columns)
                
                if selected_table == "Asset":
                    query += " AND `month` LIKE %s AND `year` LIKE %s"
                    params.append(f"%{asset_month}%")
                    params.append(f"%{asset_year}%")
                    
                    # Filter by ดำเนินการ (plain search)
                    if asset_status != "All":
                        query += " AND `ดำเนินการ` = %s"
                        params.append(asset_status)
        
        # ======================================================
        # CASE 2: ไม่มี search input → default filter
        # ======================================================
        else:
            if selected_table == "Asset":
                query += " WHERE `month` LIKE %s AND `year` LIKE %s"
                params = [f"%{asset_month}%", f"%{asset_year}%"]
                
                # Filter by ดำเนินการ (no search input)
                if asset_status != "All":
                    query += " AND `ดำเนินการ` = %s"
                    params.append(asset_status)
        
        # limit rows
        if row_limit:
            query += f" LIMIT {row_limit}"
        
        # ============
        # SHOW SQL
        # ============
        with st.expander("🧠 SQL Query Used", expanded=False):
            formatted = query
            for p in params:
                formatted = formatted.replace("%s", f"'{p}'", 1)
            st.code(formatted, language="sql")
        
        # ============
        # EXECUTE SQL
        # ============
        with st.spinner("🔎 Searching database..."):
            try:
                df = db.execute_query(query, tuple(params))
                df = df.astype(str)
            except Exception as e:
                st.error(f"Query error: {e}")
                return
        
        st.success(f"✅ Found {len(df)} records from `{selected_table}`")

    # ⭐ ปิด column layout - จากนี้ไปจะแสดงเต็มหน้าจอ
    st.markdown("---")
    
    # ==========================================
    # 🔐 Authorization
    # ==========================================
    st.markdown("#### 🔐 Authorization (optional)")
    
    secret_key = st.text_input(
        "Enter Secret Key (optional)",
        type="password",
        placeholder="Enter your key for edit permission",
        key="auth_key_editor"
    )
    
    user_perm = get_user_permission(secret_key) if secret_key.strip() else None
    if not user_perm:
        st.error("🚫 Access denied.")
        username, user_role, is_authorized, can_edit = "Guest", "Guest", False, False 
    else:
        username = secret_key.strip()
        user_role = user_perm["role"]
        is_authorized = True
        allowed_edit = user_perm.get("allowed_edit_tables", [])
        if user_role == "Admin" or selected_table in allowed_edit:
            st.success(f"✅ Authorized as {user_role} (Edit Enabled)")
            can_edit = True
        else:
            st.warning(f"🚫 You can view but not edit `{selected_table}`.")
            can_edit = False
    
    # --- ควบคุมสิทธิ์การแก้ไข ---

    if not is_authorized:
        # st.error("🚫 คุณไม่มีสิทธิ์เข้าถึงข้อมูล")
        return   # ⛔ หยุดทำงาน ไม่แสดง DataFrame เลย
    else:
        display_df = df
 
    
    # --- Editor (แสดงเต็มหน้าจอ) ---
    st.markdown("### 🧮 Data Viewer & Editor")
    
    if display_df is not None and not display_df.empty:
        record_count = len(display_df)
        st.caption(f"📊 **Total records:** {record_count:,} รายการ")
    else:
        st.caption("⚠️ No data available to display.")
    
    edited_df = st.data_editor(
        display_df,
        num_rows="dynamic",
        use_container_width=True,
        key="data_editor_panel",
        hide_index=True,
        disabled=not can_edit
    )
    
    # ==========================================
    # 💾 Detect Changes (only if authorized)
    # ==========================================


    if edited_df is not None and not edited_df.equals(display_df):
        if not is_authorized:
            st.warning("🔒 Editing disabled — enter valid key for edit privileges.", icon="🔑")
        else:
            st.info("📝 Detected unsaved changes!")
            
            pk_col = next((c for c in ['id', 'ID', 'Ticketno', 'Ticket No', 'ticket_no', 'no', 'No'] if c in columns), None)
            if not pk_col:
                st.error("⚠️ Cannot find primary key column.")
                return
            
            update_queries, update_params, affected_keys = [], [], []
            for i, row in edited_df.iterrows():
                if i < len(display_df) and not row.equals(display_df.iloc[i]):
                    # ====== SET clause เหมือนเดิม ======
                    set_clause = ", ".join([f"`{c}`=%s" for c in columns if c != pk_col])
    
                    # ====== NEW: WHERE ให้สอดคล้องกับ SELECT ======
                    where_clause = f"`{pk_col}`=%s"
                    where_values = [row[pk_col]]
    
                    # ✅ กรณี TABLE Asset ให้ตามเงื่อนไข month/year/ดำเนินการ แบบเดียวกับตอน SELECT
                    if selected_table == "Asset":
                        # ตอน SELECT ใช้ month/year แบบ LIKE
                        where_clause += " AND `month` LIKE %s AND `year` LIKE %s"
                        where_values.append(f"%{asset_month}%")
                        where_values.append(f"%{asset_year}%")
    
                        # ถ้ามี filter ตามสถานะการดำเนินการ
                        if asset_status != "All":
                            where_clause += " AND `ดำเนินการ` = %s"
                            where_values.append(asset_status)
    
                    # ประกอบเป็น UPDATE สมบูรณ์
                    update_query = f"UPDATE `{selected_table}` SET {set_clause} WHERE {where_clause}"
                    vals = [row[c] for c in columns if c != pk_col] + where_values
    
                    update_queries.append(update_query)
                    update_params.append(vals)
                    affected_keys.append(row[pk_col])
            
            # ✅ SQL Preview ก่อนบันทึก (เหมือนเดิม)
            with st.expander("🧩 SQL Query (before saving)", expanded=True):
                for i, q in enumerate(update_queries):
                    formatted_sql = q.replace("%s", "'{}'").format(*[str(v) for v in update_params[i]])
                    st.code(formatted_sql, language="sql")
            
            confirm = st.checkbox("✅ Confirm update queries before saving", key="confirm_update") 
            
            if st.button("💾 Save Changes", type="primary", use_container_width=True, disabled=not confirm):
                try:
                    with st.spinner("💾 Applying changes..."):
                        conn = db.get_connection()
                        cursor = conn.cursor()
                        for q, vals in zip(update_queries, update_params):
                            cursor.execute(q, vals)
                        conn.commit()
                        cursor.close()
                        conn.close()
                    
                    # ✅ Log Activity
                    try:
                        log_conn = db.get_connection()
                        log_cursor = log_conn.cursor()
                        
                        executed_sql = "\n".join([
                            q.replace("%s", "'{}'").format(*[str(v) for v in vals])
                            for q, vals in zip(update_queries, update_params)
                        ])
                        if len(executed_sql) > 2000:
                            executed_sql = executed_sql[:2000] + " ... (truncated)"
                        
                        details_text = f"rows={len(affected_keys)}\n{executed_sql}"
                        
                        log_cursor.execute("""
                            INSERT INTO activity_log (username, action, target, ip_address, details)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (
                            username,
                            "Edit Data",
                            selected_table,
                            st.session_state.get('client_ip', 'unknown'),
                            details_text
                        ))
                        
                        log_conn.commit()
                        log_cursor.close()
                        log_conn.close()
                    except Exception as log_err:
                        st.warning(f"⚠️ Failed to write log: {log_err}")
                    
                    st.success("✅ Data updated successfully.")
                    st.toast("💾 Changes saved!", icon="✅")
                    
                    # เงื่อนไขเฉพาะกรณี table LK_Broadband_daily
                    if selected_table == "LK_Broadband_daily":
                        st.markdown("""
                        <div style="margin-top:10px; padding:10px; border-left:4px solid #f39c12; background-color:#fffbea;">
                            ⚠️ <b>กรุณารีเฟรชข้อมูลที่ Looker Studio</b><br>
                            👉 <a href="https://lookerstudio.google.com/reporting/1483b6e3-3477-4906-8966-ec276423ec27" target="_blank" style="color:#0073e6; text-decoration:none;">
                            เปิดลิงก์เพื่อรีเฟรชข้อมูลใน Dashboard</a>
                        </div>
                        """, unsafe_allow_html=True)
                
                except Exception as e:
                    st.error(f"❌ Update failed: {e}")
    
    # ==========================================
    # 📊 Data Display & Download
    # ==========================================
    st.markdown("---")
    st.caption("💡 Use the built-in download icon on top-right to export the visible data.")
    
    # ✅ Log full access (เฉพาะ authorized)
    if is_authorized and secret_key.strip():
        try:
            conn = db.get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO activity_log (username, action, target, ip_address, details)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                username,
                "View Full Data",
                selected_table,
                st.session_state.get('client_ip', 'unknown'),
                f"rows={len(df)}"
            ))
            conn.commit()
            cursor.close()
            conn.close()
            st.toast("📜 Logged: Full view access", icon="✅")
        except Exception as e:
            st.warning(f"⚠️ Log failed: {e}")
    
    # ==========================================
    # 📅 Footer
    # ==========================================
    st.markdown("---")
    st.caption(f"📅 Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
###########################################end 


def render_log_tab():
    st.header("📜 Activity Log")

    db = st.session_state.db_manager

    # ---- Filter Controls ----
    with st.expander("🔍 Filter Logs", expanded=False):
        cols = st.columns(4)
        with cols[0]:
            search_action = st.text_input("Action", placeholder="เช่น Import Data, Edit Data")
        with cols[1]:
            search_target = st.text_input("Target", placeholder="เช่น datacomNT, LK_Ticket")
        with cols[2]:
            search_user = st.text_input("Username", placeholder="ชื่อหรือบางส่วน")
        with cols[3]:
            limit_per_page = st.selectbox("Rows per page", [50, 100, 200, 500], index=1)

    # ---- Build SQL dynamically ----
    where_clauses = []
    params = []
    if search_action:
        where_clauses.append("action LIKE %s")
        params.append(f"%{search_action}%")
    if search_target:
        where_clauses.append("target LIKE %s")
        params.append(f"%{search_target}%")
    if search_user:
        where_clauses.append("username LIKE %s")
        params.append(f"%{search_user}%")

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    # ---- Count total rows ----
    count_query = f"SELECT COUNT(*) as total FROM activity_log {where_sql}"
    count_df = db.execute_query(count_query, tuple(params))
    total_rows = int(count_df.iloc[0]['total']) if (count_df is not None and not count_df.empty) else 0

    # ---- Pagination ----
    page_count = max(1, (total_rows + limit_per_page - 1) // limit_per_page)
    current_page = st.number_input("📄 Page", min_value=1, max_value=page_count, step=1, value=1)
    offset = (current_page - 1) * limit_per_page

    # ---- Query data ----
    query = f"""
        SELECT * FROM activity_log
        {where_sql}
        ORDER BY timestamp DESC
        LIMIT %s OFFSET %s
    """
    df = db.execute_query(query, tuple(params + [limit_per_page, offset]))

    # ---- Username masking ----

    def mask_username(name: str):
        if not name or not isinstance(name, str):
            return ""
    
        # ถ้าตัวอักษรเดียว เช่น "A"
        if len(name) == 1:
            return "*" * 6
    
        # ถ้าตั้งแต่ 2 ตัวขึ้นไป เช่น "AB", "Alex", "1177"
        return name[0] + "*" * 6 + name[-1]

 

    if df is not None and not df.empty:
        df = df.copy()

        # ✅ Mask username
        if "username" in df.columns:
            df["username"] = df["username"].apply(mask_username)

        # ✅ Hide ID column if exists
        if "id" in df.columns:
            df = df.drop(columns=["id"])

        # ✅ Format timestamp (optional)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")

        # ---- Display Data ----
        try:
            # ✅ ใช้ได้ใน Streamlit ≥ 1.36
            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True,
                hide_download_button=True  # ปิดปุ่มดาวน์โหลดอัตโนมัติ
            )
        except TypeError:
            # ✅ รองรับ Streamlit < 1.36 (ไม่มี argument นี้)
            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True
            )
        
        # ✅ ซ่อนปุ่มดาวน์โหลดด้วย CSS (backup)
        # ---- หลังจาก st.dataframe(df, ...) แล้ว ใส่ CSS นี้ ----
        st.markdown("""
        <style>
        /* Streamlit DataFrame / Data Editor download buttons */
        [data-testid="stElementToolbar"] button[aria-label*="Download"] {
            display: none !important;
        }
        button[title="Download data as CSV"],
        button[aria-label="Download data as CSV"],
        button[aria-label="Download"] {
            display: none !important;
        }
        </style>
        """, unsafe_allow_html=True)



        # ---- Navigation Buttons ----
        c1, c2, c3 = st.columns([1, 1, 6])
        with c1:
            if st.button("⬅️ Previous", disabled=(current_page <= 1)):
                st.session_state["log_page"] = current_page - 1
                st.experimental_rerun()
        with c2:
            if st.button("➡️ Next", disabled=(current_page >= page_count)):
                st.session_state["log_page"] = current_page + 1
                st.experimental_rerun()

    else:
        st.info("📭 No activity logs found.")

  
    # ---- Optional: summary chart ----
    import altair as alt
    
    with st.expander("📊 Log Summary Chart", expanded=False):
        try:
            agg_query = """
                SELECT DATE(timestamp) AS date, COUNT(*) AS count
                FROM activity_log
                GROUP BY DATE(timestamp)
                ORDER BY date ASC
                LIMIT 14
            """
            agg_df = db.execute_query(agg_query)
    
            if agg_df is not None and not agg_df.empty:
                # แปลงให้แน่ใจว่า date เป็น datetime
                agg_df["date"] = pd.to_datetime(agg_df["date"])
    
                chart = (
                    alt.Chart(agg_df)
                    .mark_bar(size=25, color="#2563eb")  # ✅ ขนาดแท่ง + สี
                    .encode(
                        x=alt.X("date:T", title="วันที่", axis=alt.Axis(labelAngle=-45)),
                        y=alt.Y("count:Q", title="จำนวนกิจกรรม"),
                        tooltip=["date:T", "count:Q"]
                    )
                    .properties(
                        width="container",
                        height=300,
                        title="📅 Log Summary (14 Days)"
                    )
                )
                st.altair_chart(chart, use_container_width=True)
            else:
                st.info("ℹ️ No log data available.")
        except Exception as e:
            st.warning(f"Chart load failed: {e}")


# ==========================================
# 🔐 USER PERMISSIONS LOADER & ACCESS CHECK
# ==========================================
def load_user_permissions(db):
    """โหลดสิทธิ์ผู้ใช้จากตาราง user_permissions"""
    try:
        df = db.execute_query("""
            SELECT username, role, allowed_tables, allowed_procedures, allowed_edit_tables
            FROM user_permissions
        """)
        if df is None or df.empty:
            return {}
        perms = {}
        for _, row in df.iterrows():
            perms[row['username']] = {
                "role": row['role'],
                "allowed_tables": [t.strip() for t in (row['allowed_tables'] or '').split(',') if t.strip()],
                "allowed_procedures": [p.strip() for p in (row['allowed_procedures'] or '').split(',') if p.strip()],
                "allowed_edit_tables": [e.strip() for e in (row['allowed_edit_tables'] or '').split(',') if e.strip()],
            }
        return perms
    except Exception as e:
        st.warning(f"⚠️ Cannot load user permissions: {e}")
        return {}

def get_user_permission(secret_key: str):
    """ดึงสิทธิ์ของผู้ใช้จาก session_state.user_permissions ตาม secret_key"""
    key = secret_key.strip()
    if not key:
        return None

    # ดึง dict สิทธิ์ผู้ใช้ทั้งหมดจาก session
    user_perms = st.session_state.get('user_permissions', {})

    # ถ้า key ไม่มีใน session — คืน None ทันที
    if key not in user_perms:
        return None

    perm = user_perms[key]

    # ✅ ตรวจให้แน่ใจว่ามี field 'username'
    # (บางกรณีอาจมีแต่ role / allowed_procedures)
    username = perm.get("username") or perm.get("user") or key

    # ✅ คืนค่ามาตรฐานพร้อม fallback
    return {
        "username": username,
        "role": perm.get("role", "Viewer"),
        "allowed_tables": perm.get("allowed_tables", []),
        "allowed_procedures": perm.get("allowed_procedures", []),
        "allowed_edit_tables": perm.get("allowed_edit_tables", []),
    }


# ==========================================
# 🔑 KEY MANAGEMENT TAB (ADMIN ONLY)
# ==========================================
def render_user_management_tab():
    st.markdown("## 🔑 Key Management")
    st.caption("สำหรับตรวจสอบสิทธิ์การเข้าถึงระบบ โดยการกรอกรหัสลับ (Secret Key)")

    # ===== Authorization =====
    secret_key = st.text_input(
        "Enter Secret Key",
        type="password",
        placeholder="Enter your key",
        key="user_mgmt_key"
    ).strip()

    user_perm = get_user_permission(secret_key)

    if not user_perm:
        st.warning("🚫 Access denied. Invalid or missing key.")
        st.stop()

    role = user_perm["role"]
    username = user_perm.get("username", "(unknown)")
    db = st.session_state.db_manager

    # ===== Role-based Display =====
    if role == "Admin":
        st.success(f"✅ Authorized as **Admin** — full access ({username})")

        try:
            df = db.execute_query("SELECT * FROM user_permissions ORDER BY role, username")
        except Exception as e:
            st.error(f"Cannot load users: {e}")
            return

        # ====== COMPLETE FIX FOR REACT ERROR #185 ======
        # Create a completely new clean DataFrame
        clean_data = {}
        
        for col in df.columns:
            if col == "id":
                # Handle id as integer
                clean_data[col] = df[col].apply(
                    lambda x: int(x) if pd.notna(x) and str(x).strip() != '' and str(x) != 'nan' else 0
                )
            elif col in ["created_at", "updated_at"]:
                # Handle datetime columns
                clean_data[col] = df[col].apply(
                    lambda x: str(x) if pd.notna(x) and str(x) != 'NaT' else ''
                )
            else:
                # Handle all other columns as clean strings
                clean_data[col] = df[col].apply(
                    lambda x: str(x) if pd.notna(x) and x is not None and str(x) not in ['nan', 'NaT', '<NA>', 'None'] else ''
                )
        
        # Create new DataFrame from clean data
        df = pd.DataFrame(clean_data)
        
        # Ensure correct dtypes
        if "id" in df.columns:
            df["id"] = df["id"].astype('int64')
        
        for col in df.columns:
            if col != "id":
                df[col] = df[col].astype(str)
        # ====== END FIX ======

        st.markdown("### 📋 Current Users (Editable)")
        edited_df = st.data_editor(
            df,
            num_rows="dynamic",
            use_container_width=True,
            key="user_editor",
            hide_index=True
        )

        # --- ปุ่มบันทึกการแก้ไข ---
        if st.button("💾 Save Changes to Database", type="primary"):
            try:
                db.execute_nonquery("DELETE FROM user_permissions")
                for _, row in edited_df.iterrows():
                    query = """
                        INSERT INTO user_permissions
                        (id, username, role, allowed_tables, allowed_procedures, allowed_edit_tables, created_at, updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,NOW(),NOW())
                    """
                    params = (
                        int(row["id"]) if str(row["id"]).strip().isdigit() and str(row["id"]).strip() != "" else None,
                        row["username"] if row["username"] != "" else None,
                        row["role"] if row["role"] != "" else None,
                        row.get("allowed_tables") if row.get("allowed_tables") != "" else None,
                        row.get("allowed_procedures") if row.get("allowed_procedures") != "" else None,
                        row.get("allowed_edit_tables") if row.get("allowed_edit_tables") != "" else None
                    )
                    db.execute_nonquery(query, params)

                st.success("✅ User permissions updated successfully!")
                st.session_state.user_permissions = load_user_permissions(db)

            except Exception as e:
                st.error(f"❌ Failed to update users: {e}")

        # --- ฟอร์มเพิ่มผู้ใช้ใหม่ (เฉพาะ Admin) ---
        with st.expander("➕ Add New User"):
            with st.form("add_user_form", clear_on_submit=True):
                cols = st.columns(2)
                with cols[0]:
                    new_username = st.text_input("Username", placeholder="เช่น adcharaporn.u")
                    new_role = st.selectbox("Role", ["Viewer", "Operator", "Admin"])
                with cols[1]:
                    allowed_tables = st.text_input("Allowed Tables (comma-separated)")
                    allowed_procs = st.text_input("Allowed Procedures (comma-separated)")
                    allowed_edit = st.text_input("Allowed Edit Tables (comma-separated)")

                submitted = st.form_submit_button("Add User")
                if submitted:
                    try:
                        query = """
                            INSERT INTO user_permissions
                            (username, role, allowed_tables, allowed_procedures, allowed_edit_tables)
                            VALUES (%s,%s,%s,%s,%s)
                        """
                        db.execute_nonquery(query, (new_username, new_role, allowed_tables, allowed_procs, allowed_edit))
                        st.success(f"✅ Added new user: {new_username}")
                        st.session_state.user_permissions = load_user_permissions(db)
                    except Exception as e:
                        st.error(f"❌ Failed to add user: {e}")

    elif role == "Operator":
        st.warning(f"👷 Authorized as **Operator** — view-only mode ({username})")

        try:
            df = db.execute_query("SELECT * FROM user_permissions WHERE username = %s", (username,))
            
            # Apply same fix for Operator view
            clean_data = {}
            for col in df.columns:
                if col == "id":
                    clean_data[col] = df[col].apply(
                        lambda x: int(x) if pd.notna(x) and str(x).strip() != '' else 0
                    )
                elif col in ["created_at", "updated_at"]:
                    clean_data[col] = df[col].apply(
                        lambda x: str(x) if pd.notna(x) and str(x) != 'NaT' else ''
                    )
                else:
                    clean_data[col] = df[col].apply(
                        lambda x: str(x) if pd.notna(x) and str(x) not in ['nan', 'NaT', '<NA>', 'None'] else ''
                    )
            df = pd.DataFrame(clean_data)
            
        except Exception as e:
            st.error(f"Cannot load your data: {e}")
            return

        st.markdown("### 📋 Your Information (View Only)")
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.info("ℹ️ You cannot add or edit user data in Operator mode.")

    else:
        st.warning(f"⚠️ Role `{role}` has no access to this section.")
        st.stop() 
 

def render_ocr_tab():
    """
    Modern OCR Document Reader with Dashboard-style Interface
    """
    
    # ตรวจสอบ OCR module
    if not OCR_AVAILABLE:
        st.error("⚠️ ระบบ OCR ยังไม่พร้อมใช้งาน กรุณาตรวจสอบการติดตั้งโมดูล ocr_module.py")
        return

    # === HEADER WITH STATS CARDS ===
    st.markdown("""
        <style>
        .stat-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 1.5rem;
            border-radius: 12px;
            color: white;
            margin-bottom: 1rem;
        }
        .stat-card-green {
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        }
        .stat-card-orange {
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        }
        .stat-card-blue {
            background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
        }
        .stat-card h3 {
            margin: 0;
            font-size: 2rem;
            font-weight: bold;
        }
        .stat-card p {
            margin: 0;
            font-size: 1rem;
            opacity: 0.9;
        }
        </style>
    """, unsafe_allow_html=True)

    # Dashboard Cards
    col1, col2, col3, col4 = st.columns(4)
    
    stats = get_dashboard_stats()
    
    with col1:
        st.markdown(f"""
            <div class="stat-card">
                <p>📄 งานทั้งหมด</p>
                <h3>{stats['total']}</h3>
            </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
            <div class="stat-card stat-card-orange">
                <p>⏳ รอดำเนินการ</p>
                <h3>{stats['pending']}</h3>
            </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
            <div class="stat-card stat-card-green">
                <p>✅ เสร็จแล้ว</p>
                <h3>{stats['completed']}</h3>
            </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
            <div class="stat-card stat-card-blue">
                <p>📊 วันนี้</p>
                <h3>{stats['today']}</h3>
            </div>
        """, unsafe_allow_html=True)

    st.divider()

    # === TAB NAVIGATION ===
    tab1, tab2 = st.tabs(["📤 อัพโหลดเอกสาร", "📋 จัดการเอกสาร"])
    
    with tab1:
        render_upload_section()
    
    with tab2:
        render_management_section()


def render_upload_section():
    """ส่วนอัพโหลดและ OCR เอกสาร"""
    
    st.markdown("### 📤 อัพโหลดเอกสารใหม่")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        uploaded = st.file_uploader(
            "เลือกไฟล์เอกสาร",
            type=["pdf", "png", "jpg", "jpeg"],
            help="รองรับไฟล์ PDF, PNG, JPG (ขนาดไม่เกิน 10MB)",
            label_visibility="collapsed"
        )
    
    with col2:
        st.info("💡 **Tips**: ไฟล์ที่มีความคมชัดจะให้ผลลัพธ์ที่ดีกว่า")

    if uploaded:
        with st.spinner("🔍 กำลังประมวลผล OCR..."):
            try:
                ocr = EnhancedThaiDocumentOCR()
                
                # บันทึกไฟล์ชั่วคราว
                import tempfile
                file_ext = os.path.splitext(uploaded.name)[1].lower() or ".pdf"
                with tempfile.NamedTemporaryFile(delete=False, suffix=file_ext) as tmp:
                    tmp.write(uploaded.read())
                    temp_path = tmp.name

                result = ocr.process_document(temp_path)
                os.remove(temp_path)

                if not result:
                    st.warning("⚠️ ไม่สามารถอ่านข้อมูลจากไฟล์นี้ได้")
                    return

                confidence = result.get('confidence', 0.0)
                if confidence >= 80:
                    st.success(f"✅ OCR สำเร็จ! ความแม่นยำ: {confidence:.1f}%")
                elif confidence >= 60:
                    st.warning(f"⚠️ OCR สำเร็จ แต่ความแม่นยำปานกลาง: {confidence:.1f}%")
                else:
                    st.error(f"❌ ความแม่นยำต่ำ: {confidence:.1f}% - กรุณาตรวจสอบข้อมูล")

                render_ocr_form(result, uploaded.name)

            except Exception as e:
                st.error(f"❌ เกิดข้อผิดพลาด: {str(e)}")


def render_ocr_form(result, filename):
    """ฟอร์มบันทึกข้อมูล OCR แบบ Modern"""
    
    st.markdown("---")
    st.markdown("### 📝 ข้อมูลเอกสาร")
    
    key_fields = result.get("key_fields", {})
    
    # Form Layout
    with st.form("ocr_save_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        
        with col1:
            doc_no = st.text_input(
                "📄 เลขที่หนังสือ *",
                value=key_fields.get("เลขที่หนังสือ", ""),
                placeholder="เช่น ศธ 0201/1234"
            )
            
            doc_date = st.date_input(
                "📅 วันที่หนังสือ *",
                value=parse_thai_date(key_fields.get("วันที่", ""))
            )
            
            subject = st.text_area(
                "📋 เรื่อง *",
                value=key_fields.get("เรื่อง", ""),
                height=120,
                placeholder="หัวเรื่องของเอกสาร"
            )
        
        with col2:
            recipient = st.text_input(
                "👤 เรียน / ผู้รับ",
                value=key_fields.get("เรียน", ""),
                placeholder="ชื่อผู้รับหนังสือ"
            )
            
            priority = st.selectbox(
                "⚡ ระดับความสำคัญ",
                options=["ปกติ", "ด่วน", "ด่วนที่สุด"],
                index=0
            )
            
            tags = st.text_input(
                "🏷️ Tags (คั่นด้วยเครื่องหมาย ,)",
                placeholder="เช่น งบประมาณ, การประชุม"
            )
        
        content = st.text_area(
            "📄 สาระสำคัญ",
            value=key_fields.get("เนื้อหา", ""),
            height=100,
            placeholder="สรุปเนื้อหาสำคัญของเอกสาร"
        )
        
        with st.expander("📜 ข้อความทั้งหมดจาก OCR"):
            st.text_area(
                "Full Text",
                value=result.get("text", ""),
                height=200,
                disabled=True,
                label_visibility="collapsed"
            )
        
        # Submit Buttons
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col1:
            submit = st.form_submit_button("💾 บันทึกเอกสาร", type="primary", use_container_width=True)
        
        with col2:
            st.form_submit_button("🔄 รีเซ็ต", use_container_width=True)
        
        if submit:
            if not doc_no or not subject:
                st.error("❌ กรุณากรอก **เลขที่หนังสือ** และ **เรื่อง**")
            else:
                save_ocr_document(
                    doc_no, doc_date, subject, recipient, content,
                    result.get("text", ""), result.get("confidence", 0),
                    filename, priority, tags
                )


def render_management_section():
    """ส่วนจัดการเอกสาร แบบ Dashboard"""
    
    st.markdown("### 📋 ตารางเอกสารทั้งหมด")
    
    # Filter Bar
    col1, col2, col3, col4 = st.columns([3, 2, 1, 1])
    
    with col1:
        search_term = st.text_input(
            "🔍 ค้นหา",
            placeholder="เลขที่ / เรื่อง / ผู้รับ / Tags",
            key="search_doc"
        )
    
    with col2:
        status_filter = st.selectbox(
            "📊 สถานะ",
            ["ทั้งหมด", "รอดำเนินการ", "เสร็จแล้ว"],
            key="status_filter"
        )
    
    with col3:
        if st.button("🔄 รีเฟรช", use_container_width=True):
            st.rerun()
    
    with col4:
        if st.button("📥 Export", use_container_width=True):
            export_documents(search_term, status_filter)

    st.markdown("---")

    # Load and Display Documents
    try:
        df = load_documents(search_term, status_filter)
        
        if not df.empty:
            # Custom styling for dataframe
            st.markdown("""
                <style>
                .dataframe {
                    font-size: 14px;
                }
                </style>
            """, unsafe_allow_html=True)
            
            # Display table with custom config
            st.dataframe(
                df,
                use_container_width=True,
                height=500,
                column_config={
                    "id": st.column_config.NumberColumn("ID", width="small"),
                    "doc_no": st.column_config.TextColumn("เลขที่หนังสือ", width="medium"),
                    "doc_date": st.column_config.TextColumn("วันที่", width="small"),
                    "subject": st.column_config.TextColumn("เรื่อง", width="large"),
                    "recipient": st.column_config.TextColumn("ผู้รับ", width="medium"),
                    "priority": st.column_config.TextColumn("ความสำคัญ", width="small"),
                    "status": st.column_config.TextColumn("สถานะ", width="medium"),
                    "confidence": st.column_config.TextColumn("OCR %", width="small"),
                    "created_at": st.column_config.TextColumn("สร้างเมื่อ", width="medium"),
                },
                hide_index=True
            )
            
            st.markdown(f"**แสดง {len(df)} รายการ**")
            
            # Document Actions
            st.markdown("---")
            render_quick_actions(df)
            
        else:
            st.info("📭 ไม่พบเอกสารในระบบ")
    
    except Exception as e:
        st.error(f"⚠️ เกิดข้อผิดพลาด: {str(e)}")


def render_quick_actions(df):
    """Quick Actions สำหรับจัดการเอกสาร"""
    
    st.markdown("### 🛠️ จัดการเอกสาร")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        selected_id = st.selectbox(
            "เลือกเอกสาร",
            options=df['id'].tolist(),
            format_func=lambda x: f"ID {x}: {df[df['id']==x]['doc_no'].values[0]} - {df[df['id']==x]['subject'].values[0][:40]}...",
            key="select_doc_action"
        )
    
    with col2:
        action = st.selectbox(
            "การดำเนินการ",
            ["-- เลือก --", "✏️ แก้ไข", "✅ ปิดงาน", "🗑️ ลบ"],
            key="action_type"
        )
    
    if action == "✏️ แก้ไข":
        render_edit_form(selected_id)
    elif action == "✅ ปิดงาน":
        render_close_form(selected_id)
    elif action == "🗑️ ลบ":
        render_delete_form(selected_id)


def render_edit_form(doc_id):
    """ฟอร์มแก้ไขเอกสาร"""
    
    try:
        doc = get_document_by_id(doc_id)
        if not doc:
            st.error("❌ ไม่พบเอกสาร")
            return
        
        with st.form(f"edit_form_{doc_id}"):
            st.markdown(f"##### ✏️ แก้ไขเอกสาร ID: {doc_id}")
            
            col1, col2 = st.columns(2)
            
            with col1:
                new_doc_no = st.text_input("เลขที่หนังสือ", value=doc['doc_no'])
                new_subject = st.text_area("เรื่อง", value=doc['subject'], height=100)
                new_content = st.text_area("สาระสำคัญ", value=doc['content'] or "", height=100)
            
            with col2:
                new_doc_date = st.date_input("วันที่", value=parse_date_string(doc['doc_date']))
                new_recipient = st.text_input("ผู้รับ", value=doc['recipient'] or "")
                new_priority = st.selectbox(
                    "ความสำคัญ",
                    ["ปกติ", "ด่วน", "ด่วนที่สุด"],
                    index=["ปกติ", "ด่วน", "ด่วนที่สุด"].index(doc['priority'])
                )
                new_tags = st.text_input("Tags", value=doc['tags'] or "")
            
            col1, col2 = st.columns(2)
            with col1:
                submit = st.form_submit_button("💾 บันทึกการแก้ไข", type="primary", use_container_width=True)
            with col2:
                cancel = st.form_submit_button("❌ ยกเลิก", use_container_width=True)
            
            if submit:
                update_document(doc_id, new_doc_no, new_doc_date, new_subject, 
                              new_recipient, new_content, new_priority, new_tags)
                st.success("✅ แก้ไขข้อมูลเรียบร้อย!")
                time.sleep(1)
                st.rerun()
    
    except Exception as e:
        st.error(f"❌ เกิดข้อผิดพลาด: {str(e)}")


def render_close_form(doc_id):
    """ฟอร์มปิดงาน"""
    
    with st.form(f"close_form_{doc_id}"):
        st.markdown(f"##### ✅ ปิดงานเอกสาร ID: {doc_id}")
        
        close_note = st.text_area(
            "หมายเหตุการปิดงาน",
            placeholder="ระบุรายละเอียดการดำเนินการ...",
            height=100
        )
        
        col1, col2 = st.columns(2)
        with col1:
            confirm = st.form_submit_button("✅ ยืนยันปิดงาน", type="primary", use_container_width=True)
        with col2:
            cancel = st.form_submit_button("❌ ยกเลิก", use_container_width=True)
        
        if confirm:
            close_document(doc_id, close_note)
            st.success("✅ ปิดงานเรียบร้อยแล้ว!")
            st.balloons()
            time.sleep(1)
            st.rerun()


def render_delete_form(doc_id):
    """ฟอร์มลบเอกสาร"""
    
    st.warning("⚠️ **คำเตือน:** การลบเอกสารจะไม่สามารถกู้คืนได้!")
    
    with st.form(f"delete_form_{doc_id}"):
        confirm_text = st.text_input(
            f"พิมพ์ 'DELETE {doc_id}' เพื่อยืนยัน",
            placeholder=f"DELETE {doc_id}"
        )
        
        col1, col2 = st.columns(2)
        with col1:
            delete = st.form_submit_button("🗑️ ลบเอกสาร", type="secondary", use_container_width=True)
        with col2:
            cancel = st.form_submit_button("❌ ยกเลิก", use_container_width=True)
        
        if delete:
            if confirm_text == f"DELETE {doc_id}":
                delete_document(doc_id)
                st.success("🗑️ ลบเอกสารเรียบร้อยแล้ว!")
                time.sleep(1)
                st.rerun()
            else:
                st.error("❌ กรุณาพิมพ์ข้อความยืนยันให้ถูกต้อง")


# === Database Helper Functions ===

def get_dashboard_stats():
    """ดึงสถิติสำหรับ Dashboard"""
    try:
        db_manager = DatabaseManager()
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM ocr")
        total = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM ocr WHERE status='on_process'")
        pending = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM ocr WHERE status='closed'")
        completed = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM ocr WHERE DATE(created_at) = CURDATE()")
        today = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return {
            'total': total,
            'pending': pending,
            'completed': completed,
            'today': today
        }
    except:
        return {'total': 0, 'pending': 0, 'completed': 0, 'today': 0}


def load_documents(search_term, status_filter):
    """โหลดเอกสารจากฐานข้อมูล"""
    db_manager = DatabaseManager()
    conn = db_manager.get_connection()
    
    query = """
        SELECT 
            id, doc_no, doc_date, subject, recipient, 
            priority, status, 
            ROUND(ocr_confidence, 1) as confidence,
            DATE_FORMAT(created_at, '%d/%m/%Y %H:%i') as created_at
        FROM ocr
        WHERE 1=1
    """
    params = []
    
    if search_term:
        query += """ AND (
            doc_no LIKE %s OR 
            subject LIKE %s OR 
            recipient LIKE %s OR 
            tags LIKE %s
        )"""
        search_pattern = f"%{search_term}%"
        params.extend([search_pattern] * 4)
    
    if status_filter == "รอดำเนินการ":
        query += " AND status = 'on_process'"
    elif status_filter == "เสร็จแล้ว":
        query += " AND status = 'closed'"
    
    query += " ORDER BY id DESC LIMIT 200"
    
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    
    if not df.empty:
        # Format data
        df['doc_date'] = df['doc_date'].astype(str)
        df['confidence'] = df['confidence'].apply(lambda x: f"{x}%")
        df['priority'] = df['priority'].fillna('ปกติ')
        
        # Status mapping with icons
        status_map = {
            'on_process': '⏳ รอดำเนินการ',
            'closed': '✅ เสร็จแล้ว'
        }
        df['status'] = df['status'].map(status_map)
    
    return df


def get_document_by_id(doc_id):
    """ดึงข้อมูลเอกสารตาม ID"""
    try:
        db_manager = DatabaseManager()
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, doc_no, doc_date, subject, recipient, content, 
                   priority, tags, status
            FROM ocr WHERE id = %s
        """, (doc_id,))
        
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if row:
            return {
                'id': row[0],
                'doc_no': row[1],
                'doc_date': row[2],
                'subject': row[3],
                'recipient': row[4],
                'content': row[5],
                'priority': row[6] or 'ปกติ',
                'tags': row[7],
                'status': row[8]
            }
        return None
    except:
        return None


def save_ocr_document(doc_no, doc_date, subject, recipient, content, 
                      full_text, confidence, filename, priority, tags):
    """บันทึกเอกสาร OCR"""
    try:
        db_manager = DatabaseManager()
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        sql = """
            INSERT INTO ocr 
            (doc_no, doc_date, subject, recipient, content, full_text, 
             ocr_confidence, source_file, priority, tags, status, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.execute(sql, (
            doc_no, doc_date, subject, recipient or None, content or None,
            full_text, confidence, filename, priority, tags or None,
            'on_process', st.session_state.get('username', 'system')
        ))
        
        conn.commit()
        doc_id = cursor.lastrowid
        cursor.close()
        conn.close()
        
        st.success(f"✅ บันทึกเอกสาร ID: {doc_id} เรียบร้อยแล้ว!")
        st.balloons()
        time.sleep(1.5)
        st.rerun()
        
    except Exception as e:
        st.error(f"❌ ไม่สามารถบันทึกได้: {str(e)}")


def update_document(doc_id, doc_no, doc_date, subject, recipient, content, priority, tags):
    """อัพเดทเอกสาร"""
    try:
        db_manager = DatabaseManager()
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        sql = """
            UPDATE ocr 
            SET doc_no=%s, doc_date=%s, subject=%s, recipient=%s, 
                content=%s, priority=%s, tags=%s, updated_at=NOW()
            WHERE id=%s
        """
        
        cursor.execute(sql, (
            doc_no, doc_date, subject, recipient, content, priority, tags, doc_id
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        st.error(f"❌ เกิดข้อผิดพลาด: {str(e)}")


def close_document(doc_id, close_note):
    """ปิดงานเอกสาร"""
    try:
        db_manager = DatabaseManager()
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE ocr 
            SET status='closed', close_note=%s, closed_at=NOW(), 
                closed_by=%s
            WHERE id=%s
        """, (close_note, st.session_state.get('username', 'system'), doc_id))
        
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        st.error(f"❌ เกิดข้อผิดพลาด: {str(e)}")


def delete_document(doc_id):
    """ลบเอกสาร"""
    try:
        db_manager = DatabaseManager()
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM ocr WHERE id=%s", (doc_id,))
        
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        st.error(f"❌ เกิดข้อผิดพลาด: {str(e)}")


def export_documents(search_term, status_filter):
    """Export เอกสารเป็น CSV"""
    try:
        df = load_documents(search_term, status_filter)
        if not df.empty:
            csv = df.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                "📥 ดาวน์โหลด CSV",
                csv,
                f"ocr_documents_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                "text/csv",
                key="download_csv"
            )
    except Exception as e:
        st.error(f"❌ ไม่สามารถ export ได้: {str(e)}")


def parse_thai_date(date_str):
    """แปลงวันที่ภาษาไทย"""
    from datetime import datetime
    
    if not date_str or date_str == 'None':
        return datetime.now().date()
    
    try:
        date_str = str(date_str).replace('พ.ศ.', '').replace('ค.ศ.', '').strip()
        
        for fmt in ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%y"]:
            try:
                parsed = datetime.strptime(date_str, fmt)
                if parsed.year > 2500:
                    parsed = parsed.replace(year=parsed.year - 543)
                return parsed.date()
            except:
                continue
        
        return datetime.now().date()
    except:
        return datetime.now().date()


def parse_date_string(date_str):
    """แปลง date string เป็น date object"""
    from datetime import datetime
    
    try:
        if isinstance(date_str, str):
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        return date_str
    except:
        return datetime.now().date()
         
 
# ===== MAIN APPLICATION =====
def main():
    try:
        st.markdown("""
        <div class="main-header">
            <h1>🚀 Database Management Hub</h1>
            <p>Complete database management system with AI OCR document processing, import automation, procedures, update tracking and file merger</p>
        </div>
        """, unsafe_allow_html=True)

        if 'client_ip' not in st.session_state:
            try:
                import requests
                st.session_state['client_ip'] = requests.get('https://api.ipify.org').text
            except:
                st.session_state['client_ip'] = 'unknown'
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
        # โหลดสิทธิ์ผู้ใช้ทั้งหมด (ครั้งเดียว)
        if 'user_permissions' not in st.session_state:
            st.session_state.user_permissions = load_user_permissions(st.session_state.db_manager)




        with st.sidebar:
            # === CONFIGURATION SECTION ===
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
                st.cache_data.clear()
                st.rerun()
        
            try:
                tables_info = get_cached_tables_info()
                tables = [table['TABLE_NAME'] for table in tables_info] if tables_info else []
            except Exception:
                tables = []
                tables_info = []
        
            st.write(f"📊 Available Tables: {len(tables)}")
        
            # === SMART AI ASSISTANT ===
            st.markdown("---")
            st.subheader("🧠 Smart AI Assistant")
            st.caption("พิมพ์คำถามเพื่อให้ AI วิเคราะห์หรืออธิบายข้อมูลในฐานข้อมูล")
        
            user_query = st.text_area(
                "💬 Ask AI",
                placeholder="เช่น “แสดง 5 ตารางที่อัปเดตล่าสุด” หรือ “SQL หายอดรวมจากตาราง datacomNT”",
                key="ai_assistant_query",
                height=80
            )
        
            if st.button("🚀 Analyze with AI", use_container_width=True):
                if not user_query.strip():
                    st.warning("⚠️ กรุณาพิมพ์คำถามก่อน", icon="💡")
                else:
                    with st.spinner("🤖 AI กำลังวิเคราะห์ข้อมูล..."):
                        # Mockup AI Response (ยังไม่เชื่อมจริง)
                        st.success("✨ AI Suggestion:")
                        st.write("```sql\nSELECT * FROM datacomNT ORDER BY timestamp DESC LIMIT 5;\n```")
                        st.caption("💡 นี่เป็นตัวอย่าง SQL ที่ AI แนะนำ")
        
            # === RECENT ACTIVITY ===
            st.markdown("---")
            st.subheader("🕓 Recent Activity")
        
            try:
                db = st.session_state.get('db_manager')
                if db:
                    df_log = db.execute_query("""
                        SELECT username, action, target, timestamp
                        FROM activity_log
                        ORDER BY timestamp DESC
                        LIMIT 5
                    """)
                else:
                    df_log = None
            except Exception as e:
                df_log = None
                st.warning(f"⚠️ Cannot load activity log: {e}")
        
            if df_log is not None and not df_log.empty:
                # ซ่อน username กลาง
           
                def mask_username(name: str):
                    if not name or not isinstance(name, str):
                        return ""
                
                    # ถ้าตัวอักษรเดียว เช่น "A"
                    if len(name) == 1:
                        return "*" * 6
                
                    # ถ้าตั้งแต่ 2 ตัวขึ้นไป เช่น "AB", "Alex", "1177"
                    return name[0] + "*" * 6 + name[-1]
        
                df_log["username"] = df_log["username"].apply(mask_username)
                for _, row in df_log.iterrows():
                    st.markdown(
                        f"• **{row['action']}** → `{row['target']}`  \n"
                        f"<span style='color:gray;font-size:0.85em;'>👤 {row['username']} — 🕒 {row['timestamp']}</span>",
                        unsafe_allow_html=True
                    )
            else:
                st.info("ยังไม่มีบันทึกกิจกรรมล่าสุด")
        
            # === STYLING ===
            st.markdown("""
            <style>
            [data-testid="stSidebar"] {
                background: linear-gradient(180deg, #f9fafc 0%, #eef1f9 100%);
                padding: 1rem 1.2rem;
                font-family: 'Sarabun', sans-serif;
            }
            [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3, [data-testid="stSidebar"] h4 {
                color: #3b3b98;
            }
            [data-testid="stSidebar"] button {
                border-radius: 8px !important;
            }
            </style>
            """, unsafe_allow_html=True)


        tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([ "📁 Import Data", "⚙️ Run Procedures","🧾 View & Edit Data","🔗 File Merger","🧠 AI OCR","📜 Logs","🔑 Key Management"])
        with tab1:
            render_import_tab()
        with tab2:
            render_procedures_tab()
        with tab3:
            render_data_editor_tab()  # ✅ เพิ่มใหม่
        with tab4:
            render_merger_tab() 
        with tab5:
            render_ocr_tab()
        with tab6:
            render_log_tab()
        with tab7:
            render_user_management_tab()
    except Exception as e:
        st.error(f"Application error: {e}")

if __name__ == "__main__":
    main()
