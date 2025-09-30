import streamlit as st
import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
from datetime import datetime
import time
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
    page_title="Data Import Hub",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ===== OPTIMIZATION 1: CACHING =====
@st.cache_data(ttl=300)  # Cache for 5 minutes
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

@st.cache_data(ttl=60)  # Cache for 1 minute
def get_cached_table_preview(table_name, limit=5):
    """Cache table preview with smaller limit"""
    try:
        db_manager = DatabaseManager()
        # Use LIMIT in SQL query instead of loading all data
        query = f"SELECT * FROM {table_name} ORDER BY id DESC LIMIT {limit}"
        return db_manager.execute_query(query)
    except Exception as e:
        return pd.DataFrame()

# ===== NEW: PROCEDURES FUNCTIONS =====
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
        conn = db_manager.get_connection()
        
        if not conn:
            return {'success': False, 'error': 'No database connection'}
        
        cursor = conn.cursor()
        
        # Build CALL statement
        if parameters:
            placeholders = ', '.join(['%s'] * len(parameters))
            call_statement = f"CALL {procedure_name}({placeholders})"
            cursor.execute(call_statement, parameters)
        else:
            call_statement = f"CALL {procedure_name}()"
            cursor.execute(call_statement)
        
        # Fetch results if any
        results = []
        try:
            for result in cursor.stored_results():
                results.append(result.fetchall())
        except:
            pass
        
        conn.commit()
        cursor.close()
        
        return {
            'success': True,
            'message': f'Procedure {procedure_name} executed successfully',
            'results': results
        }
        
    except Error as e:
        return {'success': False, 'error': str(e)}
    except Exception as e:
        return {'success': False, 'error': str(e)}

# ===== OPTIMIZATION 2: SIMPLIFIED CSS =====
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
    
    .procedure-card {
        background: white;
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        padding: 1rem;
        margin: 0.5rem 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    
    .procedure-card:hover {
        border-color: #667eea;
        box-shadow: 0 4px 8px rgba(102,126,234,0.1);
    }
</style>
""", unsafe_allow_html=True)

def render_import_tab():
    """Render the Import tab (original functionality)"""
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.header("📁 File Import")
        
        # Initialize session state
        if 'db_manager' not in st.session_state:
            st.session_state.db_manager = DatabaseManager()
        if 'file_processor' not in st.session_state:
            st.session_state.file_processor = FileProcessor()
        
        # Get tables
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
            # Show basic info immediately
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
            
            col_preview1, col_preview2 = st.columns([1, 2])
            
            with col_preview1:
                show_preview = st.button("🔄 Show Preview", type="secondary")
            
            with col_preview2:
                if show_preview:
                    st.info("Loading preview...")
            
            if show_preview:
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
                help="Max size: 200MB"
            )
            
            if uploaded_file:
                file_size = uploaded_file.size
                if file_size > 200 * 1024 * 1024:
                    st.error("❌ File too large! Please use files smaller than 200MB")
                    return
                
                try:
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    status_text.text("📁 Processing file...")
                    progress_bar.progress(25)
                    
                    df = st.session_state.file_processor.process_file(uploaded_file)
                    progress_bar.progress(50)
                    
                    if df is not None:
                        max_preview_rows = 1000
                        if len(df) > max_preview_rows:
                            preview_df = df.head(max_preview_rows)
                            st.warning(f"📊 Large file detected! Showing first {max_preview_rows} rows for preview. Full {len(df)} rows will be imported.")
                        else:
                            preview_df = df
                        
                        progress_bar.progress(75)
                        status_text.text("✅ File loaded successfully!")
                        
                        st.success(f"✅ File loaded! {len(df)} rows, {len(df.columns)} columns")
                        
                        with st.expander("📋 File Preview", expanded=False):
                            st.dataframe(preview_df.head(10), use_container_width=True, hide_index=True)
                        
                        progress_bar.progress(100)
                        
                        st.subheader("🔗 Column Mapping")
                        
                        table_columns = get_cached_table_columns(selected_table)
                        
                        if table_columns:
                            def smart_auto_map(file_cols, db_cols):
                                mapping = {}
                                db_col_names = {col['COLUMN_NAME'].lower(): col['COLUMN_NAME'] for col in db_cols}
                                
                                for file_col in file_cols:
                                    file_col_clean = file_col.lower().strip().replace(' ', '_')
                                    
                                    if file_col_clean in db_col_names:
                                        mapping[file_col] = db_col_names[file_col_clean]
                                    else:
                                        for db_key, db_val in db_col_names.items():
                                            if file_col_clean in db_key or db_key in file_col_clean:
                                                mapping[file_col] = db_val
                                                break
                                
                                return mapping
                            
                            auto_mapping = smart_auto_map(df.columns, table_columns)
                            
                            mapping = {}
                            
                            if auto_mapping:
                                st.success(f"🎯 Auto-mapped {len(auto_mapping)} columns")
                                
                                col_map1, col_map2 = st.columns(2)
                                with col_map1:
                                    st.write("**Auto-mapped columns:**")
                                with col_map2:
                                    if st.button("✅ Use Auto-mapping"):
                                        mapping = auto_mapping.copy()
                            
                            unmapped_cols = [col for col in df.columns if col not in auto_mapping]
                            
                            if unmapped_cols:
                                st.write(f"**Map remaining {len(unmapped_cols)} columns:**")
                                
                                db_options = ["🚫 Skip"] + [col['COLUMN_NAME'] for col in table_columns]
                                
                                display_cols = unmapped_cols[:10]
                                if len(unmapped_cols) > 10:
                                    st.warning(f"Showing first 10 of {len(unmapped_cols)} unmapped columns")
                                
                                for file_col in display_cols:
                                    mapped_col = st.selectbox(
                                        f"📄 {file_col}",
                                        options=db_options,
                                        key=f"map_{file_col}"
                                    )
                                    
                                    if mapped_col != "🚫 Skip":
                                        mapping[file_col] = mapped_col
                            
                            final_mapping = {**auto_mapping, **mapping}
                            
                            if final_mapping:
                                st.markdown("### 🚀 Import Data")
                                
                                col_import1, col_import2 = st.columns([2, 1])
                                
                                with col_import1:
                                    st.info(f"Ready to import {len(df)} rows with {len(final_mapping)} columns")
                                
                                with col_import2:
                                    import_button = st.button("🚀 Import Now!", type="primary")
                                
                                if import_button:
                                    try:
                                        import_progress = st.progress(0)
                                        import_status = st.empty()
                                        
                                        import_status.text("🔄 Preparing data...")
                                        import_progress.progress(20)
                                        
                                        chunk_size = 10000
                                        total_rows = len(df)
                                        
                                        if total_rows > chunk_size:
                                            import_status.text(f"🔄 Importing {total_rows} rows in chunks...")
                                            
                                            for i in range(0, total_rows, chunk_size):
                                                chunk = df.iloc[i:i+chunk_size]
                                                result = st.session_state.db_manager.import_data(selected_table, chunk, final_mapping)
                                                
                                                progress = min(100, int((i + chunk_size) / total_rows * 80) + 20)
                                                import_progress.progress(progress)
                                                import_status.text(f"🔄 Imported {min(i + chunk_size, total_rows)} / {total_rows} rows")
                                        else:
                                            import_status.text("🔄 Importing data...")
                                            import_progress.progress(50)
                                            result = st.session_state.db_manager.import_data(selected_table, df, final_mapping)
                                        
                                        import_progress.progress(100)
                                        
                                        if result.get('success', False):
                                            st.success(f"🎉 Import completed! {result.get('rows_affected', 0)} rows imported")
                                            st.balloons()
                                            st.cache_data.clear()
                                        else:
                                            st.error(f"❌ Import failed: {result.get('error', 'Unknown error')}")
                                    
                                    except Exception as e:
                                        st.error(f"❌ Import error: {str(e)}")
                            
                            else:
                                st.warning("⚠️ Please map at least one column")
                        
                        else:
                            st.error("❌ Could not fetch table structure")
                
                except Exception as e:
                    st.error(f"❌ Error processing file: {str(e)}")
    
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
        if st.button("🔄 Refresh All", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

def render_procedures_tab():
    """Render the Procedures/Update tab"""
    st.header("⚙️ Database Procedures & Updates")
    
    # Initialize database manager
    if 'db_manager' not in st.session_state:
        st.session_state.db_manager = DatabaseManager()
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("🔧 Stored Procedures")
        
        # Refresh button
        if st.button("🔄 Refresh Procedures List", type="secondary"):
            st.cache_data.clear()
            st.rerun()
        
        # Get procedures
        procedures = get_stored_procedures()
        
        if procedures:
            st.info(f"Found {len(procedures)} stored procedures")
            
            # Filter procedures
            search_query = st.text_input("🔍 Search procedures", placeholder="Type to filter...")
            
            if search_query:
                filtered_procedures = [p for p in procedures if search_query.lower() in p['ROUTINE_NAME'].lower()]
            else:
                filtered_procedures = procedures
            
            # Display procedures
            for proc in filtered_procedures:
                with st.expander(f"📦 {proc['ROUTINE_NAME']} ({proc['ROUTINE_TYPE']})"):
                    
                    # Procedure info
                    col_info1, col_info2 = st.columns(2)
                    
                    with col_info1:
                        st.write(f"**Type:** {proc['ROUTINE_TYPE']}")
                        if proc.get('RETURNS'):
                            st.write(f"**Returns:** {proc['RETURNS']}")
                    
                    with col_info2:
                        if proc.get('CREATED'):
                            st.write(f"**Created:** {str(proc['CREATED'])[:10]}")
                        if proc.get('LAST_ALTERED'):
                            st.write(f"**Modified:** {str(proc['LAST_ALTERED'])[:10]}")
                    
                    if proc.get('ROUTINE_COMMENT'):
                        st.info(f"💬 {proc['ROUTINE_COMMENT']}")
                    
                    # Get parameters
                    params = get_procedure_parameters(proc['ROUTINE_NAME'])
                    
                    if params:
                        st.write("**Parameters:**")
                        
                        param_values = []
                        
                        for param in params:
                            param_name = param['PARAMETER_NAME']
                            param_mode = param['PARAMETER_MODE']
                            param_type = param['DATA_TYPE']
                            
                            st.write(f"- `{param_name}` ({param_mode}) - {param_type}")
                            
                            # Input field for IN parameters
                            if param_mode == 'IN':
                                if param_type in ['int', 'bigint', 'smallint', 'tinyint']:
                                    value = st.number_input(
                                        f"Enter {param_name}",
                                        key=f"param_{proc['ROUTINE_NAME']}_{param_name}",
                                        step=1
                                    )
                                    param_values.append(int(value))
                                elif param_type in ['decimal', 'float', 'double']:
                                    value = st.number_input(
                                        f"Enter {param_name}",
                                        key=f"param_{proc['ROUTINE_NAME']}_{param_name}",
                                        step=0.01
                                    )
                                    param_values.append(float(value))
                                elif param_type in ['date']:
                                    value = st.date_input(
                                        f"Enter {param_name}",
                                        key=f"param_{proc['ROUTINE_NAME']}_{param_name}"
                                    )
                                    param_values.append(str(value))
                                elif param_type in ['datetime', 'timestamp']:
                                    value = st.text_input(
                                        f"Enter {param_name} (YYYY-MM-DD HH:MM:SS)",
                                        key=f"param_{proc['ROUTINE_NAME']}_{param_name}"
                                    )
                                    param_values.append(value if value else None)
                                else:
                                    value = st.text_input(
                                        f"Enter {param_name}",
                                        key=f"param_{proc['ROUTINE_NAME']}_{param_name}"
                                    )
                                    param_values.append(value if value else None)
                    else:
                        st.write("**No parameters required**")
                        param_values = None
                    
                    st.markdown("---")
                    
                    # Execute button
                    col_exec1, col_exec2 = st.columns([1, 2])
                    
                    with col_exec1:
                        execute_btn = st.button(
                            f"▶️ Execute",
                            key=f"exec_{proc['ROUTINE_NAME']}",
                            type="primary"
                        )
                    
                    with col_exec2:
                        if execute_btn:
                            st.info("Executing procedure...")
                    
                    if execute_btn:
                        try:
                            with st.spinner(f"Executing {proc['ROUTINE_NAME']}..."):
                                result = execute_procedure(proc['ROUTINE_NAME'], param_values if param_values else None)
                            
                            if result['success']:
                                st.success(f"✅ {result['message']}")
                                
                                # Display results if any
                                if result.get('results'):
                                    st.write("**Results:**")
                                    for i, res in enumerate(result['results']):
                                        if res:
                                            st.write(f"Result set {i+1}:")
                                            st.write(res)
                                
                                # Clear cache to refresh data
                                st.cache_data.clear()
                            else:
                                st.error(f"❌ Execution failed: {result['error']}")
                        
                        except Exception as e:
                            st.error(f"❌ Error: {str(e)}")
        
        else:
            st.warning("⚠️ No stored procedures found in database")
            st.info("💡 Create stored procedures in your MySQL database to see them here")
    
    with col2:
        st.subheader("📊 Quick Stats")
        
        # Procedure statistics
        if procedures:
            total_procs = len(procedures)
            procedures_by_type = {}
            
            for proc in procedures:
                proc_type = proc['ROUTINE_TYPE']
                procedures_by_type[proc_type] = procedures_by_type.get(proc_type, 0) + 1
            
            st.markdown(f"""
            <div class="metric-card">
                <h3>{total_procs}</h3>
                <p>Total Procedures</p>
            </div>
            """, unsafe_allow_html=True)
            
            for proc_type, count in procedures_by_type.items():
                st.markdown(f"""
                <div class="metric-card">
                    <h4>{count}</h4>
                    <p>{proc_type}s</p>
                </div>
                """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        st.subheader("ℹ️ Info")
        st.info("""
        **How to use:**
        
        1. Select a procedure from the list
        2. Fill in required parameters
        3. Click Execute to run
        4. View results below
        
        **Tips:**
        - Use procedures for batch updates
        - Complex data transformations
        - Scheduled maintenance tasks
        """)

def main():
    try:
        # Header
        st.markdown("""
        <div class="main-header">
            <h1>🚀 Data Import Hub</h1>
            <p>Fast file import system with database management</p>
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
            
            # Test connection
            if 'connection_status' not in st.session_state:
                try:
                    st.session_state.connection_status = st.session_state.db_manager.test_connection()
                except Exception as e:
                    st.session_state.connection_status = False
            
            if st.session_state.connection_status:
                st.markdown('<div class="status-success">✅ Database Connected</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="status-error">❌ Database Connection Failed</div>', unsafe_allow_html=True)
                return
            
            # Refresh button
            if st.button("🔄 Refresh", key="refresh_sidebar"):
                st.cache_data.clear()
                st.rerun()
            
            try:
                tables_info = get_cached_tables_info()
                tables = [table['TABLE_NAME'] for table in tables_info] if tables_info else []
            except Exception as e:
                st.warning(f"Could not get table info: {e}")
                tables = []
                tables_info = []

            st.write(f"📊 Available Tables: {len(tables)}")
            
            # Recent activity
            if tables_info:
                st.subheader("🕒 Recent Activity")
                
                sorted_tables = sorted(
                    [t for t in tables_info if t.get('UPDATE_TIME')], 
                    key=lambda x: x.get('UPDATE_TIME', ''), 
                    reverse=True
                )[:3]
                
                for table_info in sorted_tables:
                    table_name = table_info.get('TABLE_NAME', 'Unknown')
                    update_time = table_info.get('UPDATE_TIME')
                    row_count = table_info.get('TABLE_ROWS', 0) or 0
                    
                    if update_time:
                        try:
                            if isinstance(update_time, str):
                                time_str = update_time[:16]
                            else:
                                time_str = update_time.strftime("%Y-%m-%d %H:%M")
                            
                            st.markdown(f"""
                            <div style="background: #f8f9fa; padding: 0.5rem; border-radius: 4px; margin: 0.2rem 0; border-left: 3px solid #007bff;">
                                <div style="font-weight: bold; font-size: 12px;">📄 {table_name}</div>
                                <div style="font-size: 11px; color: #666;">🕒 {time_str} | 📊 {row_count:,} rows</div>
                            </div>
                            """, unsafe_allow_html=True)
                        except:
                            pass
        
        # ===== NEW: TABS NAVIGATION =====
        tab1, tab2 = st.tabs(["📁 Import Data", "⚙️ Run Procedures"])
        
        with tab1:
            render_import_tab()
        
        with tab2:
            render_procedures_tab()
    
    except Exception as e:
        st.error(f"Application error: {e}")

if __name__ == "__main__":
    main()
