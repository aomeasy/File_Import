import streamlit as st
import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
from datetime import datetime
import re

# Import our modules with error handling
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
    initial_sidebar_state="collapsed"
)

# Custom CSS for modern AI-themed design
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        text-align: center;
        color: white;
    }
    
    .status-success {
        background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        margin: 1rem 0;
    }
    
    .status-error {
        background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        margin: 1rem 0;
    }
    
    .table-preview {
        border: 2px solid #e0e0e0;
        border-radius: 10px;
        padding: 1rem;
        background: #f8f9ff;
    }
    
    .debug-info {
        background: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #007bff;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

def auto_map_columns(file_cols, db_cols):
    """Auto-map columns with support for Thai characters and fuzzy matching"""
    
    auto_mapping = {}
    
    # สร้าง dictionary ของ database columns
    db_col_dict = {col['COLUMN_NAME']: col['COLUMN_NAME'] for col in db_cols}
    
    def normalize_text(text):
        """Normalize text for better matching"""
        if not text:
            return ""
        
        # แปลงเป็น lowercase
        text = text.lower().strip()
        
        # ลบช่องว่าง underscore และเครื่องหมายพิเศษ
        text = text.replace('_', '').replace(' ', '').replace('-', '')
        
        # ลบสระและวรรณยุกต์บางตัวที่อาจแตกต่าง (สำหรับภาษาไทย)
        text = text.replace('ำ', 'า').replace('ั', '').replace('์', '').replace('่', '').replace('้', '').replace('๊', '').replace('๋', '')
        
        return text
    
    def calculate_similarity(str1, str2):
        """Calculate similarity between two strings (0-1)"""
        str1_norm = normalize_text(str1)
        str2_norm = normalize_text(str2)
        
        if str1_norm == str2_norm:
            return 1.0
        
        # Check if one contains the other
        if str1_norm in str2_norm or str2_norm in str1_norm:
            return 0.8
        
        # Calculate character overlap
        set1 = set(str1_norm)
        set2 = set(str2_norm)
        
        if not set1 or not set2:
            return 0.0
        
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        
        return intersection / union if union > 0 else 0.0
    
    # Manual mapping rules สำหรับ columns ที่รู้จัก
    manual_rules = {
        'ลำดมท': 'ลำดับที่',
        'ลาดมท': 'ลำดับที่',
        'ลําดับที่': 'ลำดับที่',
        'ip_address': 'IP Address',
        'ipaddress': 'IP Address',
        'source_file': None,  # Skip this column
        'ระยะเวลาคนล_ddhhmm': 'ระยะเวลาคืนลี dd:hh:mm',
        'ระยะเวลาคนล': 'ระยะเวลาคืนลี dd:hh:mm',
        'ศนยบรการ': 'ศูนย์บริการ',
        'ศูนยบริการ': 'ศูนย์บริการ'
    }
    
    for file_col in file_cols:
        file_col_lower = file_col.lower().strip()
        file_col_norm = normalize_text(file_col)
        
        mapped = False
        
        # 1. Check manual rules first
        if file_col_lower in manual_rules:
            mapped_value = manual_rules[file_col_lower]
            if mapped_value and mapped_value in db_col_dict:
                auto_mapping[file_col] = mapped_value
                mapped = True
                continue
            elif mapped_value is None:
                # Skip this column
                continue
        
        # 2. Try exact match (case-insensitive)
        for db_col in db_cols:
            db_col_name = db_col['COLUMN_NAME']
            if file_col_lower == db_col_name.lower():
                auto_mapping[file_col] = db_col_name
                mapped = True
                break
        
        if mapped:
            continue
        
        # 3. Try normalized match
        best_match = None
        best_similarity = 0.0
        
        for db_col in db_cols:
            db_col_name = db_col['COLUMN_NAME']
            similarity = calculate_similarity(file_col, db_col_name)
            
            # Consider it a match if similarity > 0.7
            if similarity > best_similarity and similarity >= 0.7:
                best_similarity = similarity
                best_match = db_col_name
        
        if best_match:
            auto_mapping[file_col] = best_match
            mapped = True
        
        # 4. Try substring match
        if not mapped:
            for db_col in db_cols:
                db_col_name = db_col['COLUMN_NAME']
                db_col_norm = normalize_text(db_col_name)
                
                if len(file_col_norm) >= 3 and len(db_col_norm) >= 3:
                    if file_col_norm in db_col_norm or db_col_norm in file_col_norm:
                        auto_mapping[file_col] = db_col_name
                        mapped = True
                        break
    
    return auto_mapping

def main():
    try:
        # Header
        st.markdown("""
        <div class="main-header">
            <h1>🚀 Data Import Hub</h1>
            <p>Modern file import system powered by AI-driven interface</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Initialize components
        try:
            db_manager = DatabaseManager()
        except Exception as e:
            st.error(f"Failed to initialize DatabaseManager: {e}")
            st.error("Please check your database configuration in database.py")
            return
        
        try:
            file_processor = FileProcessor()
        except Exception as e:
            st.error(f"Failed to initialize FileProcessor: {e}")
            return

        # Connection status bar
        try:
            connection_status = db_manager.test_connection()
            
            if connection_status:
                # Get tables count
                try:
                    tables_info = db_manager.get_tables_with_info()
                    tables = [table['TABLE_NAME'] for table in tables_info] if tables_info else []
                    tables_count = len(tables)
                except:
                    tables_count = 0
                    tables = []
                    tables_info = []
                
                st.markdown(f"""
                <div style="background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); 
                            padding: 1rem; border-radius: 8px; margin-bottom: 1.5rem; 
                            display: flex; justify-content: space-between; align-items: center;">
                    <div style="color: white; font-weight: bold;">
                        <span style="font-size: 18px;">✅ Database Connected</span>
                    </div>
                    <div style="color: white; font-weight: bold;">
                        <span style="font-size: 16px;">📊 Available Tables: {tables_count}</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown("""
                <div style="background: linear-gradient(135deg, #fa709a 0%, #fee140 100%); 
                            padding: 1rem; border-radius: 8px; margin-bottom: 1.5rem;">
                    <span style="color: white; font-weight: bold; font-size: 18px;">
                        ❌ Database Connection Failed
                    </span>
                </div>
                """, unsafe_allow_html=True)
                st.error("Please check database configuration")
                return
        except Exception as e:
            st.error(f"Database connection error: {e}")
            return
        
        # Main content
        st.header("📁 File Import")
        
        # Table selection
        selected_table = st.selectbox(
            "🎯 Select Target Table",
            options=[""] + tables,
            help="Choose the table where you want to import your data",
            format_func=lambda x: x if x == "" else f"📄 {x}"
        )
        
        # Show table info when selected
        if selected_table:
            if tables_info:
                table_details = None
                for table_info in tables_info:
                    if table_info.get('TABLE_NAME') == selected_table:
                        table_details = table_info
                        break
                
                if table_details:
                    st.markdown("### 📊 Table Information")
                    
                    col1_info, col2_info, col3_info = st.columns(3)
                    
                    with col1_info:
                        row_count = table_details.get('TABLE_ROWS', 0) or 0
                        st.metric("📊 Total Rows", f"{row_count:,}")
                    
                    with col2_info:
                        update_time = table_details.get('UPDATE_TIME')
                        if update_time:
                            try:
                                if isinstance(update_time, str):
                                    last_update = update_time[:19] if len(update_time) > 19 else update_time
                                else:
                                    last_update = update_time.strftime("%Y-%m-%d %H:%M:%S")
                                st.metric("🕒 Last Update", last_update)
                            except:
                                st.metric("🕒 Last Update", "Unknown")
                        else:
                            st.metric("🕒 Last Update", "No data")
                    
                    with col3_info:
                        data_length = table_details.get('DATA_LENGTH', 0) or 0
                        if data_length and data_length > 0:
                            size_mb = data_length / (1024 * 1024)
                            if size_mb > 1:
                                st.metric("💾 Size", f"{size_mb:.1f} MB")
                            else:
                                size_kb = data_length / 1024
                                st.metric("💾 Size", f"{size_kb:.1f} KB")
                        else:
                            st.metric("💾 Size", "Unknown")
                    
                    create_time = table_details.get('CREATE_TIME')
                    if create_time:
                        try:
                            if isinstance(create_time, str):
                                create_str = create_time[:19] if len(create_time) > 19 else create_time
                            else:
                                create_str = create_time.strftime("%Y-%m-%d %H:%M:%S")
                            st.info(f"📅 **Table Created:** {create_str}")
                        except:
                            pass
                else:
                    st.markdown("### 📊 Table Information")
                    st.info(f"📄 Selected table: **{selected_table}**")
            
            else:
                st.markdown("### 📊 Table Information")
                st.info(f"📄 Selected table: **{selected_table}**")
                st.warning("⚠️ Detailed table information is not available")
        
        if selected_table:
            # Table preview section
            st.subheader(f"👀 Preview: {selected_table}")
            
            col_preview1, col_preview2, col_preview3 = st.columns([2, 1, 1])
            
            with col_preview1:
                show_preview_btn = st.button("🔄 Show Last 5 Rows", type="secondary")
            
            with col_preview2:
                debug_table_btn = st.button("🔍 Debug Table", help="Debug table connection issues")
            
            with col_preview3:
                if st.button("📋 Show Columns"):
                    try:
                        columns = db_manager.get_table_columns(selected_table)
                        if columns:
                            st.write(f"**Table has {len(columns)} columns:**")
                            col_names = [col['COLUMN_NAME'] for col in columns[:10]]
                            st.write(", ".join(col_names))
                            if len(columns) > 10:
                                st.write(f"... and {len(columns) - 10} more columns")
                        else:
                            st.warning("Could not fetch column information")
                    except Exception as e:
                        st.error(f"Error fetching columns: {e}")
            
            # Debug functionality
            if debug_table_btn:
                st.markdown("### 🔍 Table Debug Information")
                
                with st.container():
                    st.markdown('<div class="debug-info">', unsafe_allow_html=True)
                    
                    st.write("**1. Connection Test:**")
                    conn_test = db_manager.test_connection()
                    st.write(f"   Connection Status: {'✅ Connected' if conn_test else '❌ Failed'}")
                    
                    if conn_test:
                        st.write("**2. Table Existence:**")
                        try:
                            test_query = f"SELECT 1 FROM `{selected_table}` LIMIT 1"
                            result = db_manager.execute_query(test_query)
                            exists = not result.empty
                            st.write(f"   Table Exists: {'✅ Yes' if exists else '❌ No'}")
                            
                            if exists:
                                st.write("**3. Row Count Test:**")
                                try:
                                    count_query = f"SELECT COUNT(*) as count FROM `{selected_table}`"
                                    count_result = db_manager.execute_query(count_query)
                                    if not count_result.empty:
                                        row_count = count_result['count'].iloc[0]
                                        st.write(f"   Total Rows: {row_count:,}")
                                    else:
                                        st.write("   Total Rows: Could not determine")
                                except Exception as e:
                                    st.write(f"   Row Count Error: {e}")
                                
                                st.write("**4. Column Information:**")
                                try:
                                    columns = db_manager.get_table_columns(selected_table)
                                    if columns:
                                        st.write(f"   Columns Found: {len(columns)}")
                                        st.write(f"   First 5 Columns: {[col['COLUMN_NAME'] for col in columns[:5]]}")
                                    else:
                                        st.write("   Columns: Could not fetch")
                                except Exception as e:
                                    st.write(f"   Column Error: {e}")
                                
                                st.write("**5. Sample Query Test:**")
                                try:
                                    sample_query = f"SELECT * FROM `{selected_table}` LIMIT 1"
                                    sample_result = db_manager.execute_query(sample_query)
                                    if not sample_result.empty:
                                        st.write("   Sample Query: ✅ Success")
                                        st.write(f"   Sample Data Columns: {list(sample_result.columns)}")
                                    else:
                                        st.write("   Sample Query: ⚠️ No data returned")
                                except Exception as e:
                                    st.write(f"   Sample Query Error: {e}")
                            
                        except Exception as e:
                            st.write(f"   Table Check Error: {e}")
                    
                    st.markdown('</div>', unsafe_allow_html=True)
            
            # Show preview
            if show_preview_btn:
                try:
                    with st.spinner("🔄 Loading table preview..."):
                        preview_data = db_manager.get_table_preview(selected_table)
                    
                    if not preview_data.empty:
                        st.markdown('<div class="table-preview">', unsafe_allow_html=True)
                        st.dataframe(
                            preview_data,
                            use_container_width=True,
                            hide_index=True
                        )
                        st.markdown('</div>', unsafe_allow_html=True)
                        
                        st.success(f"📊 Table has {len(preview_data.columns)} columns and showing last 5 rows")
                    else:
                        st.warning("📭 Table is empty or preview unavailable")
                        st.info("💡 Try using the Debug Table button above to diagnose the issue")
                except Exception as e:
                    st.error(f"❌ Error fetching table data: {str(e)}")
                    st.info("💡 Use the Debug Table button to get more detailed error information")
            
            # File upload
            st.subheader("📤 Upload File")
            uploaded_file = st.file_uploader(
                "Choose a file to import",
                type=['csv', 'xlsx', 'xls'],
                help="Supported formats: CSV, Excel (.xlsx, .xls)"
            )
            
            if uploaded_file:
                try:
                    df = file_processor.process_file(uploaded_file)
                    
                    if df is not None:
                        st.success(f"✅ File loaded successfully! {len(df)} rows, {len(df.columns)} columns")
                        
                        st.subheader("📋 File Preview")
                        st.dataframe(df.head(), use_container_width=True, hide_index=True)
                        
                        # Column mapping
                        st.subheader("🔗 Column Mapping")
                        st.info("📌 Map your file columns to database table columns before importing")
                        
                        table_columns = db_manager.get_table_columns(selected_table)
                        
                        if table_columns:
                            mapping = {}
                            
                            # Column comparison
                            col_map1, col_map2 = st.columns(2)
                            
                            with col_map1:
                                st.markdown("**📁 File Columns:**")
                                for i, col in enumerate(df.columns):
                                    st.write(f"**{i+1}.** {col}")
                            
                            with col_map2:
                                st.markdown("**🗄️ Database Table Columns:**")
                                for i, col_info in enumerate(table_columns):
                                    col_name = col_info['COLUMN_NAME']
                                    col_type = col_info['DATA_TYPE']
                                    nullable = "NULL" if col_info['IS_NULLABLE'] == 'YES' else "NOT NULL"
                                    st.write(f"**{i+1}.** {col_name} `({col_type})` - {nullable}")
                            
                            st.markdown("---")
                            
                            st.markdown("### 🎯 **Map Your Columns:**")
                            
                            # Auto-mapping
                            auto_mapping = auto_map_columns(df.columns, table_columns)
                            
                            if auto_mapping:
                                st.success(f"✨ Auto-mapped {len(auto_mapping)} columns successfully!")
                            
                            # Mapping dropdowns
                            for i, file_col in enumerate(df.columns):
                                map_col1, map_col2 = st.columns([1, 1])
                                
                                with map_col1:
                                    st.write(f"📄 **{file_col}**")
                                
                                with map_col2:
                                    db_options = ["🚫 Skip this column"] + [col['COLUMN_NAME'] for col in table_columns]
                                    
                                    default_index = 0
                                    if file_col in auto_mapping:
                                        suggested_col = auto_mapping[file_col]
                                        if suggested_col in [col['COLUMN_NAME'] for col in table_columns]:
                                            default_index = [col['COLUMN_NAME'] for col in table_columns].index(suggested_col) + 1
                                    
                                    mapped_col = st.selectbox(
                                        f"Map to:",
                                        options=db_options,
                                        index=default_index,
                                        key=f"mapping_{file_col}_{i}",
                                        label_visibility="collapsed",
                                        help=f"Auto-suggested: {auto_mapping.get(file_col, 'No suggestion')}" if file_col in auto_mapping else "No auto-suggestion available"
                                    )
                                    
                                    if mapped_col != "🚫 Skip this column":
                                        mapping[file_col] = mapped_col
                            
                            st.markdown("---")
                            
                            if mapping:
                                st.markdown("### 📋 **Mapping Summary:**")
                                mapping_df = pd.DataFrame([
                                    {"File Column": file_col, "→": "→", "Database Column": db_col}
                                    for file_col, db_col in mapping.items()
                                ])
                                st.dataframe(mapping_df, use_container_width=True, hide_index=True)
                                
                                st.markdown("### 🚀 **Ready to Import!**")
                                
                                col_import1, col_import2 = st.columns([2, 1])
                                
                                with col_import1:
                                    st.info(f"✅ Ready to import **{len(df)}** rows with **{len(mapping)}** mapped columns into `{selected_table}` table")
                                
                                with col_import2:
                                    import_button = st.button(
                                        "🚀 Import Data Now!",
                                        type="primary",
                                        use_container_width=True,
                                        help="Click to start importing data to database"
                                    )
                                
                                if import_button:
                                    try:
                                        with st.spinner("🔄 Importing data to database..."):
                                            import time
                                            time.sleep(1)
                                            result = db_manager.import_data(selected_table, df, mapping)
                                        
                                        if result['success']:
                                            st.success(f"🎉 **Import Successful!** \n\n✅ Imported **{result['rows_affected']}** rows into `{selected_table}` table")
                                            st.balloons()
                                            
                                            st.markdown("### 📊 **Updated Table Preview:**")
                                            updated_preview = db_manager.get_table_preview(selected_table)
                                            if not updated_preview.empty:
                                                st.dataframe(updated_preview, use_container_width=True, hide_index=True)
                                                st.success(f"📈 Table `{selected_table}` now contains updated data!")
                                            else:
                                                st.warning("Could not load updated preview")
                                        else:
                                            st.error(f"❌ **Import Failed:** {result['error']}")
                                            st.error("Please check your data and column mapping, then try again.")
                                    
                                    except Exception as e:
                                        st.error(f"❌ **Import Error:** {str(e)}")
                                        st.error("Something went wrong during the import process.")
                            
                            else:
                                st.warning("⚠️ Please map at least one column to proceed with import")
                                st.info("💡 **Tip:** Select database columns from the dropdowns above to map your file data")
                                
                                st.markdown("### 🚀 **Import Section:**")
                                st.button(
                                    "🚫 No Mapping - Cannot Import",
                                    disabled=True,
                                    use_container_width=True,
                                    help="Please map at least one column first"
                                )
                        else:
                            st.error("❌ Could not fetch table structure")
                            
                except Exception as e:
                    st.error(f"❌ Error processing file: {str(e)}")
    
    except Exception as e:
        st.error(f"Main application error: {e}")
        with st.expander("🔍 Debug Information"):
            import traceback
            st.code(traceback.format_exc())

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"Application error: {e}")
        st.error("Please check your configuration and try again")
        
        with st.expander("🔍 Debug Information"):
            import traceback
            st.code(traceback.format_exc())
