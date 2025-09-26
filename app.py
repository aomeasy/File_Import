import streamlit as st
import pandas as pd
import mysql.connector
from mysql.connector import Error
from database import DatabaseManager
from file_processor import FileProcessor
import os
from datetime import datetime

# Configure page
st.set_page_config(
    page_title="AI Data Import Hub",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
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
    
    .metric-card {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
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
</style>
""", unsafe_allow_html=True)

def main():
    # Header
    st.markdown("""
    <div class="main-header">
        <h1>🚀 AI Data Import Hub</h1>
        <p>Modern file import system powered by AI-driven interface</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Initialize components
    db_manager = DatabaseManager()
    file_processor = FileProcessor()
    
    # Sidebar configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Connection status
        if db_manager.test_connection():
            st.markdown("""
            <div class="status-success">
                ✅ Database Connected
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="status-error">
                ❌ Database Connection Failed
            </div>
            """, unsafe_allow_html=True)
            st.error("Please check database configuration")
            return
        
        # Get available tables with info
        try:
            tables_info = db_manager.get_tables_with_info()
            tables = [table['TABLE_NAME'] for table in tables_info] if tables_info else []
        except Exception as e:
            st.error(f"Error getting table info: {e}")
            tables_info = []
            tables = db_manager.get_tables()  # Fallback to simple table list
        
        st.write(f"📊 Available Tables: {len(tables)}")
        
        # Show table information
        if tables_info and len(tables_info) > 0:
            st.subheader("📋 Tables Info")
            
            for table_info in tables_info[:5]:  # Show first 5 tables
                table_name = table_info.get('TABLE_NAME', 'Unknown')
                row_count = table_info.get('TABLE_ROWS', 0) or 0
                update_time = table_info.get('UPDATE_TIME')
                
                with st.expander(f"📄 {table_name}", expanded=False):
                    st.write(f"**Rows:** {row_count:,}")
                    
                    if update_time:
                        try:
                            # Format datetime
                            if isinstance(update_time, str):
                                st.write(f"**Last Update:** {update_time}")
                            else:
                                formatted_time = update_time.strftime("%Y-%m-%d %H:%M:%S")
                                st.write(f"**Last Update:** {formatted_time}")
                        except:
                            st.write(f"**Last Update:** {str(update_time)}")
                    else:
                        st.write("**Last Update:** No data")
                    
                    # Show data size
                    data_length = table_info.get('DATA_LENGTH', 0) or 0
                    if data_length and data_length > 0:
                        size_mb = data_length / (1024 * 1024)
                        if size_mb > 1:
                            st.write(f"**Size:** {size_mb:.1f} MB")
                        else:
                            size_kb = data_length / 1024
                            st.write(f"**Size:** {size_kb:.1f} KB")
                    else:
                        st.write("**Size:** Unknown")
            
            if len(tables_info) > 5:
                st.info(f"... and {len(tables_info) - 5} more tables")
        elif len(tables) > 0:
            st.subheader("📋 Tables")
            for table in tables[:10]:
                st.write(f"📄 {table}")
            if len(tables) > 10:
                st.info(f"... and {len(tables) - 10} more tables")
        
        st.markdown("---")
    
    # Main content
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("📁 File Import")
        
        # Table selection with enhanced info
        selected_table = st.selectbox(
            "🎯 Select Target Table",
            options=[""] + tables,
            help="Choose the table where you want to import your data",
            format_func=lambda x: x if x == "" else f"📄 {x}"
        )
        
        # Show detailed table info when selected
        if selected_table and tables_info:
            # Find table info
            table_details = None
            for table_info in tables_info:
                if table_info.get('TABLE_NAME') == selected_table:
                    table_details = table_info
                    break
            
            if table_details:
                # Create info box
                col_info1, col_info2 = st.columns(2)
                
                with col_info1:
                    row_count = table_details.get('TABLE_ROWS', 0) or 0
                    st.metric("📊 Rows", f"{row_count:,}")
                
                with col_info2:
                    update_time = table_details.get('UPDATE_TIME')
                    if update_time:
                        try:
                            if isinstance(update_time, str):
                                last_update = update_time[:10] if len(update_time) > 10 else update_time
                            else:
                                last_update = update_time.strftime("%Y-%m-%d")
                            st.metric("🕒 Last Update", last_update)
                        except:
                            st.metric("🕒 Last Update", "Unknown")
                    else:
                        st.metric("🕒 Last Update", "No data")
                
                # Show size info
                data_length = table_details.get('DATA_LENGTH', 0) or 0
                if data_length and data_length > 0:
                    size_mb = data_length / (1024 * 1024)
                    if size_mb > 1:
                        st.info(f"💾 Table Size: {size_mb:.1f} MB")
                    else:
                        size_kb = data_length / 1024
                        st.info(f"💾 Table Size: {size_kb:.1f} KB")
                else:
                    st.info("💾 Table Size: Unknown")
        
        if selected_table:
            # Show table preview
            st.subheader(f"👀 Preview: {selected_table}")
            
            if st.button("🔄 Show Last 5 Rows", type="secondary"):
                try:
                    preview_data = db_manager.get_table_preview(selected_table)
                    if not preview_data.empty:
                        st.markdown('<div class="table-preview">', unsafe_allow_html=True)
                        st.dataframe(
                            preview_data,
                            use_container_width=True,
                            hide_index=True
                        )
                        st.markdown('</div>', unsafe_allow_html=True)
                        
                        # Show table info
                        st.info(f"📊 Table has {len(preview_data.columns)} columns and showing last 5 rows")
                    else:
                        st.warning("📭 Table is empty")
                except Exception as e:
                    st.error(f"❌ Error fetching table data: {str(e)}")
            
            # File upload
            st.subheader("📤 Upload File")
            uploaded_file = st.file_uploader(
                "Choose a file to import",
                type=['csv', 'xlsx', 'xls'],
                help="Supported formats: CSV, Excel (.xlsx, .xls)"
            )
            
            if uploaded_file:
                try:
                    # Process file
                    df = file_processor.process_file(uploaded_file)
                    
                    if df is not None:
                        st.success(f"✅ File loaded successfully! {len(df)} rows, {len(df.columns)} columns")
                        
                        # Show file preview
                        st.subheader("📋 File Preview")
                        st.dataframe(df.head(), use_container_width=True, hide_index=True)
                        
                        # Column mapping section
                        st.subheader("🔗 Column Mapping")
                        st.info("📌 Map your file columns to database table columns before importing")
                        
                        # Get table columns
                        table_columns = db_manager.get_table_columns(selected_table)
                        
                        if table_columns:
                            mapping = {}
                            
                            # Show column comparison
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
                            
                            # Mapping interface
                            st.markdown("### 🎯 **Map Your Columns:**")
                            
                            # Create auto-mapping logic
                            def auto_map_columns(file_cols, db_cols):
                                auto_mapping = {}
                                db_col_names = [col['COLUMN_NAME'].lower() for col in db_cols]
                                
                                for file_col in file_cols:
                                    file_col_clean = file_col.lower().strip()
                                    
                                    # Try exact match first
                                    for db_col in db_cols:
                                        if file_col_clean == db_col['COLUMN_NAME'].lower():
                                            auto_mapping[file_col] = db_col['COLUMN_NAME']
                                            break
                                    
                                    # If no exact match, try partial match
                                    if file_col not in auto_mapping:
                                        for db_col in db_cols:
                                            db_col_name = db_col['COLUMN_NAME'].lower()
                                            if (file_col_clean in db_col_name or 
                                                db_col_name in file_col_clean or
                                                file_col_clean.replace('_', '') == db_col_name.replace('_', '')):
                                                auto_mapping[file_col] = db_col['COLUMN_NAME']
                                                break
                                
                                return auto_mapping
                            
                            # Get auto-mapping suggestions
                            auto_mapping = auto_map_columns(df.columns, table_columns)
                            
                            # Create mapping dropdowns with auto-suggestions
                            for i, file_col in enumerate(df.columns):
                                map_col1, map_col2 = st.columns([1, 1])
                                
                                with map_col1:
                                    st.write(f"📄 **{file_col}**")
                                
                                with map_col2:
                                    # Prepare options
                                    db_options = ["🚫 Skip this column"] + [col['COLUMN_NAME'] for col in table_columns]
                                    
                                    # Set default index based on auto-mapping
                                    default_index = 0  # Skip by default
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
                            
                            # Show mapping summary and import button
                            st.markdown("---")
                            
                            if mapping:
                                st.markdown("### 📋 **Mapping Summary:**")
                                mapping_df = pd.DataFrame([
                                    {"File Column": file_col, "→": "→", "Database Column": db_col}
                                    for file_col, db_col in mapping.items()
                                ])
                                st.dataframe(mapping_df, use_container_width=True, hide_index=True)
                                
                                # Import section - ALWAYS SHOW THIS
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
                                
                                # Import process
                                if import_button:
                                    try:
                                        with st.spinner("🔄 Importing data to database..."):
                                            import time
                                            time.sleep(1)  # Show spinner
                                            result = db_manager.import_data(selected_table, df, mapping)
                                        
                                        if result['success']:
                                            st.success(f"🎉 **Import Successful!** \n\n✅ Imported **{result['rows_affected']}** rows into `{selected_table}` table")
                                            st.balloons()
                                            
                                            # Show updated table preview
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
                                
                                # Show empty import button (disabled state)
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
    
    with col2:
        st.header("📊 Dashboard")
        
        # Stats
        if tables:
            st.markdown(f"""
            <div class="metric-card">
                <h3>{len(tables)}</h3>
                <p>Available Tables</p>
            </div>
            """, unsafe_allow_html=True)
        
        # Recent activity (placeholder)
        st.subheader("🕒 Recent Activity")
        st.info("Import activity will appear here")
        
        # Quick actions
        st.subheader("⚡ Quick Actions")
        if st.button("🔄 Refresh Tables", use_container_width=True):
            st.rerun()
        
        if st.button("📋 View All Tables", use_container_width=True):
            if tables_info and len(tables_info) > 0:
                st.subheader("📊 All Tables Summary")
                
                # Create summary DataFrame
                summary_data = []
                for table_info in tables_info:
                    update_time = table_info.get('UPDATE_TIME')
                    table_name = table_info.get('TABLE_NAME', 'Unknown')
                    row_count = table_info.get('TABLE_ROWS', 0) or 0
                    
                    if update_time:
                        try:
                            if isinstance(update_time, str):
                                update_str = update_time[:10] if len(update_time) > 10 else update_time
                            else:
                                update_str = update_time.strftime("%Y-%m-%d")
                        except:
                            update_str = "Unknown"
                    else:
                        update_str = "No data"
                    
                    summary_data.append({
                        "Table": table_name,
                        "Rows": f"{row_count:,}",
                        "Last Update": update_str
                    })
                
                if summary_data:
                    summary_df = pd.DataFrame(summary_data)
                    st.dataframe(summary_df, use_container_width=True, hide_index=True)
                else:
                    st.write("No table data available")
            elif len(tables) > 0:
                st.write("**Available Tables:**")
                for table in tables:
                    st.write(f"• {table}")
            else:
                st.write("No tables found")

if __name__ == "__main__":
    main()
