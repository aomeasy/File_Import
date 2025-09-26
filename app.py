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
        
        # Get available tables
        tables = db_manager.get_tables()
        st.write(f"📊 Available Tables: {len(tables)}")
    
    # Main content
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("📁 File Import")
        
        # Table selection
        selected_table = st.selectbox(
            "🎯 Select Target Table",
            options=[""] + tables,
            help="Choose the table where you want to import your data"
        )
        
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
                        
                        # Get table columns
                        table_columns = db_manager.get_table_columns(selected_table)
                        
                        if table_columns:
                            mapping = {}
                            col_map1, col_map2 = st.columns(2)
                            
                            with col_map1:
                                st.write("**File Columns:**")
                                for col in df.columns:
                                    st.write(f"• {col}")
                            
                            with col_map2:
                                st.write("**Table Columns:**")
                                for col_info in table_columns:
                                    col_name = col_info['COLUMN_NAME']
                                    col_type = col_info['DATA_TYPE']
                                    nullable = "NULL" if col_info['IS_NULLABLE'] == 'YES' else "NOT NULL"
                                    st.write(f"• {col_name} ({col_type}) - {nullable}")
                            
                            # Create mapping interface
                            st.write("**Map File Columns to Table Columns:**")
                            for file_col in df.columns:
                                mapped_col = st.selectbox(
                                    f"Map '{file_col}' to:",
                                    options=["-- Skip --"] + [col['COLUMN_NAME'] for col in table_columns],
                                    key=f"mapping_{file_col}"
                                )
                                if mapped_col != "-- Skip --":
                                    mapping[file_col] = mapped_col
                            
                            # Import button
                            if mapping and st.button("🚀 Import Data", type="primary"):
                                try:
                                    with st.spinner("Importing data..."):
                                        result = db_manager.import_data(selected_table, df, mapping)
                                        
                                    if result['success']:
                                        st.success(f"🎉 Successfully imported {result['rows_affected']} rows!")
                                        st.balloons()
                                        
                                        # Show updated table preview
                                        st.subheader("📊 Updated Table Preview")
                                        updated_preview = db_manager.get_table_preview(selected_table)
                                        st.dataframe(updated_preview, use_container_width=True, hide_index=True)
                                        
                                    else:
                                        st.error(f"❌ Import failed: {result['error']}")
                                        
                                except Exception as e:
                                    st.error(f"❌ Import error: {str(e)}")
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
            if tables:
                for table in tables:
                    st.write(f"• {table}")
            else:
                st.write("No tables found")

if __name__ == "__main__":
    main()
