import streamlit as st
import pandas as pd
import time
from datetime import datetime
import logging
from typing import Dict, List, Any

# Import custom modules
from database_config import db_manager
from file_processor import file_processor
from ui_components import ui

# Configure page
st.set_page_config(
    page_title="AI Data Import System",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for modern styling
st.markdown("""
<style>
    .main {
        padding-top: 1rem;
    }
    
    .stAlert {
        border-radius: 10px;
        border: none;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.5rem 1rem;
        font-weight: 500;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }
    
    .stSelectbox > label {
        font-weight: 500;
        color: #333;
    }
    
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border-left: 4px solid #667eea;
    }
    
    .success-card {
        background: linear-gradient(135deg, #d4edda 0%, #c3e6cb 100%);
        border-left-color: #28a745;
    }
    
    .warning-card {
        background: linear-gradient(135deg, #fff3cd 0%, #ffeaa7 100%);
        border-left-color: #ffc107;
    }
    
    .error-card {
        background: linear-gradient(135deg, #f8d7da 0%, #f5c6cb 100%);
        border-left-color: #dc3545;
    }
    
    .sidebar .stSelectbox {
        margin-bottom: 1rem;
    }
    
    .dataframe {
        border-radius: 10px;
        overflow: hidden;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    div[data-testid="metric-container"] {
        background-color: white;
        border: 1px solid #e1e4e8;
        padding: 1rem;
        border-radius: 8px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

class DataImportApp:
    def __init__(self):
        self.setup_session_state()
        self.setup_logging()
    
    def setup_session_state(self):
        """Initialize session state variables"""
        if 'uploaded_data' not in st.session_state:
            st.session_state.uploaded_data = None
        if 'file_metadata' not in st.session_state:
            st.session_state.file_metadata = {}
        if 'column_mapping' not in st.session_state:
            st.session_state.column_mapping = {}
        if 'import_history' not in st.session_state:
            st.session_state.import_history = []
        if 'db_connected' not in st.session_state:
            st.session_state.db_connected = False
    
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def run(self):
        """Main application runner"""
        # Show hero section
        ui.show_hero_section()
        
        # Sidebar
        self.show_sidebar()
        
        # Main content
        self.show_main_content()
        
        # Footer
        ui.show_footer()
    
    def show_sidebar(self):
        """Display sidebar with navigation and settings"""
        st.sidebar.markdown("## 🎛️ Control Panel")
        
        # Database connection status
        self.check_database_connection()
        
        # Navigation
        page = st.sidebar.selectbox(
            "🧭 Navigation",
            ["File Import", "Data Dashboard", "Import History", "Settings"],
            help="Choose what you want to do"
        )
        
        st.session_state.current_page = page
        
        # Quick stats if data is loaded
        if st.session_state.uploaded_data is not None:
            st.sidebar.markdown("### 📊 Current File")
            metadata = st.session_state.file_metadata
            st.sidebar.metric("Rows", f"{metadata.get('rows', 0):,}")
            st.sidebar.metric("Columns", metadata.get('columns', 0))
            st.sidebar.metric("Size", f"{metadata.get('size_bytes', 0) / 1024:.1f} KB")
        
        # Database info
        st.sidebar.markdown("### 🗄️ Database Info")
        st.sidebar.code(f"""
Host: {db_manager.DB_CONFIG['host']}
Port: {db_manager.DB_CONFIG['port']}
Database: {db_manager.DB_CONFIG['database']}
        """)
    
    def check_database_connection(self):
        """Check and display database connection status"""
        if st.sidebar.button("🔄 Test Connection"):
            with st.sidebar:
                with st.spinner("Testing connection..."):
                    st.session_state.db_connected = db_manager.test_connection()
        
        status = "🟢 Connected" if st.session_state.db_connected else "🔴 Disconnected"
        st.sidebar.markdown(f"**Database Status:** {status}")
    
    def show_main_content(self):
        """Display main content based on selected page"""
        page = st.session_state.get('current_page', 'File Import')
        
        if page == "File Import":
            self.show_file_import_page()
        elif page == "Data Dashboard":
            self.show_dashboard_page()
        elif page == "Import History":
            self.show_history_page()
        elif page == "Settings":
            self.show_settings_page()
    
    def show_file_import_page(self):
        """Main file import interface"""
        st.markdown("## 📁 File Import Wizard")
        
        # Step 1: File Upload
        uploaded_file = ui.show_file_upload_zone()
        
        if uploaded_file is not None:
            # Process file
            if st.session_state.uploaded_data is None or st.session_state.file_metadata.get('filename') != uploaded_file.name:
                self.process_uploaded_file(uploaded_file)
            
            if st.session_state.uploaded_data is not None:
                self.show_import_workflow()
    
    def process_uploaded_file(self, uploaded_file):
        """Process the uploaded file"""
        with st.spinner("🔄 Processing file..."):
            df, metadata = file_processor.read_file(uploaded_file)
            
            if df is not None:
                st.session_state.uploaded_data = df
                st.session_state.file_metadata = metadata
                st.success(f"✅ File processed successfully! Found {len(df)} rows and {len(df.columns)} columns.")
                
                # Log the upload
                self.logger.info(f"File uploaded: {metadata.get('filename')} - {len(df)} rows")
            else:
                st.error(f"❌ Error processing file: {metadata.get('error', 'Unknown error')}")
                st.session_state.uploaded_data = None
                st.session_state.file_metadata = {}
    
    def show_import_workflow(self):
        """Show the complete import workflow"""
        df = st.session_state.uploaded_data
        metadata = st.session_state.file_metadata
        
        # File analysis
        ui.show_file_analysis(metadata)
        
        # Data preview
        ui.show_data_preview(df, "📋 Data Preview")
        
        # Dashboard charts
        with st.expander("📊 Data Insights", expanded=False):
            ui.create_dashboard_charts(df)
        
        # Table selection
        st.markdown("---")
        target_table, create_new = ui.show_table_selector(
            available_tables=["R06", "R07", "R08", "R09", "R10"],
            default_table="R06"
        )
        
        # Get table information
        table_info = db_manager.get_table_info(target_table)
        
        if table_info.get('exists', False):
            st.success(f"✅ Table '{target_table}' exists in database")
            db_columns = [col['name'] for col in table_info['columns']]
        elif create_new:
            st.info(f"ℹ️ Table '{target_table}' will be created automatically")
            db_columns = list(df.columns)  # Use file columns as base
        else:
            st.error(f"❌ Table '{target_table}' does not exist")
            return
        
        # Column mapping
        st.markdown("---")
        file_columns = list(df.columns)
        suggested_mapping = file_processor.suggest_column_mapping(file_columns, db_columns)
        
        column_mapping = ui.show_column_mapping_interface(
            file_columns, db_columns, suggested_mapping
        )
        st.session_state.column_mapping = column_mapping
        
        # Import options
        st.markdown("---")
        import_options = ui.show_import_options()
        
        # Advanced options
        advanced_options = ui.show_advanced_options()
        
        # Validation and Import
        st.markdown("---")
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col1:
            if st.button("🔍 Validate Data", type="secondary", use_container_width=True):
                self.validate_data_for_import(df, column_mapping, import_options)
        
        with col2:
            if st.button("👀 Preview Import", type="secondary", use_container_width=True):
                self.preview_import_data(df, column_mapping)
        
        with col3:
            if st.button("🚀 Start Import", type="primary", use_container_width=True):
                self.start_import_process(df, target_table, column_mapping, import_options, create_new)
    
    def validate_data_for_import(self, df: pd.DataFrame, column_mapping: Dict, options: Dict):
        """Validate data before import"""
        st.markdown("### 🔍 Data Validation Results")
        
        with st.spinner("Validating data..."):
            processed_df, errors = file_processor.validate_data_for_import(df, column_mapping)
            
            warnings = []
            
            # Additional validations
            if len(processed_df) == 0:
                errors.append("No data remains after processing")
            
            # Check for missing required mappings
            unmapped_cols = [col for col in df.columns if col not in column_mapping or not column_mapping[col]]
            if unmapped_cols:
                warnings.append(f"Unmapped columns will be skipped: {', '.join(unmapped_cols)}")
            
            # Show results
            ui.show_validation_results(errors, warnings)
            
            if not errors:
                st.success("✅ Data validation completed successfully!")
                
                # Show processing stats
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Rows to Import", len(processed_df))
                with col2:
                    st.metric("Columns Mapped", len(column_mapping))
                with col3:
                    st.metric("Data Quality", "Good" if len(warnings) == 0 else "Fair")
    
    def preview_import_data(self, df: pd.DataFrame, column_mapping: Dict):
        """Preview processed data before import"""
        st.markdown("### 👀 Import Preview")
        
        with st.spinner("Preparing preview..."):
            processed_df, _ = file_processor.validate_data_for_import(df, column_mapping)
            
            if not processed_df.empty:
                ui.show_data_preview(processed_df, "🔮 Processed Data Preview")
                
                # Show mapping summary
                st.markdown("#### 🔗 Applied Column Mapping")
                mapping_summary = []
                for file_col, db_col in column_mapping.items():
                    if db_col:
                        mapping_summary.append({"File Column": file_col, "Database Column": db_col})
                
                if mapping_summary:
                    mapping_df = pd.DataFrame(mapping_summary)
                    st.dataframe(mapping_df, use_container_width=True)
            else:
                st.error("No data to preview after processing")
    
    def start_import_process(self, df: pd.DataFrame, table_name: str, column_mapping: Dict, options: Dict, create_new: bool):
        """Execute the data import process"""
        st.markdown("### 🚀 Import Process")
        
        start_time = time.time()
        
        try:
            # Validate data
            processed_df, errors = file_processor.validate_data_for_import(df, column_mapping)
            
            if errors and options.get('validate_data', True):
                st.error("❌ Data validation failed. Please fix the issues before importing.")
                ui.show_validation_results(errors)
                return
            
            # Create table if needed
            if create_new:
                with st.spinner("Creating table..."):
                    success = db_manager.create_table_from_dataframe(processed_df, table_name)
                    if not success:
                        st.error("❌ Failed to create table")
                        return
                    st.success(f"✅ Table '{table_name}' created successfully")
            
            # Import data
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            batch_size = options.get('batch_size', 1000)
            total_rows = len(processed_df)
            rows_imported = 0
            
            # Process in batches
            for i in range(0, total_rows, batch_size):
                batch = processed_df.iloc[i:i+batch_size]
                
                if options.get('show_progress', True):
                    progress = (i + len(batch)) / total_rows
                    progress_bar.progress(progress)
                    status_text.text(f"Importing batch {i//batch_size + 1}: {len(batch)} rows")
                
                # Import batch
                success, message = db_manager.insert_dataframe(
                    batch, table_name, options.get('if_exists', 'append')
                )
                
                if success:
                    rows_imported += len(batch)
                else:
                    st.error(f"❌ Failed to import batch: {message}")
                    break
            
            # Calculate statistics
            end_time = time.time()
            processing_time = end_time - start_time
            success_rate = (rows_imported / total_rows) * 100
            
            # Show results
            if rows_imported > 0:
                stats = {
                    'rows_imported': rows_imported,
                    'processing_time': processing_time,
                    'success_rate': success_rate
                }
                
                ui.show_import_summary(
                    True, 
                    f"Data imported successfully to table '{table_name}'", 
                    stats
                )
                
                # Add to history
                self.add_to_import_history({
                    'timestamp': datetime.now(),
                    'filename': st.session_state.file_metadata.get('filename', 'Unknown'),
                    'table': table_name,
                    'rows_imported': rows_imported,
                    'success': True,
                    'processing_time': processing_time
                })
                
                # Log success
                self.logger.info(f"Import completed: {rows_imported} rows to {table_name}")
                
            else:
                ui.show_import_summary(False, "No data was imported")
                
        except Exception as e:
            error_msg = f"Import failed: {str(e)}"
            ui.show_import_summary(False, error_msg)
            self.logger.error(error_msg)
    
    def show_dashboard_page(self):
        """Show data dashboard and analytics"""
        st.markdown("## 📊 Data Dashboard")
        
        if st.session_state.uploaded_data is not None:
            df = st.session_state.uploaded_data
            
            # Overview metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Records", f"{len(df):,}")
            with col2:
                st.metric("Total Columns", len(df.columns))
            with col3:
                numeric_cols = len(df.select_dtypes(include=['number']).columns)
                st.metric("Numeric Columns", numeric_cols)
            with col4:
                missing_cells = df.isnull().sum().sum()
                st.metric("Missing Values", f"{missing_cells:,}")
            
            # Interactive charts
            ui.create_dashboard_charts(df)
            
            # Data quality dashboard
            st.markdown("### 🔍 Data Quality Analysis")
            
            # Missing data heatmap
            if df.isnull().any().any():
                import plotly.graph_objects as go
                
                missing_data = df.isnull()
                
                fig = go.Figure(data=go.Heatmap(
                    z=missing_data.astype(int),
                    x=missing_data.columns,
                    y=list(range(len(missing_data))),
                    colorscale=[[0, 'white'], [1, 'red']],
                    showscale=False
                ))
                
                fig.update_layout(
                    title="Missing Data Pattern (Red = Missing)",
                    xaxis_title="Columns",
                    yaxis_title="Rows",
                    height=400
                )
                
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("📤 Please upload a file first to view the dashboard")
    
    def show_history_page(self):
        """Show import history"""
        st.markdown("## 📜 Import History")
        
        history = st.session_state.import_history
        
        if history:
            # Convert to DataFrame for display
            history_df = pd.DataFrame(history)
            history_df['timestamp'] = pd.to_datetime(history_df['timestamp'])
            history_df = history_df.sort_values('timestamp', ascending=False)
            
            # Summary stats
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Total Imports", len(history))
            with col2:
                successful = sum(1 for h in history if h['success'])
                st.metric("Successful", successful)
            with col3:
                total_rows = sum(h['rows_imported'] for h in history if h['success'])
                st.metric("Total Rows Imported", f"{total_rows:,}")
            
            # History table
            st.dataframe(
                history_df[['timestamp', 'filename', 'table', 'rows_imported', 'success', 'processing_time']],
                use_container_width=True
            )
            
            # Clear history button
            if st.button("🗑️ Clear History"):
                st.session_state.import_history = []
                st.rerun()
        else:
            st.info("No import history available yet")
    
    def show_settings_page(self):
        """Show application settings"""
        st.markdown("## ⚙️ Settings")
        
        # Database settings
        st.markdown("### 🗄️ Database Configuration")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.text_input("Host", value=db_manager.DB_CONFIG['host'], disabled=True)
            st.text_input("Database", value=db_manager.DB_CONFIG['database'], disabled=True)
            st.text_input("User", value=db_manager.DB_CONFIG['user'], disabled=True)
        
        with col2:
            st.number_input("Port", value=db_manager.DB_CONFIG['port'], disabled=True)
            connection_status = "🟢 Connected" if st.session_state.db_connected else "🔴 Disconnected"
            st.text_input("Status", value=connection_status, disabled=True)
        
        # Application settings
        st.markdown("### 🎛️ Application Settings")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Default Import Settings**")
            default_batch_size = st.number_input("Default batch size", min_value=100, max_value=10000, value=1000)
            default_if_exists = st.selectbox("Default if exists action", ["append", "replace", "fail"])
            
        with col2:
            st.markdown("**UI Preferences**")
            show_advanced_by_default = st.checkbox("Show advanced options by default", value=False)
            auto_validate = st.checkbox("Auto-validate data", value=True)
        
        # Performance settings
        st.markdown("### ⚡ Performance Settings")
        
        col1, col2 = st.columns(2)
        
        with col1:
            max_file_size = st.number_input("Max file size (MB)", min_value=1, max_value=500, value=100)
            max_preview_rows = st.number_input("Max preview rows", min_value=10, max_value=1000, value=100)
        
        with col2:
            enable_caching = st.checkbox("Enable data caching", value=True)
            parallel_processing = st.checkbox("Enable parallel processing", value=False)
        
        # Save settings
        if st.button("💾 Save Settings"):
            # In a real app, you'd save these to a config file or database
            st.success("Settings saved successfully!")
        
        # Export/Import settings
        st.markdown("### 📁 Backup & Restore")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("📤 Export Import History"):
                if st.session_state.import_history:
                    history_df = pd.DataFrame(st.session_state.import_history)
                    csv = history_df.to_csv(index=False)
                    st.download_button(
                        label="Download History CSV",
                        data=csv,
                        file_name=f"import_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
                else:
                    st.warning("No history to export")
        
        with col2:
            if st.button("🗑️ Reset Application"):
                if st.checkbox("I understand this will clear all data"):
                    st.session_state.clear()
                    st.success("Application reset successfully!")
                    st.rerun()
        
        # System information
        st.markdown("### 🔧 System Information")
        
        system_info = {
            "Python Version": "3.8+",
            "Streamlit Version": st.__version__,
            "Database Type": "MySQL",
            "Supported File Types": ", ".join(['.csv', '.xlsx', '.xls']),
            "Max Concurrent Imports": "1",
            "Session Active": "Yes" if st.session_state else "No"
        }
        
        for key, value in system_info.items():
            col1, col2 = st.columns([1, 2])
            with col1:
                st.text(key)
            with col2:
                st.code(value)
    
    def add_to_import_history(self, import_record: Dict):
        """Add import record to history"""
        st.session_state.import_history.append(import_record)
        
        # Keep only last 100 records
        if len(st.session_state.import_history) > 100:
            st.session_state.import_history = st.session_state.import_history[-100:]

def main():
    """Main function to run the application"""
    try:
        app = DataImportApp()
        app.run()
    except Exception as e:
        st.error(f"❌ Application Error: {str(e)}")
        st.exception(e)

if __name__ == "__main__":
    main()
