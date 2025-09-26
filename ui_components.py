import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import Dict, List, Any
import json

class UIComponents:
    def __init__(self):
        self.primary_color = "#1f77b4"
        self.success_color = "#2ca02c"
        self.warning_color = "#ff7f0e"
        self.error_color = "#d62728"
        
    def show_hero_section(self):
        """Display hero section with modern design"""
        st.markdown("""
        <div style="
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 2rem;
            border-radius: 10px;
            margin-bottom: 2rem;
            color: white;
            text-align: center;
        ">
            <h1 style="font-size: 2.5rem; margin-bottom: 0.5rem; font-weight: 300;">
                🚀 AI-Powered Data Import System
            </h1>
            <p style="font-size: 1.2rem; opacity: 0.9; margin: 0;">
                Smart file processing with auto-mapping and validation
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    def show_file_upload_zone(self):
        """Enhanced file upload area"""
        st.markdown("""
        <style>
        .upload-zone {
            border: 2px dashed #667eea;
            border-radius: 10px;
            padding: 2rem;
            text-align: center;
            background: linear-gradient(145deg, #f8f9ff, #e8eeff);
            margin: 1rem 0;
            transition: all 0.3s ease;
        }
        .upload-zone:hover {
            border-color: #764ba2;
            background: linear-gradient(145deg, #f0f2ff, #e0e5ff);
        }
        </style>
        """, unsafe_allow_html=True)
        
        with st.container():
            st.markdown('<div class="upload-zone">', unsafe_allow_html=True)
            st.markdown("### 📁 Upload Your Data File")
            st.markdown("Supported formats: CSV, Excel (.xlsx, .xls)")
            
            uploaded_file = st.file_uploader(
                "Choose a file",
                type=['csv', 'xlsx', 'xls'],
                help="Select your data file to import into the database"
            )
            st.markdown('</div>', unsafe_allow_html=True)
            
        return uploaded_file
    
    def show_file_analysis(self, metadata: Dict):
        """Display file analysis with beautiful cards"""
        if not metadata:
            return
            
        st.markdown("### 📊 File Analysis")
        
        # Main stats
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            self._create_metric_card(
                "📄 File Size", 
                f"{metadata.get('size_bytes', 0) / 1024:.1f} KB",
                "success"
            )
        
        with col2:
            self._create_metric_card(
                "📏 Rows", 
                f"{metadata.get('rows', 0):,}",
                "info"
            )
        
        with col3:
            self._create_metric_card(
                "📋 Columns", 
                f"{metadata.get('columns', 0)}",
                "info"
            )
        
        with col4:
            self._create_metric_card(
                "💾 Memory", 
                f"{metadata.get('memory_usage_mb', 0):.2f} MB",
                "warning"
            )
        
        # Data quality overview
        if 'null_counts' in metadata:
            self._show_data_quality_overview(metadata)
    
    def _create_metric_card(self, title: str, value: str, color_type: str = "info"):
        """Create a metric card with custom styling"""
        colors = {
            "success": "#d4edda",
            "info": "#d1ecf1", 
            "warning": "#fff3cd",
            "error": "#f8d7da"
        }
        
        bg_color = colors.get(color_type, colors["info"])
        
        st.markdown(f"""
        <div style="
            background-color: {bg_color};
            padding: 1rem;
            border-radius: 8px;
            text-align: center;
            border-left: 4px solid {self.primary_color};
        ">
            <h4 style="margin: 0; color: #495057;">{title}</h4>
            <h2 style="margin: 0.5rem 0 0 0; color: #212529;">{value}</h2>
        </div>
        """, unsafe_allow_html=True)
    
    def _show_data_quality_overview(self, metadata: Dict):
        """Show data quality metrics"""
        st.markdown("#### 🔍 Data Quality Overview")
        
        null_counts = metadata.get('null_counts', {})
        data_types = metadata.get('data_types', {})
        
        if null_counts:
            # Create null percentage chart
            null_data = []
            for col, null_info in null_counts.items():
                null_data.append({
                    'Column': col,
                    'Null_Percentage': null_info.get('percentage', 0),
                    'Null_Count': null_info.get('count', 0)
                })
            
            null_df = pd.DataFrame(null_data)
            
            if len(null_df) > 0:
                fig = px.bar(
                    null_df.head(10), 
                    x='Column', 
                    y='Null_Percentage',
                    title="Missing Data Percentage by Column (Top 10)",
                    color='Null_Percentage',
                    color_continuous_scale='Reds'
                )
                fig.update_layout(
                    height=400,
                    xaxis_tickangle=-45,
                    title_x=0.5
                )
                st.plotly_chart(fig, use_container_width=True)
    
    def show_column_mapping_interface(self, file_columns: List[str], db_columns: List[str], suggested_mapping: Dict[str, str]):
        """Interactive column mapping interface"""
        st.markdown("### 🔗 Column Mapping")
        st.markdown("Map your file columns to database columns:")
        
        mapping = {}
        
        # Create mapping interface with better UX
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.markdown("**📁 File Columns**")
        with col2:
            st.markdown("**🗄️ Database Columns**")
        
        # Add "Skip" option to database columns
        db_options = ["(Skip this column)"] + db_columns
        
        for i, file_col in enumerate(file_columns):
            col1, col2 = st.columns([1, 1])
            
            with col1:
                st.markdown(f"`{file_col}`")
            
            with col2:
                # Pre-select suggested mapping if available
                default_index = 0
                if file_col in suggested_mapping:
                    suggested_db_col = suggested_mapping[file_col]
                    if suggested_db_col in db_columns:
                        default_index = db_columns.index(suggested_db_col) + 1
                
                selected = st.selectbox(
                    f"Map to:",
                    options=db_options,
                    index=default_index,
                    key=f"mapping_{i}",
                    label_visibility="collapsed"
                )
                
                if selected != "(Skip this column)":
                    mapping[file_col] = selected
        
        return mapping
    
    def show_data_preview(self, df: pd.DataFrame, title: str = "Data Preview"):
        """Show data preview with enhanced formatting"""
        if df.empty:
            st.warning("No data to preview")
            return
            
        st.markdown(f"### 👀 {title}")
        
        # Show basic info
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Rows", len(df))
        with col2:
            st.metric("Total Columns", len(df.columns))
        with col3:
            st.metric("Memory Usage", f"{df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        
        # Show preview with pagination
        preview_rows = st.slider("Rows to preview", min_value=5, max_value=min(100, len(df)), value=10)
        
        # Apply styling to the dataframe
        styled_df = df.head(preview_rows).style.apply(self._highlight_nulls, axis=None)
        st.dataframe(styled_df, use_container_width=True, height=400)
        
        # Show column information
        with st.expander("📋 Column Information"):
            col_info = []
            for col in df.columns:
                col_info.append({
                    'Column': col,
                    'Type': str(df[col].dtype),
                    'Non-Null': df[col].count(),
                    'Null': df[col].isnull().sum(),
                    'Unique': df[col].nunique()
                })
            
            col_df = pd.DataFrame(col_info)
            st.dataframe(col_df, use_container_width=True)
    
    def _highlight_nulls(self, df):
        """Highlight null values in dataframe"""
        return df.isnull().applymap(lambda x: 'background-color: #ffebee' if x else '')
    
    def show_validation_results(self, errors: List[str], warnings: List[str] = None):
        """Display validation results with proper formatting"""
        if errors:
            st.markdown("### ⚠️ Validation Issues")
            for error in errors:
                st.error(f"❌ {error}")
        
        if warnings:
            st.markdown("### ⚡ Warnings")
            for warning in warnings:
                st.warning(f"⚠️ {warning}")
        
        if not errors and not warnings:
            st.success("✅ All validations passed!")
    
    def show_import_progress(self, progress_text: str, progress_value: float = None):
        """Show import progress with animations"""
        if progress_value is not None:
            progress_bar = st.progress(progress_value)
            st.text(progress_text)
        else:
            with st.spinner(progress_text):
                st.empty()
    
    def show_import_summary(self, success: bool, message: str, stats: Dict = None):
        """Show import completion summary"""
        if success:
            st.balloons()
            st.success(f"🎉 {message}")
            
            if stats:
                st.markdown("### 📈 Import Statistics")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Rows Imported", stats.get('rows_imported', 0))
                with col2:
                    st.metric("Processing Time", f"{stats.get('processing_time', 0):.2f}s")
                with col3:
                    st.metric("Success Rate", f"{stats.get('success_rate', 100):.1f}%")
        else:
            st.error(f"❌ {message}")
    
    def show_database_status(self, is_connected: bool, db_info: Dict = None):
        """Show database connection status"""
        if is_connected:
            st.success("🟢 Database Connected")
            if db_info:
                with st.expander("Database Information"):
                    st.json(db_info)
        else:
            st.error("🔴 Database Connection Failed")
    
    def show_table_selector(self, available_tables: List[str], default_table: str = "R06"):
        """Table selection interface"""
        st.markdown("### 🗄️ Select Target Table")
        
        selected_table = st.selectbox(
            "Choose destination table:",
            options=available_tables if available_tables else [default_table],
            index=0 if default_table in available_tables else 0,
            help="Select the database table where data will be imported"
        )
        
        # Option to create new table
        create_new = st.checkbox("Create new table if it doesn't exist")
        
        return selected_table, create_new
    
    def show_import_options(self):
        """Show import configuration options"""
        st.markdown("### ⚙️ Import Options")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if_exists = st.selectbox(
                "If data exists:",
                options=["append", "replace", "fail"],
                index=0,
                help="Choose what to do if the table already has data"
            )
            
            batch_size = st.number_input(
                "Batch size:",
                min_value=100,
                max_value=10000,
                value=1000,
                step=100,
                help="Number of rows to process at once"
            )
        
        with col2:
            validate_data = st.checkbox(
                "Validate data before import",
                value=True,
                help="Perform data validation checks"
            )
            
            show_progress = st.checkbox(
                "Show detailed progress",
                value=True,
                help="Display progress information during import"
            )
        
        return {
            "if_exists": if_exists,
            "batch_size": batch_size,
            "validate_data": validate_data,
            "show_progress": show_progress
        }
    
    def create_dashboard_charts(self, df: pd.DataFrame):
        """Create dashboard charts for data visualization"""
        if df.empty:
            return
            
        st.markdown("### 📊 Data Insights Dashboard")
        
        # Numeric columns analysis
        numeric_cols = df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            col1, col2 = st.columns(2)
            
            with col1:
                # Distribution plot
                selected_col = st.selectbox("Select column for distribution:", numeric_cols)
                if selected_col:
                    fig = px.histogram(
                        df, 
                        x=selected_col, 
                        title=f"Distribution of {selected_col}",
                        nbins=30
                    )
                    fig.update_layout(height=400, title_x=0.5)
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Summary statistics
                st.markdown("**Summary Statistics**")
                stats_df = df[numeric_cols].describe()
                st.dataframe(stats_df, use_container_width=True)
        
        # Categorical data analysis
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns
        if len(categorical_cols) > 0:
            selected_cat_col = st.selectbox("Select categorical column:", categorical_cols)
            if selected_cat_col:
                value_counts = df[selected_cat_col].value_counts().head(10)
                
                fig = px.bar(
                    x=value_counts.index,
                    y=value_counts.values,
                    title=f"Top 10 values in {selected_cat_col}",
                    labels={'x': selected_cat_col, 'y': 'Count'}
                )
                fig.update_layout(height=400, title_x=0.5, xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
    
    def show_advanced_options(self):
        """Show advanced configuration options"""
        with st.expander("🔧 Advanced Options"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Data Processing**")
                handle_duplicates = st.selectbox(
                    "Handle duplicates:",
                    ["keep_all", "drop_duplicates", "mark_duplicates"]
                )
                
                date_format = st.text_input(
                    "Date format (optional):",
                    placeholder="e.g., %Y-%m-%d %H:%M:%S"
                )
            
            with col2:
                st.markdown("**Performance**")
                chunk_size = st.number_input(
                    "Processing chunk size:",
                    min_value=500,
                    max_value=50000,
                    value=5000
                )
                
                parallel_processing = st.checkbox(
                    "Enable parallel processing",
                    value=False
                )
            
            return {
                "handle_duplicates": handle_duplicates,
                "date_format": date_format if date_format else None,
                "chunk_size": chunk_size,
                "parallel_processing": parallel_processing
            }
    
    def show_footer(self):
        """Show application footer"""
        st.markdown("---")
        st.markdown("""
        <div style="text-align: center; color: #666; padding: 1rem;">
            <p>🚀 AI-Powered Data Import System | Built with Streamlit & Modern UI/UX</p>
            <p>💡 Intelligent file processing • 🔗 Smart column mapping • ⚡ High performance</p>
        </div>
        """, unsafe_allow_html=True)

# Global UI components instance
ui = UIComponents()
