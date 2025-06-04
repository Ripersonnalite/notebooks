import streamlit as st
import os
import pandas as pd
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, asc
import pyspark.sql.functions as F
import time
import sys
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, BooleanType, TimestampType

# Must be the first Streamlit command
st.set_page_config(page_title="PySpark DB Manager", layout="wide")

# Path to the warehouse
WAREHOUSE_PATH = os.path.expanduser("~/spark_warehouse_streamlit")

# Create Spark session
@st.cache_resource
def create_spark_session():
    """Create and return a Spark session"""
    try:
        # Set up Spark with more memory
        spark = (SparkSession.builder
                .appName("PySpark DB Manager")
                .config("spark.sql.warehouse.dir", WAREHOUSE_PATH)
                .config("spark.driver.memory", "4g")  # Increase driver memory
                .config("spark.executor.memory", "4g")  # Increase executor memory
                .config("spark.sql.adaptive.enabled", "true")  # Enable adaptive query execution
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")  # Faster pandas conversion
                .config("spark.driver.maxResultSize", "1g")  # Increase max result size
                .config("spark.network.timeout", "800s")  # Increase network timeout
                .config("spark.executor.heartbeatInterval", "60s")  # Increase heartbeat interval
                .enableHiveSupport()
                .getOrCreate())
        
        # Set a default timeout for queries
        spark.conf.set("spark.sql.execution.timeout", "60s")
        
        return spark
    except Exception as e:
        st.error(f"Error creating Spark session: {str(e)}")
        # Create a basic session as fallback
        return SparkSession.builder.appName("PySpark DB Manager Fallback").getOrCreate()

# Function to check if Spark session is active and reconnect if needed
def ensure_spark_connection():
    """Ensure Spark session is active, recreate if needed"""
    try:
        # Try to use the existing session
        if "spark" in st.session_state and st.session_state.spark is not None:
            # Test if the connection is still alive
            try:
                st.session_state.spark.sql("SELECT 1").collect()
                return st.session_state.spark
            except:
                st.warning("⚠️ Lost connection to Spark. Reconnecting...")
                # If test fails, create a new session
                st.session_state.spark = create_spark_session()
                return st.session_state.spark
        else:
            # If no session exists, create one
            st.session_state.spark = create_spark_session()
            return st.session_state.spark
    except Exception as e:
        st.error(f"Failed to connect to Spark: {str(e)}")
        if "JAVA_OPTS" not in os.environ:
            st.info("💡 Try running with more memory: `JAVA_OPTS=\"-Xmx8g\" streamlit run pyspark_db_manager.py`")
        sys.exit(1)

# Get all databases in the warehouse
def get_databases(spark):
    """Return a list of all databases"""
    result = spark.sql("SHOW DATABASES").collect()
    
    # Check the actual column name in the result
    if result and hasattr(result[0], "databaseName"):
        return [row.databaseName for row in result]
    elif result and hasattr(result[0], "namespace"):
        return [row.namespace for row in result]
    elif result:
        # If we can't determine the column name, get the first column
        first_col = result[0].__fields__[0]
        return [getattr(row, first_col) for row in result]
    else:
        return []

# Get tables in a database
def get_tables(spark, database):
    """Return a list of all tables in a database"""
    try:
        spark.sql(f"USE {database}")
        result = spark.sql("SHOW TABLES").collect()
        
        # Check column names and extract table names
        if result:
            if hasattr(result[0], "tableName"):
                return [row.tableName for row in result]
            elif hasattr(result[0], "name"):
                return [row.name for row in result]
            elif result[0].__fields__:
                # If we can't determine the exact column name, get the last column
                # (in many Spark versions, the table name is the last column)
                col_name = result[0].__fields__[-1]
                return [getattr(row, col_name) for row in result]
        
        return []
    except Exception as e:
        st.error(f"Error getting tables for database {database}: {str(e)}")
        return []

# Get table schema for a table
def get_table_schema(spark, database, table):
    """Return the schema for a table"""
    try:
        spark.sql(f"USE {database}")
        return spark.table(f"{table}").schema
    except Exception as e:
        st.error(f"Error getting table schema: {str(e)}")
        return None

# Get sample data from a table
def get_table_sample(spark, database, table, limit=10):
    """Return a sample of rows from a table"""
    try:
        spark.sql(f"USE {database}")
        
        # First check if table exists
        if not spark.catalog.tableExists(table):
            st.warning(f"Table '{table}' does not exist in database '{database}'")
            return pd.DataFrame()
        
        # Get column names first to avoid fetching all data
        columns_query = f"SELECT * FROM {table} LIMIT 0"
        schema = spark.sql(columns_query).schema
        column_names = [field.name for field in schema.fields]
        
        # If there are too many columns, select only the first 20
        if len(column_names) > 20:
            selected_columns = column_names[:20]
            st.warning(f"Table has {len(column_names)} columns. Showing only the first 20 to avoid memory issues.")
            columns_str = ", ".join(selected_columns)
            query = f"SELECT {columns_str} FROM {table} LIMIT {limit}"
        else:
            query = f"SELECT * FROM {table} LIMIT {limit}"
        
        # Execute the query with a timeout
        spark.conf.set("spark.sql.execution.timeout", "60s")
        result = spark.sql(query)
        
        # Convert to pandas DataFrame
        return result.toPandas()
    except Exception as e:
        st.error(f"Error getting sample data: {str(e)}")
        return pd.DataFrame(columns=column_names if 'column_names' in locals() else [])

# Get row count for a table
def get_table_count(spark, database, table):
    """Return the number of rows in a table"""
    try:
        spark.sql(f"USE {database}")
        
        # First check if table exists
        if not spark.catalog.tableExists(table):
            st.warning(f"Table '{table}' does not exist in database '{database}'")
            return 0
        
        # For large tables, use approximate count to avoid memory issues
        try:
            # First try with a fast approximate count
            approx_result = spark.sql(f"SELECT COUNT(1) as count FROM {table} TABLESAMPLE (1 PERCENT)")
            sample_count = approx_result.collect()[0]['count']
            
            # If the sample is small, do an exact count
            if sample_count < 100:
                result = spark.sql(f"SELECT COUNT(1) as count FROM {table}")
                return int(result.collect()[0]['count'])
            else:
                # Scale up the approximate count
                st.info("Using approximate row count for large table")
                return int(sample_count * 100)
        except:
            # Fall back to standard count with timeout
            spark.conf.set("spark.sql.execution.timeout", "30s")
            result = spark.sql(f"SELECT COUNT(1) as count FROM {table}")
            return int(result.collect()[0]['count'])
    except Exception as e:
        st.error(f"Error getting row count: {str(e)}")
        return 0

# Execute custom SQL query
def execute_query(spark, query):
    """Execute a custom SQL query and return results"""
    try:
        # Set a timeout for the query
        spark.conf.set("spark.sql.execution.timeout", "120s")
        
        result = spark.sql(query)
        
        # For queries that return data, convert to pandas
        if hasattr(result, "toPandas"):
            # Limit result size to avoid memory issues
            if hasattr(result, "count"):
                row_count = result.count()
                if row_count > 10000:
                    st.warning(f"Query returned {row_count:,} rows. Limiting to 10,000 rows to avoid memory issues.")
                    result = result.limit(10000)
            
            return result.toPandas()
        return pd.DataFrame({"result": ["Query executed successfully"]})
    except Exception as e:
        return str(e)

# Create a new table
def create_table(spark, database, table_name, schema_str, format_type="parquet"):
    """Create a new table with the specified schema"""
    try:
        spark.sql(f"USE {database}")
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} ({schema_str})
        USING {format_type}
        """)
        return True, f"Table '{table_name}' created successfully"
    except Exception as e:
        return False, str(e)

# Drop a table
def drop_table(spark, database, table_name):
    """Drop a table"""
    try:
        spark.sql(f"USE {database}")
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        return True, f"Table '{table_name}' dropped successfully"
    except Exception as e:
        return False, str(e)

# Truncate a table
def truncate_table(spark, database, table_name):
    """Truncate a table (remove all rows)"""
    try:
        spark.sql(f"USE {database}")
        spark.sql(f"TRUNCATE TABLE {table_name}")
        return True, f"Table '{table_name}' truncated successfully"
    except Exception as e:
        return False, str(e)

# Add a column to a table
def add_column(spark, database, table_name, column_name, column_type):
    """Add a new column to a table"""
    try:
        spark.sql(f"USE {database}")
        spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({column_name} {column_type})")
        return True, f"Column '{column_name}' added to '{table_name}'"
    except Exception as e:
        return False, str(e)

# Drop a column from a table
def drop_column(spark, database, table_name, column_name):
    """Drop a column from a table"""
    try:
        # Get current columns
        schema = get_table_schema(spark, database, table_name)
        columns = [field.name for field in schema.fields if field.name != column_name]
        
        if len(columns) == 0:
            return False, "Cannot drop all columns from a table"
        
        # Create a new table without the column
        column_str = ", ".join(columns)
        
        # Create a temporary view
        spark.sql(f"""
        CREATE OR REPLACE TEMPORARY VIEW temp_view AS
        SELECT {column_str} FROM {table_name}
        """)
        
        # Drop the original table
        spark.sql(f"DROP TABLE {table_name}")
        
        # Create the new table from the temp view
        spark.sql(f"""
        CREATE TABLE {table_name} AS
        SELECT * FROM temp_view
        """)
        
        return True, f"Column '{column_name}' dropped from '{table_name}'"
    except Exception as e:
        return False, str(e)

# Insert data into a table
def insert_data(spark, database, table_name, data_df):
    """Insert data into a table from a pandas DataFrame"""
    try:
        spark.sql(f"USE {database}")
        # Convert pandas DataFrame to Spark DataFrame
        spark_df = spark.createDataFrame(data_df)
        # Insert data into table
        spark_df.write.insertInto(table_name)
        return True, f"Data inserted into '{table_name}' successfully"
    except Exception as e:
        return False, str(e)

# Generate CREATE TABLE statement
def generate_create_table_statement(spark, database, table_name):
    """Generate a CREATE TABLE statement for an existing table"""
    try:
        spark.sql(f"USE {database}")
        # Get table schema
        schema = get_table_schema(spark, database, table_name)
        # Build column definitions
        columns = []
        for field in schema.fields:
            columns.append(f"{field.name} {field.dataType.simpleString()}")
        
        column_str = ",\n    ".join(columns)
        
        # Build CREATE TABLE statement
        create_statement = f"""CREATE TABLE {table_name} (
    {column_str}
)"""
        return create_statement
    except Exception as e:
        return str(e)

# Main application
def main():
    st.title("PySpark Database Manager")
    
    # Initialize Spark session
    spark = create_spark_session()
    
    # Get all databases
    databases = get_databases(spark)
    
    # Sidebar for navigation and database selection
    st.sidebar.title("Navigation")
    
    # Add database selector to sidebar
    selected_db = st.sidebar.selectbox("Select Database", databases)
    
    # Navigation menu
    page = st.sidebar.radio(
        "Select a page",
        ["Database Explorer", "Table Manager", "SQL Executor", "Data Import/Export"]
    )
    
    if page == "Database Explorer":
        st.header("Database Explorer")
        
        # Display database info
        st.write(f"**Database:** {selected_db}")
        
        # Get tables for selected database
        tables = get_tables(spark, selected_db)
        st.write(f"**Tables:** {len(tables)}")
        
        # Display tables
        st.subheader("Tables")
        if tables:
            for table in tables:
                with st.expander(table):
                    # Get table schema
                    schema = get_table_schema(spark, selected_db, table)
                    
                    # Display schema
                    if schema:
                        st.write("**Schema:**")
                        # Limit schema display to prevent memory issues with large tables
                        max_columns = 50
                        if len(schema.fields) > max_columns:
                            st.warning(f"Table has {len(schema.fields)} columns. Showing first {max_columns} to avoid memory issues.")
                            schema_fields = schema.fields[:max_columns]
                        else:
                            schema_fields = schema.fields
                            
                        schema_data = [[field.name, field.dataType.simpleString(), field.nullable] 
                                    for field in schema_fields]
                        schema_df = pd.DataFrame(schema_data, columns=["Column", "Type", "Nullable"])
                        st.dataframe(schema_df)
                    else:
                        st.warning(f"Could not retrieve schema for table '{table}'")
                    
                    # Display sample data
                    st.write("**Sample Data (Limited Preview):**")
                    sample = get_table_sample(spark, selected_db, table, 5)
                    if not sample.empty:
                        st.dataframe(sample)
                    else:
                        st.warning(f"No data available or could not query table '{table}'")
                    
                    # Display row count
                    try:
                        count = get_table_count(spark, selected_db, table)
                        st.write(f"**Row count:** {count:,}")
                    except Exception as e:
                        st.warning(f"Could not get row count: {str(e)}")
        else:
            st.info(f"No tables found in database '{selected_db}'")
    
    elif page == "Table Manager":
        st.header("Table Manager")
        
        # Get tables for selected database
        tables = get_tables(spark, selected_db)
        
        # Create new table section
        st.subheader("Create New Table")
        
        with st.form("create_table_form"):
            table_name = st.text_input("Table Name")
            
            col_count = st.number_input("Number of Columns", min_value=1, max_value=50, value=3)
            
            schema_cols = []
            for i in range(int(col_count)):
                col1, col2, col3 = st.columns([2, 2, 1])
                with col1:
                    col_name = st.text_input(f"Column {i+1} Name", key=f"col_name_{i}")
                with col2:
                    col_type = st.selectbox(
                        f"Column {i+1} Type", 
                        ["STRING", "INTEGER", "DOUBLE", "FLOAT", "BOOLEAN", "DATE", "TIMESTAMP"],
                        key=f"col_type_{i}"
                    )
                with col3:
                    nullable = st.checkbox("Nullable", value=True, key=f"nullable_{i}")
                    
                if col_name:
                    nullable_str = "" if nullable else "NOT NULL"
                    schema_cols.append(f"{col_name} {col_type} {nullable_str}")
            
            format_type = st.selectbox("Storage Format", ["PARQUET", "ORC", "CSV", "JSON"])
            
            submit_button = st.form_submit_button("Create Table")
            
            if submit_button:
                if table_name and schema_cols:
                    schema_str = ", ".join(schema_cols)
                    success, message = create_table(spark, selected_db, table_name, schema_str, format_type)
                    if success:
                        st.success(message)
                        # Refresh tables list
                        tables = get_tables(spark, selected_db)
                    else:
                        st.error(message)
                else:
                    st.warning("Please enter a table name and at least one column")
        
        # Manage existing tables
        st.subheader("Manage Existing Tables")
        
        if tables:
            selected_table = st.selectbox("Select Table", tables)
            
            # Get table schema
            schema = get_table_schema(spark, selected_db, selected_table)
            
            # Display schema
            st.write("**Schema:**")
            schema_data = [[field.name, field.dataType.simpleString(), field.nullable] 
                        for field in schema.fields]
            schema_df = pd.DataFrame(schema_data, columns=["Column", "Type", "Nullable"])
            st.dataframe(schema_df)
            
            # Table operations
            st.write("**Table Operations:**")
            op_col1, op_col2, op_col3 = st.columns(3)
            
            with op_col1:
                if st.button("Drop Table"):
                    confirm = st.checkbox("Confirm table deletion")
                    if confirm:
                        success, message = drop_table(spark, selected_db, selected_table)
                        if success:
                            st.success(message)
                            # Refresh tables list
                            tables = get_tables(spark, selected_db)
                            st.experimental_rerun()
                        else:
                            st.error(message)
            
            with op_col2:
                if st.button("Truncate Table"):
                    confirm = st.checkbox("Confirm truncate operation")
                    if confirm:
                        success, message = truncate_table(spark, selected_db, selected_table)
                        if success:
                            st.success(message)
                        else:
                            st.error(message)
            
            with op_col3:
                if st.button("Show CREATE TABLE"):
                    create_statement = generate_create_table_statement(spark, selected_db, selected_table)
                    st.code(create_statement, language="sql")
            
            # Column operations
            st.write("**Column Operations:**")
            col_op_tab1, col_op_tab2 = st.tabs(["Add Column", "Drop Column"])
            
            with col_op_tab1:
                add_col1, add_col2, add_col3 = st.columns([2, 2, 1])
                
                with add_col1:
                    new_col_name = st.text_input("New Column Name")
                
                with add_col2:
                    new_col_type = st.selectbox(
                        "New Column Type", 
                        ["STRING", "INTEGER", "DOUBLE", "FLOAT", "BOOLEAN", "DATE", "TIMESTAMP"]
                    )
                
                with add_col3:
                    if st.button("Add Column"):
                        if new_col_name:
                            success, message = add_column(spark, selected_db, selected_table, new_col_name, new_col_type)
                            if success:
                                st.success(message)
                                # Refresh to show updated schema
                                st.experimental_rerun()
                            else:
                                st.error(message)
                        else:
                            st.warning("Please enter a column name")
            
            with col_op_tab2:
                drop_col1, drop_col2 = st.columns([3, 1])
                
                with drop_col1:
                    column_options = [field.name for field in schema.fields]
                    col_to_drop = st.selectbox("Select Column to Drop", column_options)
                
                with drop_col2:
                    if st.button("Drop Column"):
                        confirm = st.checkbox("Confirm column deletion")
                        if confirm:
                            success, message = drop_column(spark, selected_db, selected_table, col_to_drop)
                            if success:
                                st.success(message)
                                # Refresh to show updated schema
                                st.experimental_rerun()
                            else:
                                st.error(message)
            
            # Sample data
            st.write("**Sample Data:**")
            row_limit = st.slider("Number of rows to show", min_value=5, max_value=100, value=10)
            sample = get_table_sample(spark, selected_db, selected_table, row_limit)
            st.dataframe(sample)
            
            # Display row count
            count = get_table_count(spark, selected_db, selected_table)
            try:
                st.write(f"**Row count:** {count:,}")
            except:
                st.write(f"**Row count:** {count}")
        else:
            st.info(f"No tables found in database '{selected_db}'")
    
    elif page == "SQL Executor":
        st.header("SQL Executor")
        
        # Use selected database
        spark.sql(f"USE {selected_db}")
        
        # SQL query input - Increased height
        st.subheader("Enter SQL Query")
        query = st.text_area("SQL Query", height=600)
        
        # SQL template options
        st.subheader("SQL Templates")
        template_type = st.selectbox(
            "Select a template",
            ["Custom Query", "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE TABLE", "ALTER TABLE", "DROP TABLE"]
        )
        
        # Get tables for selected database
        tables = get_tables(spark, selected_db)
        
        if template_type != "Custom Query":
            if tables:
                template_table = st.selectbox("Select a table", tables)
                
                if template_type == "SELECT":
                    template_query = f"SELECT * FROM {template_table} LIMIT 100"
                    st.code(template_query, language="sql")
                    if st.button("Use Template"):
                        query = template_query
                        st.session_state.query = query
                        st.experimental_rerun()
                
                elif template_type == "INSERT":
                    # Get table schema
                    schema = get_table_schema(spark, selected_db, template_table)
                    columns = [field.name for field in schema.fields]
                    column_str = ", ".join(columns)
                    value_placeholders = ", ".join(["?"] * len(columns))
                    
                    template_query = f"INSERT INTO {template_table} ({column_str}) VALUES ({value_placeholders})"
                    st.code(template_query, language="sql")
                    if st.button("Use Template"):
                        query = template_query
                        st.session_state.query = query
                        st.experimental_rerun()
                
                elif template_type == "UPDATE":
                    # Get table schema
                    schema = get_table_schema(spark, selected_db, template_table)
                    columns = [field.name for field in schema.fields]
                    
                    if columns:
                        template_query = f"UPDATE {template_table} SET {columns[0]} = 'new_value' WHERE {columns[0]} = 'old_value'"
                        st.code(template_query, language="sql")
                        if st.button("Use Template"):
                            query = template_query
                            st.session_state.query = query
                            st.experimental_rerun()
                
                elif template_type == "DELETE":
                    # Get table schema
                    schema = get_table_schema(spark, selected_db, template_table)
                    columns = [field.name for field in schema.fields]
                    
                    if columns:
                        template_query = f"DELETE FROM {template_table} WHERE {columns[0]} = 'value'"
                        st.code(template_query, language="sql")
                        if st.button("Use Template"):
                            query = template_query
                            st.session_state.query = query
                            st.experimental_rerun()
                
                elif template_type == "CREATE TABLE":
                    template_query = f"""CREATE TABLE new_table (
    column1 STRING,
    column2 INTEGER,
    column3 DOUBLE
) USING PARQUET"""
                    st.code(template_query, language="sql")
                    if st.button("Use Template"):
                        query = template_query
                        st.session_state.query = query
                        st.experimental_rerun()
                
                elif template_type == "ALTER TABLE":
                    template_query = f"ALTER TABLE {template_table} ADD COLUMNS (new_column STRING)"
                    st.code(template_query, language="sql")
                    if st.button("Use Template"):
                        query = template_query
                        st.session_state.query = query
                        st.experimental_rerun()
                
                elif template_type == "DROP TABLE":
                    template_query = f"DROP TABLE IF EXISTS {template_table}"
                    st.code(template_query, language="sql")
                    if st.button("Use Template"):
                        query = template_query
                        st.session_state.query = query
                        st.experimental_rerun()
            else:
                st.info(f"No tables found in database '{selected_db}'")
        
        # Execute button
        if st.button("Execute Query"):
            if query:
                with st.spinner("Executing query..."):
                    result = execute_query(spark, query)
                    
                    if isinstance(result, pd.DataFrame):
                        st.success("Query executed successfully")
                        st.subheader("Results")
                        st.dataframe(result)
                        
                        # Show number of rows
                        st.write(f"Returned {len(result):,} rows")
                        
                        # Download link
                        csv = result.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            "Download Results as CSV",
                            csv,
                            "query_results.csv",
                            "text/csv",
                            key='download-csv'
                        )
                    else:
                        st.error(f"Error executing query: {result}")
            else:
                st.warning("Please enter a query")
        
        # Show query history
        st.subheader("Query History")
        if "query_history" not in st.session_state:
            st.session_state.query_history = []
        
        if query and st.session_state.get("last_query") != query:
            st.session_state.query_history.append(query)
            st.session_state.last_query = query
        
        # Display query history
        if st.session_state.query_history:
            for i, hist_query in enumerate(reversed(st.session_state.query_history[-10:])):  # Show last 10 queries
                with st.expander(f"Query {len(st.session_state.query_history) - i}"):
                    st.code(hist_query, language="sql")
                    if st.button("Rerun", key=f"rerun_{i}"):
                        query = hist_query
                        st.session_state.query = query
                        st.experimental_rerun()
        else:
            st.info("No query history")
    
    elif page == "Data Import/Export":
        st.header("Data Import/Export")
        
        # Use selected database
        spark.sql(f"USE {selected_db}")
        
        # Get tables for selected database
        tables = get_tables(spark, selected_db)
        
        # Choose import or export
        import_export_tab = st.tabs(["Import Data", "Export Data"])
        
        with import_export_tab[0]:  # Import Data
            st.subheader("Import Data")
            
            # File upload
            uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])
            
            if uploaded_file is not None:
                # Read the CSV file
                df = pd.read_csv(uploaded_file)
                
                # Display the data
                st.write("Preview of uploaded data:")
                st.dataframe(df.head())
                
                # Table selection for import
                st.write("Import options:")
                
                # Create new table or use existing
                import_option = st.radio(
                    "Choose import method",
                    ["Create new table", "Insert into existing table"]
                )
                
                if import_option == "Create new table":
                    new_table_name = st.text_input("New Table Name")
                    
                    if st.button("Import Data"):
                        if new_table_name:
                            try:
                                # Convert pandas DataFrame to Spark DataFrame
                                spark_df = spark.createDataFrame(df)
                                
                                # Create table and insert data
                                spark_df.write.mode("overwrite").saveAsTable(f"{selected_db}.{new_table_name}")
                                
                                st.success(f"Data imported successfully into new table '{new_table_name}'")
                                
                                # Refresh tables list
                                tables = get_tables(spark, selected_db)
                            except Exception as e:
                                st.error(f"Error importing data: {str(e)}")
                        else:
                            st.warning("Please enter a table name")
                
                else:  # Insert into existing table
                    if tables:
                        target_table = st.selectbox("Select target table", tables)
                        
                        # Get table schema
                        schema = get_table_schema(spark, selected_db, target_table)
                        table_columns = [field.name for field in schema.fields]
                        
                        # Map CSV columns to table columns
                        st.write("Map CSV columns to table columns:")
                        
                        # Create mapping dictionary
                        mapping = {}
                        csv_columns = df.columns.tolist()
                        
                        for table_col in table_columns:
                            default_csv_col = table_col if table_col in csv_columns else csv_columns[0]
                            mapping[table_col] = st.selectbox(
                                f"Map '{table_col}' to", 
                                options=["-- Ignore --"] + csv_columns,
                                index=csv_columns.index(default_csv_col) + 1  # +1 because of the "-- Ignore --" option
                            )
                        
                        # Choose import mode
                        import_mode = st.radio(
                            "Import mode",
                            ["Append", "Overwrite"]
                        )
                        
                        if st.button("Import Data"):
                            try:
                                # Create a new DataFrame with the mapped columns
                                mapped_data = {}
                                for table_col, csv_col in mapping.items():
                                    if csv_col != "-- Ignore --":
                                        mapped_data[table_col] = df[csv_col].tolist()
                                
                                # Create a pandas DataFrame with the mapped data
                                mapped_df = pd.DataFrame(mapped_data)
                                
                                # Convert pandas DataFrame to Spark DataFrame
                                spark_df = spark.createDataFrame(mapped_df)
                                
                                # Write to table
                                mode = "append" if import_mode == "Append" else "overwrite"
                                spark_df.write.mode(mode).saveAsTable(f"{selected_db}.{target_table}")
                                
                                st.success(f"Data imported successfully into table '{target_table}' using {import_mode.lower()} mode")
                            except Exception as e:
                                st.error(f"Error importing data: {str(e)}")
                    else:
                        st.info(f"No tables found in database '{selected_db}'")
        
        with import_export_tab[1]:  # Export Data
            st.subheader("Export Data")
            
            if tables:
                # Table selection for export
                export_table = st.selectbox("Select table to export", tables, key="export_table_select")
                
                # Export options
                export_format = st.selectbox(
                    "Export format",
                    ["CSV", "JSON", "Parquet", "ORC"]
                )
                
                # Limit rows or export all
                export_option = st.radio(
                    "Export option",
                    ["Export all rows", "Limit rows"]
                )
                
                limit = None
                if export_option == "Limit rows":
                    limit = st.number_input("Number of rows to export", min_value=1, value=1000)
                
                # Add filtering options
                add_filter = st.checkbox("Add filters")
                filter_conditions = []
                
                if add_filter:
                    # Get table schema
                    schema = get_table_schema(spark, selected_db, export_table)
                    columns = [field.name for field in schema.fields]
                    
                    # Let user add multiple filter conditions
                    filter_count = st.number_input("Number of filters", min_value=1, max_value=5, value=1)
                    
                    for i in range(filter_count):
                        filter_col1, filter_col2, filter_col3 = st.columns(3)
                        
                        with filter_col1:
                            filter_column = st.selectbox(f"Filter column {i+1}", columns, key=f"filter_column_{i}")
                        
                        with filter_col2:
                            filter_operator = st.selectbox(
                                f"Operator {i+1}",
                                ["=", ">", "<", ">=", "<=", "!=", "LIKE", "IN", "IS NULL", "IS NOT NULL"],
                                key=f"filter_operator_{i}"
                            )
                        
                        with filter_col3:
                            filter_value = ""
                            if filter_operator not in ["IS NULL", "IS NOT NULL"]:
                                filter_value = st.text_input(f"Value {i+1}", key=f"filter_value_{i}")
                        
                        # Build filter condition
                        if filter_operator in ["IS NULL", "IS NOT NULL"]:
                            filter_conditions.append(f"{filter_column} {filter_operator}")
                        elif filter_operator == "IN":
                            # Parse comma-separated values for IN operator
                            values = [v.strip() for v in filter_value.split(",")]
                            value_str = ", ".join([f"'{v}'" for v in values])
                            filter_conditions.append(f"{filter_column} IN ({value_str})")
                        elif filter_operator == "LIKE":
                            filter_conditions.append(f"{filter_column} LIKE '{filter_value}'")
                        else:
                            filter_conditions.append(f"{filter_column} {filter_operator} '{filter_value}'")
                
                # Add sorting options
                add_sort = st.checkbox("Add sorting")
                sort_conditions = []
                
                if add_sort:
                    # Get table schema
                    schema = get_table_schema(spark, selected_db, export_table)
                    columns = [field.name for field in schema.fields]
                    
                    sort_col1, sort_col2 = st.columns(2)
                    
                    with sort_col1:
                        sort_column = st.selectbox("Sort by", columns)
                    
                    with sort_col2:
                        sort_order = st.selectbox("Order", ["ASC", "DESC"])
                    
                    sort_conditions.append(f"{sort_column} {sort_order}")
                
                # Build the query
                query = f"SELECT * FROM {export_table}"
                
                if filter_conditions:
                    query += " WHERE " + " AND ".join(filter_conditions)
                
                if sort_conditions:
                    query += " ORDER BY " + ", ".join(sort_conditions)
                
                if limit:
                    query += f" LIMIT {limit}"
                
                # Show the query
                st.code(query, language="sql")
                
                # Export button
                if st.button("Export Data"):
                    try:
                        # Execute the query
                        result = spark.sql(query)
                        
                        # Convert to pandas for export
                        pdf = result.toPandas()
                        
                        if export_format == "CSV":
                            csv = pdf.to_csv(index=False).encode('utf-8')
                            st.download_button(
                                "Download CSV",
                                csv,
                                f"{export_table}_export.csv",
                                "text/csv",
                                key='export-csv'
                            )
                            st.success(f"Exported {len(pdf):,} rows to CSV")
                        
                        elif export_format == "JSON":
                            json = pdf.to_json(orient="records").encode('utf-8')
                            st.download_button(
                                "Download JSON",
                                json,
                                f"{export_table}_export.json",
                                "application/json",
                                key='export-json'
                            )
                            st.success(f"Exported {len(pdf):,} rows to JSON")
                        
                        elif export_format == "Parquet":
                            # For Parquet and ORC, we need to save to a temp file and then read it back
                            temp_file = f"/tmp/{export_table}_export.parquet"
                            pdf.to_parquet(temp_file, index=False)
                            
                            with open(temp_file, "rb") as f:
                                st.download_button(
                                    "Download Parquet",
                                    f,
                                    f"{export_table}_export.parquet",
                                    "application/octet-stream",
                                    key='export-parquet'
                                )
                            st.success(f"Exported {len(pdf):,} rows to Parquet")
                        
                        elif export_format == "ORC":
                            st.error("ORC export not implemented in this version")
                            
                    except Exception as e:
                        st.error(f"Error exporting data: {str(e)}")
            else:
                st.info(f"No tables found in database '{selected_db}'")

    # Add footer
    st.markdown("---")
    st.caption("PySpark Database Manager • Developed by ripersonnalite@hotmail.com")

if __name__ == "__main__":
    main()