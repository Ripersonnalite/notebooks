import os
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd
from datetime import datetime

# Ensure environment variable is set correctly
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."


def get_current_user():
    """Get current user from Databricks context"""
    try:
        query = """
        SELECT CONCAT_WS(' - ',
            current_user(),
            session_user(),
            user
        ) as user_info
        """
        result = sqlQuery(query)
        if not result.empty:
            # This will show us all available user information
            st.write("Debug - Available user info:", result.iloc[0]['user_info'])
            
            # Try to get the email from session_user
            query_session = "SELECT session_user() as user"
            result_session = sqlQuery(query_session)
            if not result_session.empty:
                return result_session.iloc[0]['user']
        return None
    except Exception as e:
        st.error(f"Error getting current user: {str(e)}")
        return None


def get_current_user_old():
    """Get current user from Databricks context"""
    try:
        query = """
        SELECT 
            current_user() as service_account,
            regexp_extract(current_user(), '^([^@]+@[^@]+).*$', 1) as email
        """
        result = sqlQuery(query)
        if not result.empty:
            email = result.iloc[0]['email']
            if email:
                return email
            # Fallback to service account if email extraction fails
            return result.iloc[0]['service_account']
        return None
    except Exception as e:
        st.error(f"Error getting current user: {str(e)}")
        return None

def sqlQuery(query: str) -> pd.DataFrame:
    cfg = Config()  # Pull environment variables for auth
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

def check_existing_query():
    """Check if there's an existing query"""
    try:
        query = "SELECT * FROM text.`/Volumes/workspace/default/data1/latest_query`"
        result = sqlQuery(query)
        return result if not result.empty else None
    except Exception as e:
        st.error("Error checking existing query: " + str(e))
        return None

def save_query_to_volume(query: str):
    """Save the query to a text file in Databricks volume"""
    try:
        # Debug information
        st.write("Query to be saved:")
        st.code(query, language="sql")
        
        # Simpler direct write approach
        write_query = (
            "INSERT OVERWRITE DIRECTORY '/Volumes/workspace/default/data1/latest_query' "
            "USING TEXT "
            "SELECT q FROM (SELECT '" + query.replace("'", "''") + "' AS q)"
        )
        
        # Debug information
        st.write("Write Query to be executed:")
        st.code(write_query, language="sql")
        
        st.write("Executing query...")
        sqlQuery(write_query)
        
        st.write("Verifying saved data...")
        verify_query = "SELECT * FROM text.`/Volumes/workspace/default/data1/latest_query`"
        verification_result = sqlQuery(verify_query)
        
        if verification_result is not None and not verification_result.empty:
            st.dataframe(verification_result)
            st.write("Contents of saved query:")
            st.code(verification_result.iloc[0, 0], language="sql")
        
        return True
    except Exception as e:
        st.error("Detailed error saving query: " + str(e))
        st.code("Query that failed to save: " + query, language="sql")
        return False

# Set page config
st.set_page_config(layout="wide")

# Get and display current user
current_user = get_current_user()
if current_user:
    st.sidebar.info(f"Logged in as: {current_user}")
else:
    st.error("Could not determine current user")
    st.stop()  # Stop the app if we can't identify the user

def get_table_columns():
    """Get all columns from the table"""
    query = """
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_schema = 'default' 
    AND table_name = 'combined_financial_stocks'
    """
    return sqlQuery(query)

def get_tickers():
    """Get unique tickers from the table"""
    query = """
    SELECT DISTINCT Ticker 
    FROM workspace.default.combined_financial_stocks 
    ORDER BY Ticker
    """
    result = sqlQuery(query)
    return result['Ticker'].tolist()

def get_date_range():
    """Get the minimum and maximum dates from the table"""
    query = """
    SELECT MIN(Date) as min_date, MAX(Date) as max_date 
    FROM workspace.default.combined_financial_stocks
    """
    result = sqlQuery(query)
    return pd.to_datetime(result['min_date'].iloc[0]), pd.to_datetime(result['max_date'].iloc[0])

@st.cache_data(ttl=30)  # only re-query if it's been 30 seconds
def getData(selected_columns, limit, start_date, end_date, selected_tickers):
    columns_str = ", ".join(selected_columns) if selected_columns else "*"
    
    if selected_tickers:
        ticker_list = ", ".join("'" + ticker + "'" for ticker in selected_tickers)
        ticker_filter = "AND Ticker IN (" + ticker_list + ")"
    else:
        ticker_filter = ""
    
    query = (
        "SELECT " + columns_str + " "
        "FROM workspace.default.combined_financial_stocks "
        "WHERE Date BETWEEN '" + start_date.strftime('%Y-%m-%d') + "' AND '" + 
        end_date.strftime('%Y-%m-%d') + "' " +
        ticker_filter + " "
        "ORDER BY Date DESC "
        "LIMIT " + str(limit)
    )
    
    df = sqlQuery(query)
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'])
    return df, query

# Initialize session states
if 'columns_info' not in st.session_state:
    st.session_state.columns_info = get_table_columns()

if 'selected_columns' not in st.session_state:
    st.session_state.selected_columns = []

if 'date_range' not in st.session_state:
    st.session_state.date_range = get_date_range()

if 'tickers' not in st.session_state:
    st.session_state.tickers = get_tickers()

if 'column_order' not in st.session_state:
    st.session_state.column_order = []

# Sidebar for query design
st.sidebar.header("Query Designer")

# Convert column names to list
available_columns = st.session_state.columns_info['column_name'].tolist()

# Set default columns in desired order
default_columns = ['Ticker', 'Date', 'Open', 'Low', 'High', 'Close']

# Multi-select for columns
selected_columns = st.sidebar.multiselect(
    "Select columns to display",
    options=available_columns,
    default=default_columns
)

# Initialize column order with default columns if not set
if not st.session_state.column_order or set(st.session_state.column_order) != set(selected_columns):
    # Keep the order of existing columns and append new ones
    new_order = [col for col in st.session_state.column_order if col in selected_columns]
    new_columns = [col for col in selected_columns if col not in new_order]
    st.session_state.column_order = new_order + new_columns

# Ticker selector
st.sidebar.subheader("Ticker Filter")

# Default tickers
default_tickers = ['AAPL', 'MSFT', 'NVDA', 'TSLA']

selected_tickers = st.sidebar.multiselect(
    "Select tickers",
    options=st.session_state.tickers,
    default=default_tickers if all(ticker in st.session_state.tickers for ticker in default_tickers) else None
)

# Date range selector
st.sidebar.subheader("Date Range Filter")
# Set default start date to 2000-01-01
default_start_date = pd.to_datetime('2000-01-01')
min_date, max_date = st.session_state.date_range
start_date = st.sidebar.date_input(
    "Start Date",
    value=default_start_date,
    min_value=min_date,
    max_value=max_date
)
end_date = st.sidebar.date_input(
    "End Date",
    value=max_date,
    min_value=min_date,
    max_value=max_date
)

# Limit selector
limit = st.sidebar.number_input(
    "Number of rows to display",
    min_value=1,
    max_value=100000,
    value=50000,
    step=1000
)

# Column reordering section using a more intuitive interface
st.sidebar.subheader("Column Order")
if selected_columns:
    st.sidebar.write("Drag columns to reorder:")
    
    # Initialize column order if needed
    if not st.session_state.column_order or set(st.session_state.column_order) != set(selected_columns):
        st.session_state.column_order = selected_columns.copy()
    
    # Create a container for the reorderable columns
    for i, col in enumerate(st.session_state.column_order):
        col_container = st.sidebar.container()
        col_container.markdown(f"### {i+1}. {col}")
        
        # Up/Down buttons
        col1, col2 = col_container.columns(2)
        if i > 0:
            if col1.button("↑", key=f"up_{col}"):
                st.session_state.column_order[i], st.session_state.column_order[i-1] = \
                st.session_state.column_order[i-1], st.session_state.column_order[i]
                st.rerun()
        
        if i < len(st.session_state.column_order) - 1:
            if col2.button("↓", key=f"down_{col}"):
                st.session_state.column_order[i], st.session_state.column_order[i+1] = \
                st.session_state.column_order[i+1], st.session_state.column_order[i]
                st.rerun()

    final_columns = st.session_state.column_order
else:
    final_columns = []

# Main content
st.header("Data Preview")

if final_columns:
    data, current_query = getData(final_columns, limit, start_date, end_date, selected_tickers)
    
    # Display the dataframe
    st.dataframe(data=data, height=600, use_container_width=True)
    
    # Create two columns for metrics
    col1, col2, col3 = st.columns(3)
    
    # Display row count in first column
    col1.metric("Number of rows", len(data))
    
    # Save Query button in second column with debug information
    if col2.button("Save Query"):
        st.info("Attempting to save query...")
        if save_query_to_volume(current_query):
            st.success("Query saved successfully!")
        else:
            st.error("Failed to save query")
    
    # Download CSV button in third column
    csv = data.to_csv(index=False).encode('utf-8')
    col3.download_button(
        "Download CSV",
        csv,
        "query_results.csv",
        "text/csv",
        key='download-csv'
    )
    
    # Display current query
    st.code(current_query, language="sql")
    
    # Show date range and ticker information
    info_text = "Showing data from " + str(start_date) + " to " + str(end_date)
    if selected_tickers:
        info_text += " for tickers: " + ", ".join(selected_tickers)
    st.info(info_text)
    
else:
    st.warning("Please select at least one column to display data.")
