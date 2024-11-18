import streamlit as st
import duckdb
import pandas as pd
from datetime import datetime
import io
import time

# Set page config with minimal interface
st.set_page_config(
    page_title="Stock Data Download",
    page_icon="📊",
    layout="wide"
)

# Initialize connection to DuckDB
@st.cache_resource
def init_connection():
    return duckdb.connect('financials_data.db', read_only=True)

# Get unique sectors with counts
@st.cache_data
def get_sectors_with_counts():
    query = """
    SELECT 
        si.sector,
        COUNT(DISTINCT si.symbol) as unique_tickers
    FROM Stocks_Info si
    WHERE si.sector IS NOT NULL
    GROUP BY si.sector
    ORDER BY si.sector
    """
    return init_connection().execute(query).fetchall()

# Get detailed sector summary
@st.cache_data
def get_sector_summary():
    query = """
    SELECT 
        si.sector,
        COUNT(DISTINCT si.symbol) as unique_tickers,
        COUNT(*) as total_rows,
        MIN(sf.Date) as earliest_date,
        MAX(sf.Date) as latest_date
    FROM Stocks_Info si
    LEFT JOIN Stocks_Float sf ON si.symbol = sf.Ticker
    WHERE si.sector IS NOT NULL
    GROUP BY si.sector
    ORDER BY unique_tickers DESC
    """
    return pd.DataFrame(
        init_connection().execute(query).fetchall(),
        columns=['Sector', 'Unique Tickers', 'Total Records', 'First Date', 'Last Date']
    )

# Get tickers for a specific sector
@st.cache_data
def get_tickers_by_sector(sector):
    query = f"""
    SELECT DISTINCT symbol 
    FROM Stocks_Info 
    WHERE sector = '{sector}'
    ORDER BY symbol
    """
    return init_connection().execute(query).fetchall()

def get_total_rows(query):
    """Get total number of rows for progress calculation"""
    count_query = f"SELECT COUNT(*) as count FROM ({query}) sq"
    return init_connection().execute(count_query).fetchone()[0]

def format_number(num):
    """Format large numbers with K, M suffix"""
    if num >= 1_000_000:
        return f"{num/1_000_000:.1f}M"
    elif num >= 1_000:
        return f"{num/1_000:.1f}K"
    return str(num)

def get_download_data(query):
    buffer = io.StringIO()
    conn = init_connection()
    
    # Get total rows for progress bar
    total_rows = get_total_rows(query)
    
    # Create a progress bar
    progress_bar = st.progress(0)
    status_text = st.empty()
    rows_processed = 0
    
    # Execute query and write directly to buffer
    chunk_size = 10000
    result = conn.execute(query)
    
    # Write header
    first_chunk = True
    start_time = time.time()
    
    try:
        while True:
            chunk = result.fetch_df_chunk(chunk_size)
            if chunk is None or len(chunk) == 0:
                break
                
            if first_chunk:
                chunk.to_csv(buffer, index=False)
                first_chunk = False
            else:
                chunk.to_csv(buffer, index=False, header=False)
            
            # Update progress
            rows_processed += len(chunk)
            progress = min(rows_processed / total_rows, 1.0)
            progress_bar.progress(progress)
            
            # Calculate and display estimated time remaining
            elapsed_time = time.time() - start_time
            if progress > 0:
                estimated_total_time = elapsed_time / progress
                remaining_time = estimated_total_time - elapsed_time
                status_text.text(f"Processing {rows_processed:,} of {total_rows:,} rows... " +
                               f"(Estimated time remaining: {remaining_time:.1f}s)")

    finally:
        # Clean up
        progress_bar.empty()
        status_text.empty()
    
    return buffer.getvalue()

def main():
    st.title("📊 Stock Data Download")
    
    # Create a form for all filters
    with st.form("download_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            start_date = st.date_input(
                "Start Date",
                pd.Timestamp('2000-01-01'),
                key="start_date"
            )
        
        with col2:
            end_date = st.date_input(
                "End Date",
                pd.Timestamp.now(),
                key="end_date"
            )

        # Simplified sector selector
        sectors_with_counts = get_sectors_with_counts()
        sector_options = [f"{sector[0]} ({sector[1]} stocks)" for sector in sectors_with_counts]
        sectors_dict = {f"{sector[0]} ({sector[1]} stocks)": sector[0] for sector in sectors_with_counts}
        
        selected_sector_display = st.selectbox(
            "Select Sector",
            options=sector_options,
            index=sector_options.index(next(s for s in sector_options if "Information Technology" in s)) 
                  if any("Information Technology" in s for s in sector_options) else 0,
            key="sector"
        )
        
        selected_sector = sectors_dict[selected_sector_display]

        # Ticker selector
        sector_tickers = [ticker[0] for ticker in get_tickers_by_sector(selected_sector)]
        select_all = st.checkbox("Select All Tickers", key="select_all")
        
        if select_all:
            selected_tickers = sector_tickers
            st.info(f"Selected all {len(sector_tickers)} tickers")
        else:
            selected_tickers = st.multiselect(
                "Select Tickers",
                options=sector_tickers,
                default=[sector_tickers[0]] if sector_tickers else [],
                key="tickers"
            )

        # Metrics selector
        metrics = st.multiselect(
            "Select Metrics",
            ["Open", "Low", "High", "Close", "Volume", "SMA_20", "EMA_20"],
            default=["Open", "Low", "High", "Close", "Volume"],
            key="metrics"
        )

        # Submit button
        submitted = st.form_submit_button("Prepare Download", use_container_width=True)

    # Display sector summary table
    st.subheader("Sector Summary")
    summary_df = get_sector_summary()
    
    # Format the numbers in the summary
    summary_df['Total Records'] = summary_df['Total Records'].apply(format_number)
    summary_df['First Date'] = pd.to_datetime(summary_df['First Date']).dt.strftime('%Y-%m-%d')
    summary_df['Last Date'] = pd.to_datetime(summary_df['Last Date']).dt.strftime('%Y-%m-%d')
    
    # Display the formatted table
    st.dataframe(
        summary_df,
        column_config={
            "Sector": st.column_config.TextColumn("Sector", width="medium"),
            "Unique Tickers": st.column_config.NumberColumn("Unique Tickers", format="%d"),
            "Total Records": st.column_config.TextColumn("Total Records", width="small"),
            "First Date": st.column_config.TextColumn("First Date", width="small"),
            "Last Date": st.column_config.TextColumn("Last Date", width="small")
        },
        use_container_width=True,
        hide_index=True
    )

    # Process form submission
    if submitted:
        if not selected_tickers:
            st.warning("Please select at least one ticker.")
            return

        if not metrics:
            st.warning("Please select at least one metric.")
            return

        # Show row count for selected tickers
        count_query = f"""
        SELECT COUNT(*) as count
        FROM Stocks_Float sf
        WHERE sf.Ticker IN ('{"', '".join(selected_tickers)}')
        AND sf.Date BETWEEN '{start_date}' AND '{end_date}'
        """
        selected_count = init_connection().execute(count_query).fetchone()[0]
        st.info(f"Selected data will contain {format_number(selected_count)} records")

        # Prepare query
        tickers_str = "', '".join(selected_tickers)
        query = f"""
        SELECT sf.Date, sf.Ticker, {', '.join(f'sf.{metric}' for metric in metrics)}
        FROM Stocks_Float sf
        WHERE sf.Ticker IN ('{tickers_str}')
        AND sf.Date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY sf.Date DESC, sf.Ticker
        """

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{selected_sector}_stock_data_{timestamp}.csv"

        try:
            with st.spinner('Preparing your data...'):
                data = get_download_data(query)
                st.success("✅ Data is ready!")
                st.download_button(
                    "📥 Download Data",
                    data=data,
                    file_name=filename,
                    mime="text/csv",
                    use_container_width=True
                )
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()