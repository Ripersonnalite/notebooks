import yfinance as yf
import pandas as pd
import time
from datetime import datetime
import os
import socket
import gc
import glob
from IPython.display import display

# Global Constants
TICKERS_CSV_PATH = "total_tickers.csv"
HOSTNAME = socket.gethostname()
SHARED_DIR = "/mnt/shared"

# Set this flag to True if you only want to print the divided tickers for testing.
ONLY_PRINT_DIVIDED_TICKERS = False
RUN_PARQUET_CONVERSION = False

def get_current_ip():
    """
    Try to get the machine's non-loopback IP address.
    First, explicitly check for the 'eth1' interface.
    If not found, scan the available network interfaces.
    """
    ip_address = None

    # Check for eth1 explicitly
    if os.path.exists("/sys/class/net/eth1"):
        try:
            cmd = "ip addr show eth1 | grep 'inet ' | awk '{print $2}' | cut -d/ -f1"
            ip = os.popen(cmd).read().strip()
            if ip and not ip.startswith("127."):
                ip_address = ip
                print(f"Found IP on eth1: {ip_address}")
        except Exception as e:
            print(f"Error detecting IP on eth1: {e}")

    # If eth1 is not available or doesn't yield an IP, scan other interfaces.
    if not ip_address:
        try:
            interfaces = os.listdir('/sys/class/net/')
            for iface in interfaces:
                if iface == 'lo' or iface.startswith('vir'):
                    continue
                cmd = f"ip addr show {iface} | grep 'inet ' | awk '{{print $2}}' | cut -d/ -f1"
                ip = os.popen(cmd).read().strip()
                if ip and not ip.startswith("127."):
                    ip_address = ip
                    print(f"Found IP on {iface}: {ip_address}")
                    break
        except Exception as e:
            print(f"Error detecting IP from interfaces: {e}")
    
    # Fallback method
    if not ip_address:
        ip_address = socket.gethostbyname(socket.gethostname())
        print(f"Falling back to socket.gethostbyname: {ip_address}")
    
    return ip_address

def load_csv():
    """Load the CSV file with tickers."""
    if not os.path.exists(TICKERS_CSV_PATH):
        print("Ticker CSV file not found!")
        return pd.DataFrame()
    else:
        print(f"Loading CSV file from {TICKERS_CSV_PATH}...")
        return pd.read_csv(TICKERS_CSV_PATH)

def clear_existing_parquet_files():
    """Clear any existing Parquet files before starting a new run."""
    files_to_delete = glob.glob(f"stocks_data_{HOSTNAME}_*.parquet")
    for file in files_to_delete:
        print(f"Clearing existing Parquet file: {file}")
        os.remove(file)

def clear_existing_csv_files():
    """Clear any existing CSV files matching the naming pattern before starting a new run."""
    csv_files = glob.glob(f"stocks_data_{HOSTNAME}_*.csv")
    for file in csv_files:
        print(f"Clearing existing CSV file: {file}")
        os.remove(file)

def append_to_parquet(df, parquet_file_path):
    """Append data to the Parquet file using concatenation."""
    try:
        print("\nAppending data to Parquet file...")
        if not os.path.exists(parquet_file_path):
            print("Creating new Parquet file...")
            df.to_parquet(parquet_file_path)
        else:
            existing_df = pd.read_parquet(parquet_file_path)
            combined_df = pd.concat([existing_df, df])
            combined_df.to_parquet(parquet_file_path)
            del existing_df, combined_df

        parquet_df = pd.read_parquet(parquet_file_path)
        total_rows = len(parquet_df)
        print(f"Updated total rows in Parquet file: {total_rows}\n")
        del parquet_df
        gc.collect()

    except Exception as e:
        print(f"\nError in append_to_parquet: {e}\n")
        raise

def fetch_yfinance_data(tickers, start_date="1930-01-01", end_date=datetime.now().strftime('%Y-%m-%d')):
    """
    Fetch stock data and dump each ticker's data immediately to CSV files,
    creating a new CSV file every `batch_size` tickers.
    """
    total_tickers = len(tickers)
    total_rows_processed = 0
    batch_size = 1500  # Adjust the batch size if needed.
    file_counter = 1   # Used to differentiate files for each batch

    for idx, ticker in enumerate(tickers):
        if idx % batch_size == 0:
            csv_file_path = f"stocks_data_{HOSTNAME}_{file_counter}.csv"
            file_counter += 1
            if os.path.exists(csv_file_path):
                os.remove(csv_file_path)
        
        print(f"Processing {ticker} ({idx+1}/{total_tickers})")
        try:
            print(f"Downloading data for {ticker}...")
            ticker_data = yf.download(ticker, start=start_date, end=end_date, interval="1d")
            print(f"Data for {ticker} downloaded.")

            if ticker_data.empty:
                print(f"No data downloaded for {ticker}. Skipping.")
                continue

            ticker_data['Ticker'] = ticker
            ticker_data['Daily_Return'] = (ticker_data['Close'] / ticker_data['Close'].shift(1) - 1) * 100
            ticker_data['Cumulative_Return'] = (ticker_data['Close'] / ticker_data['Close'].iloc[0] - 1) * 100
            ticker_data['SMA_20'] = ticker_data['Close'].rolling(window=20, min_periods=1).mean()
            ticker_data['EMA_20'] = ticker_data['Close'].ewm(span=20, min_periods=1, adjust=False).mean()

            write_header = not os.path.exists(csv_file_path)
            ticker_data.to_csv(csv_file_path, mode='a', header=write_header, index=False)
            
            total_rows_processed += len(ticker_data)
            print(f"{ticker} processed successfully. Rows processed: {len(ticker_data)}")

        except Exception as e:
            print(f"Error processing ticker {ticker}: {e}")
            continue

    print(f"\nTotal rows processed: {total_rows_processed}")

def main_to_parquet():
    def csv_to_parquet(csv_file_path, parquet_file_path=None):
        """
        Convert a CSV file to a Parquet file.
        """
        if not os.path.exists(csv_file_path):
            print(f"CSV file '{csv_file_path}' not found!")
            return None

        df = pd.read_csv(csv_file_path, low_memory=False)
        
        numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        if parquet_file_path is None:
            parquet_file_path = os.path.splitext(csv_file_path)[0] + '.parquet'
        
        df.to_parquet(parquet_file_path, index=False)
        print(f"Converted '{csv_file_path}' to '{parquet_file_path}'")
        return parquet_file_path

    csv_files = glob.glob(f"stocks_data_{HOSTNAME}_*.csv")
    if not csv_files:
        print("No CSV files found!")
    else:
        for csv_file in csv_files:
            csv_to_parquet(csv_file)
        
        parquet_files = glob.glob(f"stocks_data_{HOSTNAME}_*.parquet")
        if not parquet_files:
            print("No Parquet files found!")
        else:
            df = pd.concat([pd.read_parquet(file) for file in parquet_files], ignore_index=True)
            ticker_counts = df.groupby('Ticker').size().reset_index(name='row_count')
            display(ticker_counts)
def rerun_parquet_conversion():
    """Function to manually rerun the CSV to Parquet conversion."""
    try:
        print("\nRerunning CSV to Parquet conversion...")
        csv_files = glob.glob(f"stocks_data_{HOSTNAME}_*.csv")
        if not csv_files:
            print("No CSV files found for conversion!")
            return

        for csv_file in csv_files:
            print(f"Converting file: {csv_file}")
            # Re-run the conversion to Parquet
            csv_to_parquet(csv_file)

        parquet_files = glob.glob(f"stocks_data_{HOSTNAME}_*.parquet")
        if not parquet_files:
            print("No Parquet files found after conversion!")
        else:
            df = pd.concat([pd.read_parquet(file) for file in parquet_files], ignore_index=True)
            ticker_counts = df.groupby('Ticker').size().reset_index(name='row_count')
            display(ticker_counts)

    except Exception as e:
        print(f"Error during Parquet conversion: {e}")


def run_main_code():
    """Main execution function."""
    start_time = time.time()

    try:
        # Clear existing files before starting a new run
        clear_existing_parquet_files()
        clear_existing_csv_files()

        # Load tickers from CSV
        df_pandas = load_csv()
        if df_pandas.empty:
            print("No data to process.")
            return

        # Get the full list of tickers
        full_ticker_list = df_pandas['Ticker'].unique().tolist()
        print(f"Total tickers in CSV: {len(full_ticker_list)}")

        # ----------------------
        # PARTITION TICKERS BASED ON MACHINE IP
        # ----------------------
        current_ip = get_current_ip()
        print(f"Current machine IP: {current_ip}")

        # Mapping of machine IPs to a rank (1-indexed)
        MACHINE_IPS = {
            "172.22.65.10": 1,
            "172.22.65.20": 2,
            "172.22.65.30": 3,
            "172.22.65.40": 4,
            "172.22.65.50": 5,
            "172.22.65.60": 6
        }

        machine_rank = MACHINE_IPS.get(current_ip)
        if machine_rank is None:
            print(f"Machine with IP {current_ip} is not registered.")
            return

        total_machines = len(MACHINE_IPS)
        # Sort tickers to ensure deterministic ordering
        tickers_sorted = sorted(full_ticker_list)
        # Partition tickers using modulo arithmetic
        tickers = [
            ticker for idx, ticker in enumerate(tickers_sorted)
            if idx % total_machines == machine_rank - 1
        ]
        print(f"Machine {current_ip} (rank {machine_rank}) is assigned {len(tickers)} tickers: ")
        print(tickers)

        # If the flag is set, only print the divided tickers and exit.
        if ONLY_PRINT_DIVIDED_TICKERS:
            print("ONLY_PRINT_DIVIDED_TICKERS flag is set. Exiting after printing assigned tickers.")
            return

        # ----------------------
        # FETCH DATA AND CONVERT TO PARQUET
        # ----------------------
        fetch_yfinance_data(tickers)
        main_to_parquet()

    except Exception as e:
        print(f"Error in main execution: {e}")
    finally:
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"\nTotal execution time: {elapsed_time:.2f} seconds")

def main():
    run_main_code()
    if RUN_PARQUET_CONVERSION:
        rerun_parquet_conversion()


if __name__ == "__main__":
    main()
