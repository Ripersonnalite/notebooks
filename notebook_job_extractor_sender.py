import smtplib, time, datetime, inspect, os, re
from time import sleep
from email.mime.text import MIMEText

def get_workspace_url():
    """Get the current Databricks workspace URL"""
    try:
        # Get the workspace URL from spark configuration
        workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
        return f"https://{workspace_url}"
    except Exception as e:
        print(f"Error getting workspace URL: {str(e)}")
        return None

def fix_query(query):
    """Fix date and ticker formatting in the query"""
    # First fix the dates: look for BETWEEN date AND date
    date_pattern = r'BETWEEN (\d{4}-\d{2}-\d{2}) AND (\d{4}-\d{2}-\d{2})'
    query = re.sub(date_pattern, r"BETWEEN '\1' AND '\2'", query)
    
    # Then fix the ticker list: look for IN (AAPL, MSFT, etc)
    ticker_pattern = r'IN \((.*?)\)'
    def quote_tickers(match):
        tickers = match.group(1).split(', ')
        quoted_tickers = ["'" + ticker + "'" for ticker in tickers]
        return "IN (" + ", ".join(quoted_tickers) + ")"
    
    query = re.sub(ticker_pattern, quote_tickers, query)
    return query

def send_message(subject="Email by Python", body="Default by Python"):
    """By Ricardo Kazuo"""
    # Read credentials and addresses from files
    with open('/Volumes/workspace/default/data1/gmail.txt', 'r') as file:
        password = file.read().strip()
    
    with open('/Volumes/workspace/default/data1/sender.txt', 'r') as file:
        sender_email = file.read().strip()
        
    with open('/Volumes/workspace/default/data1/receiver.txt', 'r') as file:
        receiver_email = file.read().strip()
        
    message = MIMEText(body)
    message['to'] = receiver_email
    message['from'] = f"Databricks - Extractor<{sender_email}>"
    message['subject'] = subject
    server = smtplib.SMTP('smtp.gmail.com:587')
    server.ehlo_or_helo_if_needed()
    server.starttls()
    server.ehlo_or_helo_if_needed()
    server.login(sender_email, password)
    server.sendmail(f"Databricks - Extractor<{sender_email}>", receiver_email, message.as_string())
    server.quit()

# Start timing
start_time = time.time()

# Read and fix the saved query
saved_query = spark.sql("SELECT * FROM text.`/Volumes/workspace/default/data1/latest_query`").collect()[0][0]
print("Original saved query:")
print(saved_query)

fixed_query = fix_query(saved_query)
print("\nFixed query:")
print(fixed_query)

# Execute the fixed query
print("\nExecuting query...")
df = spark.sql(fixed_query)

# Get data profile
num_rows = df.count()
num_columns = len(df.columns)
estimated_size_bytes = num_rows * num_columns * 10
estimated_size_mb = round(estimated_size_bytes / (1024 * 1024), 2)
estimated_compressed_size_mb = round(estimated_size_mb * 0.15, 2)

# Generate timestamp for the file name
current_date = datetime.datetime.now().strftime('%Y%m%d')
base_path = f"/Volumes/workspace/default/data1/{current_date}.csv.gz"

# Write with compression
df.coalesce(1).write.format("csv") \
    .option("header", "true") \
    .option("compression", "gzip") \
    .mode("overwrite") \
    .save(base_path)

# Get the actual file path including the part file name
output_files = dbutils.fs.ls(base_path)
part_file_path = [f for f in output_files if f.name.startswith('part-')][0].path

# Clean up the path
if part_file_path.startswith('dbfs:'):
    part_file_path = part_file_path[5:]

# End timing and prepare completion notification
end_time = time.time()
formatted_end_time = datetime.datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')
execution_time = round(end_time - start_time, 2)

# Get the workspace URL and construct the download link
workspace_url = get_workspace_url()
if workspace_url:
    download_url = f"{workspace_url}/ajax-api/2.0/fs/files{part_file_path}"
    print(f"\nDownload URL: {download_url}")
else:
    download_url = "Please copy the download URL from the Databricks UI"
    print("\nCouldn't generate download URL automatically. Please copy from UI.")

report = f"""
Data Export Summary:
-------------------
Query Used:
{fixed_query}

Results:
Number of Rows: {num_rows:,}
Number of Columns: {num_columns}
Compression Ratio: {round(estimated_size_mb/estimated_compressed_size_mb, 2)}:1
Execution Time: {execution_time} seconds
Location: {part_file_path}

Download Link:
{download_url}
"""

send_message(
    subject=f"Data Export Complete: {formatted_end_time} (Execution time: {execution_time} seconds)",
    body=report
)

# Display results
print("\nExecution complete. Email sent with download details.")
display(df)
