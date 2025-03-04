#!/bin/bash

# Error handling
handle_error() {
  echo "Error occurred at line $1, command: '$2'"
  echo "Installation failed. Please check the error message above."
  exit 1
}

# Set up error handling trap
trap 'handle_error ${LINENO} "$BASH_COMMAND"' ERR

# Continue on error but track failures
set +e
SETUP_SUCCESS=true

# Create a directory for downloads
DOWNLOAD_DIR="$HOME/downloads"
mkdir -p "$DOWNLOAD_DIR"

echo "Starting setup of Spark, Hadoop, Hive with PostgreSQL metastore..."

# Update System Packages
echo "Updating system packages..."
sudo apt update && sudo apt upgrade -y

# Install required dependencies
echo "Installing required dependencies..."
sudo apt install -y wget openjdk-11-jdk openjdk-11-jre python3-pip python3-venv python3-dev python3-full postgresql postgresql-contrib libpq-dev openssh-server openssh-client

# Create Python Virtual Environment first (to avoid externally managed environment error)
echo "Creating Python virtual environment..."
PYTHON_CMD="python3"
echo "Checking Python version..."
PY_VERSION=$(python3 --version)
echo "Using $PY_VERSION"

$PYTHON_CMD -m venv ~/spark_env
source ~/spark_env/bin/activate

# Now install gdown inside the virtual environment
echo "Installing gdown inside virtual environment..."
pip install gdown imblearn nltk

# Set JAVA_HOME explicitly for the current script
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$PATH:$JAVA_HOME/bin

# Verify JAVA_HOME is set
echo "JAVA_HOME is set to: $JAVA_HOME"

# Set up SSH for passwordless login to localhost
echo "Setting up SSH for passwordless login..."
ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa || true
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

# Configure SSH to not prompt for host key verification for localhost
echo "Configuring SSH host key verification..."
cat > ~/.ssh/config << 'EOF'
Host localhost
  StrictHostKeyChecking no
Host 127.0.0.1
  StrictHostKeyChecking no
Host 0.0.0.0
  StrictHostKeyChecking no
Host $(hostname)
  StrictHostKeyChecking no
EOF
chmod 0600 ~/.ssh/config

# Test SSH connection
echo "Testing SSH connection to localhost..."
ssh -o StrictHostKeyChecking=no localhost "echo SSH to localhost successful" || {
    echo "Warning: SSH to localhost failed. This may cause Hadoop startup issues."
    echo "Continuing with installation..."
    sleep 3
}

# We already created and activated the virtual environment earlier
echo "Using Python virtual environment at ~/spark_env"

# Install PySpark & Other Python Libraries
echo "Installing Python libraries..."
# Install pip build dependencies
pip install wheel setuptools
pip cache purge


# Install core packages first
echo "Installing core Python packages..."
pip install pyspark==3.5.1 
pip install findspark==1.4.0
pip install pyarrow plotly

# Install data science packages
echo "Installing data science packages..."
pip install pandas numpy matplotlib seaborn scikit-learn streamlit_option_menu

# Install notebook support
echo "Installing Jupyter support..."
pip install jupyter notebook

# Install application packages
echo "Installing application frameworks..."
pip install streamlit 

# Install Dagster
echo "Installing Dagster..."
pip install dagster dagit dagster-postgres 

# Install database adapters
echo "Installing database adapters..."
pip install psycopg2-binary

# Install gdown for file downloads (ensure it's installed again after venv activation)
echo "Installing gdown in virtual environment..."
pip install gdown

# Set up PostgreSQL for Hive metastore
echo "Setting up PostgreSQL for Hive metastore..."

# Make sure PostgreSQL is running
echo "Ensuring PostgreSQL service is running..."
sudo systemctl start postgresql || { echo "Failed to start PostgreSQL"; exit 1; }
sudo systemctl enable postgresql

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
for i in {1..30}; do
    if sudo -u postgres psql -c "SELECT 1" >/dev/null 2>&1; then
        echo "PostgreSQL is ready"
        break
    fi
    echo "Waiting for PostgreSQL... ($i/30)"
    sleep 1
    if [ $i -eq 30 ]; then
        echo "PostgreSQL did not become ready in time"
        exit 1
    fi
done

# Create PostgreSQL user and database for Hive with proper permissions
echo "Creating PostgreSQL user and database for Hive..."
sudo -u postgres psql -c "DROP USER IF EXISTS hiveuser;" 2>/dev/null || true
sudo -u postgres psql -c "DROP DATABASE IF EXISTS metastore;" 2>/dev/null || true
sudo -u postgres psql -c "CREATE USER hiveuser WITH PASSWORD 'hivepassword';"
sudo -u postgres psql -c "CREATE DATABASE metastore OWNER hiveuser;"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE metastore TO hiveuser;"

# Grant schema permissions to hiveuser
sudo -u postgres psql -d metastore -c "GRANT ALL ON SCHEMA public TO hiveuser;"
sudo -u postgres psql -d metastore -c "ALTER USER hiveuser WITH SUPERUSER;"

# Verify PostgreSQL setup
echo "Verifying PostgreSQL setup..."
if sudo -u postgres psql -c "SELECT 1 FROM pg_database WHERE datname='metastore'" | grep -q 1; then
    echo "âœ“ PostgreSQL metastore database created successfully"
else
    echo "âœ— Failed to create metastore database"
    exit 1
fi

# Download and install Hadoop (if not already done)
echo "Checking for Hadoop installation..."
if [ -d "$HOME/hadoop" ]; then
    echo "Hadoop directory already exists, skipping download."
    HADOOP_HOME="$HOME/hadoop"
else
    echo "Downloading and installing Hadoop from Google Drive..."
    cd "$DOWNLOAD_DIR"
    
    # Download from Google Drive instead of Apache mirrors
    gdown 1-iagXKcm3iOy85jJ7d8h39NbhvyqJUCj -O hadoop-3.3.6.tar.gz
    
    tar -xzf hadoop-3.3.6.tar.gz -C "$HOME"
    mv "$HOME/hadoop-3.3.6" "$HOME/hadoop"
    
    # Set Hadoop home explicitly for the rest of the script to use
    HADOOP_HOME="$HOME/hadoop"
fi

echo "HADOOP_HOME set to: $HADOOP_HOME"

# Download and install Hive (if not already done)
echo "Checking for Hive installation..."
if [ -d "$HOME/hive" ]; then
    echo "Hive directory already exists, skipping download."
    HIVE_HOME="$HOME/hive"
else
    echo "Downloading and installing Hive from Google Drive..."
    cd "$DOWNLOAD_DIR"
    
    # Download from Google Drive instead of Apache mirrors
    gdown 1-f1xiY-qztO055oOo34wHR71nra35O5S -O apache-hive-3.1.3-bin.tar.gz
    
    tar -xzf apache-hive-3.1.3-bin.tar.gz -C "$HOME"
    mv "$HOME/apache-hive-3.1.3-bin" "$HOME/hive"
    
    # Set Hive home explicitly for the rest of the script to use
    HIVE_HOME="$HOME/hive"
fi

echo "HIVE_HOME set to: $HIVE_HOME"

# Download PostgreSQL JDBC Driver (if needed)
echo "Checking for PostgreSQL JDBC Driver..."
if [ -f "$HIVE_HOME/lib/postgresql-42.6.0.jar" ]; then
    echo "PostgreSQL JDBC driver already exists in Hive lib directory."
else
    echo "Downloading PostgreSQL JDBC Driver from Google Drive..."
    cd "$DOWNLOAD_DIR"
    
    # Download from Google Drive instead of PostgreSQL website
    gdown 1-e1qMIpCOOAfkF7g2IUcNht2w4kjIUR6 -O postgresql-42.6.0.jar
    
    cp postgresql-42.6.0.jar "$HIVE_HOME/lib/"
fi

# Set up environment variables for the current script session
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop
export HIVE_HOME=$HOME/hive
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin

##############################################################################
# Set up environment variables in .bashrc for future sessions (ADJUSTED BLOCK)
##############################################################################
echo "Setting up environment variables in .bashrc..."
if ! grep -q "AUTO_ACTIVATE_SPARK_ENV" ~/.bashrc; then
    cat >> ~/.bashrc << 'EOF'

# --- Start of Spark/Hadoop/Hive environment variables (AUTO_ACTIVATE_SPARK_ENV) ---
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME="$HOME/hadoop"
export HIVE_HOME="$HOME/hive"

# --- Auto-activate the spark_env virtual environment ---
if [ -z "$VIRTUAL_ENV" ] && [ -f "$HOME/spark_env/bin/activate" ]; then
    source "$HOME/spark_env/bin/activate"
fi

# --- Set SPARK_HOME if in a virtual environment ---
export SPARK_HOME="/home/ricardo/spark_env/lib/python3.12/site-packages/pyspark"

# --- Update PATH to include important binaries ---
export PATH="$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin"
if [ -n "$SPARK_HOME" ]; then
    export PATH="$PATH:$SPARK_HOME/bin"
fi

# --- Hadoop and Hive configuration directories ---
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export HIVE_CONF_PATH="$HIVE_HOME/conf"

# --- PySpark-related PYTHONPATH (set only if SPARK_HOME is defined) ---
if [ -n "$SPARK_HOME" ]; then
    export PYTHONPATH="$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"
fi

# --- Set Hadoop native libraries for Java ---
export HADOOP_OPTS="$HADOOP_OPTS -Djava.library.path=$HADOOP_HOME/lib/native"

# --- Optional alias for manual virtual environment activation ---
alias activate="source $HOME/spark_env/bin/activate"
# --- End of Spark/Hadoop/Hive environment variables (AUTO_ACTIVATE_SPARK_ENV) ---
EOF
    echo "Environment variables added to .bashrc"
else
    echo "Environment variables already present in .bashrc"
fi
##############################################################################
# End adjusted block
##############################################################################

# Configure Hadoop for single node (pseudo-distributed)
echo "Configuring Hadoop for single-node mode..."
echo "Creating directory: $HADOOP_HOME/etc/hadoop"
mkdir -p "$HADOOP_HOME/etc/hadoop"

# Configure Hadoop to use single node without SSH
echo "Updating Hadoop configuration for local setup..."

# hadoop-env.sh - Set JAVA_HOME explicitly
cat > "$HADOOP_HOME/etc/hadoop/hadoop-env.sh" << 'EOF'
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
EOF

# core-site.xml - Use local filesystem instead of HDFS for simplicity
cat > "$HADOOP_HOME/etc/hadoop/core-site.xml" << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>file:///</value>
        <description>Use local filesystem instead of HDFS</description>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/tmp/hadoop-${user.name}</value>
    </property>
</configuration>
EOF

# hdfs-site.xml - Configure for single node
cat > "$HADOOP_HOME/etc/hadoop/hdfs-site.xml" << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>
EOF

# mapred-site.xml
cat > "$HADOOP_HOME/etc/hadoop/mapred-site.xml" << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>local</value>
    </property>
</configuration>
EOF

# Configure Hive for local mode
echo "Configuring Hive with local metastore..."
mkdir -p "$HIVE_HOME/conf"

# hive-site.xml for local mode
cat > "$HIVE_HOME/conf/hive-site.xml" << 'EOF'
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:postgresql://localhost:5432/metastore</value>
        <description>PostgreSQL JDBC connection string</description>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionDriverName</name>
        <value>org.postgresql.Driver</value>
        <description>PostgreSQL JDBC driver class</description>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionUserName</name>
        <value>hiveuser</value>
        <description>PostgreSQL user name</description>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionPassword</name>
        <value>hivepassword</value>
        <description>PostgreSQL password</description>
    </property>
    <property>
        <name>hive.metastore.warehouse.dir</name>
        <value>file:///tmp/hive/warehouse</value>
        <description>Location of default database for the warehouse</description>
    </property>
    <property>
        <name>hive.metastore.uris</name>
        <value>thrift://localhost:9083</value>
        <description>Thrift URI for the remote metastore</description>
    </property>
    <property>
        <name>datanucleus.autoCreateSchema</name>
        <value>true</value>
    </property>
    <property>
        <name>datanucleus.fixedDatastore</name>
        <value>false</value>
    </property>
    <property>
        <name>hive.server2.enable.doAs</name>
        <value>false</value>
    </property>
</configuration>
EOF

# Create necessary directories
echo "Creating local directories for Hive warehouse..."
mkdir -p /tmp/hadoop-${USER}
mkdir -p /tmp/hive/warehouse
chmod -R 777 /tmp/hive

# Remove SLF4J Library Conflict before initializing schema
echo "Checking for SLF4J library conflicts..."
rm -f "$HIVE_HOME/lib/log4j-slf4j-impl-*.jar"

# Initialize Hive schema
echo "Initializing Hive schema..."
"$HIVE_HOME/bin/schematool" -dbType postgres -initSchema

# Create systemd service file for Hive Metastore
echo "Creating systemd service file for Hive Metastore..."
cat > /tmp/hive-metastore.service << EOF
[Unit]
Description=Hive Metastore Service
After=network.target postgresql.service

[Service]
User=${USER}
Environment="JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64"
Environment="HADOOP_HOME=${HADOOP_HOME}"
Environment="HIVE_HOME=${HIVE_HOME}"
ExecStart=${HIVE_HOME}/bin/hive --service metastore
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

# Install the systemd service
echo "Installing Hive Metastore systemd service..."
sudo mv /tmp/hive-metastore.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable hive-metastore
sudo systemctl start hive-metastore

# Check if the service is running
echo "Checking Hive Metastore service status..."
if sudo systemctl is-active --quiet hive-metastore; then
    echo "âœ“ Hive Metastore service is running"
else
    echo "âš ï¸ Warning: Hive Metastore service failed to start. Check logs with: sudo journalctl -u hive-metastore"
    # Start it manually for now to complete the setup
    echo "Starting Hive Metastore manually for current session..."
    "$HIVE_HOME/bin/hive" --service metastore &
    METASTORE_PID=$!
    sleep 5
fi

# Find Spark home
SPARK_HOME=$(pip show pyspark | grep Location | awk '{print $2}')/pyspark
echo "SPARK_HOME set to: $SPARK_HOME"
export SPARK_HOME

# Create configuration for Spark with Hive
echo "Creating Spark configuration for Hive integration..."
mkdir -p "$SPARK_HOME/conf"
cp "$HIVE_HOME/conf/hive-site.xml" "$SPARK_HOME/conf/"

# Create a test script to verify installation
cat > ~/test_spark_hive.py << 'EOF'
import os
import findspark
findspark.init()
from pyspark.sql import SparkSession

print("Creating Spark session with Hive support...")
spark = SparkSession.builder \
    .appName("Test Spark Hive Integration") \
    .config("spark.sql.warehouse.dir", "file:///tmp/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

print("Creating test database and table...")
spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
spark.sql("USE test_db")
spark.sql("CREATE TABLE IF NOT EXISTS test_table (id INT, name STRING)")
spark.sql("INSERT INTO test_table VALUES (1, 'Test1'), (2, 'Test2')")

print("Querying the table...")
result = spark.sql("SELECT * FROM test_table").show()

print("Installation successful if data is displayed above!")
spark.stop()
EOF

# Create a basic Streamlit app for testing
mkdir -p ~/streamlit_app
cat > ~/streamlit_app/app.py << 'EOF'
import streamlit as st
import findspark
findspark.init()
from pyspark.sql import SparkSession

st.title("Spark + Streamlit Demo")

# Initialize Spark session with Hive support
@st.cache_resource
def get_spark():
    return SparkSession.builder \
        .appName("Streamlit Spark Integration") \
        .config("spark.sql.warehouse.dir", "file:///tmp/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

spark = get_spark()

st.header("Query Hive Table")
query = st.text_area("Enter SQL Query", "SELECT * FROM test_db.test_table")

if st.button("Run Query"):
    try:
        result = spark.sql(query)
        st.dataframe(result.toPandas())
    except Exception as e:
        st.error(f"Error: {e}")
EOF

# Create a simple Dagster pipeline for testing
mkdir -p ~/dagster_project
cat > ~/dagster_project/spark_dagster.py << 'EOF'
from dagster import asset, materialize, AssetExecutionContext, Definitions
import findspark
findspark.init()
from pyspark.sql import SparkSession

@asset
def spark_hive_integration(context: AssetExecutionContext):
    # Initialize Spark session with Hive support
    spark = SparkSession.builder \
        .appName("Dagster Spark Integration") \
        .config("spark.sql.warehouse.dir", "file:///tmp/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()
    
    # Create a test table if not exists
    spark.sql("USE test_db")
    spark.sql("CREATE TABLE IF NOT EXISTS dagster_test (id INT, name STRING)")
    spark.sql("INSERT INTO dagster_test VALUES (101, 'DagsterTest1'), (102, 'DagsterTest2')")
    
    # Query the table
    result = spark.sql("SELECT * FROM dagster_test").collect()
    context.log.info(f"Data from Hive: {result}")
    
    spark.stop()
    return result

defs = Definitions(assets=[spark_hive_integration])

if __name__ == "__main__":
    result = materialize([spark_hive_integration])
    print("Dagster pipeline executed successfully!")
EOF

# Verify the installation
echo "Verifying installation..."

# Check Python and PySpark
if python -c "import pyspark" 2>/dev/null; then
    echo "Ok - PySpark is installed correctly"
else
    echo "âœ— PySpark installation failed"
    SETUP_SUCCESS=false
fi

# Check Java
if java -version 2>&1 | grep -q "version \"11"; then
    echo "âœ“ Java 11 is installed correctly"
else
    echo "â„¹ï¸ Java is installed but may not be version 11"
    java -version
fi

# Check Hadoop
if [ -d "$HADOOP_HOME" ] && [ -x "$HADOOP_HOME/bin/hadoop" ]; then
    echo "âœ“ Hadoop is installed correctly"
else
    echo "âœ— Hadoop installation failed"
    SETUP_SUCCESS=false
fi

# Check Hive
if [ -d "$HIVE_HOME" ] && [ -x "$HIVE_HOME/bin/hive" ]; then
    echo "âœ“ Hive is installed correctly"
else
    echo "âœ— Hive installation failed"
    SETUP_SUCCESS=false
fi

# Check PostgreSQL
if systemctl is-active --quiet postgresql; then
    echo "âœ“ PostgreSQL is running"
else
    echo "âœ— PostgreSQL is not running"
    SETUP_SUCCESS=false
fi

# Check Hive Metastore Service
if systemctl is-active --quiet hive-metastore; then
    echo "âœ“ Hive Metastore service is running via systemd"
elif ps -ef | grep -v grep | grep "hive.*metastore" > /dev/null; then
    echo "âœ“ Hive Metastore is running (manual process)"
else
    echo "âœ— Hive Metastore is not running"
    SETUP_SUCCESS=false
fi

echo ""
if [ "$SETUP_SUCCESS" = true ]; then
    echo "==========================================="
    echo "âœ… Installation and configuration complete!"
    echo "==========================================="
else
    echo "=================================================="
    echo "âš ï¸  Installation completed with some issues"
    echo "=================================================="
fi

# Keep the existing gdown commands that are already there
gdown 1-6v1o4x1PtoJFs53AtTHCme5nLmGLee4 -O ~/ 
gdown 1-DnV98yFEG_ON_HvidhKIYDXAYg-v6v2 -O ~/
gdown 1-4eJ9ONGmKn45jT8S7FKJU_QYvqq_Axb -O ~/
gdown 1bpuW3eeGnqJS5_xJf4jHDudYQTFDLW0I -O ~/
gdown 1-LdMZJFFAKb2hEzmWK2G4F8Si05V8oFK -O ~/
gdown 1-Epz_Jdij6tf8z5A9ZykNMgqOOQqSD_A -O ~/dagster_project/spark_dagster.py

echo ""
echo "To test the installation, run:"
echo "source ~/.bashrc"
echo "source ~/spark_env/bin/activate"
echo "python ~/test_spark_hive.py"
echo ""
echo "To run the Streamlit app:"
echo "cd ~/streamlit_app"
echo "streamlit run app.py"
echo ""
echo "To run the Dagster pipeline:"
echo "cd ~/dagster_project"
echo "python spark_dagster.py"
echo ""
echo "The Hive metastore service is now automatically managed by systemd!"
echo "You can check its status with: sudo systemctl status hive-metastore"
echo "  - Start it with: sudo systemctl start hive-metastore"
echo "  - Stop it with: sudo systemctl stop hive-metastore"
echo "  - View logs with: sudo journalctl -u hive-metastore"
echo ""
echo "Enjoy your Spark and Hive setup with PostgreSQL metastore!"
#source ~/spark_env/bin/activate
#dagit -f ~/dagster_project/spark_dagster.py -h 0.0.0.0 -p 3030
#mkdir -p ~/.streamlit
#nano ~/.streamlit/credentials.toml
#echo -e "[server]\nheadless = true" > ~/.streamlit/config.toml
#sudo -u postgres psql -d metastore -c "SELECT * FROM \"DBS\";"
#sudo -u postgres psql -d metastore -c "SELECT * FROM \"TBLS\" WHERE \"DB_ID\" IN (SELECT \"DB_ID\" FROM \"DBS\" WHERE \"NAME\"='stocks_db');"
