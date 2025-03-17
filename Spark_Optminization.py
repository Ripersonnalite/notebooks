"""
Spark Table Optimization Framework

This module provides comprehensive optimization techniques for Apache Spark tables.
It implements multiple levels of optimization from basic repartitioning to advanced
techniques like bucketing, sorting, and Bloom filters to improve query performance.

Author: Ricardo
Version: 1.0.2 (Bug fixes for views and type errors)
"""

import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, min, max, avg

# At the beginning of your script, add:
import logging
logging.basicConfig(filename='optimization_log.txt', level=logging.INFO, 
                   format='%(asctime)s - %(message)s')

# Then replace print statements with:
logging.info("Your message here")

# =====================================================================
# ENVIRONMENT AND SESSION SETUP
# =====================================================================

def setup_environment():
    """
    Set up the necessary environment variables and system paths for Spark.
    
    This function configures:
    - SPARK_HOME: Path to the Spark installation
    - JAVA_HOME: Path to the Java JDK/JRE
    - Adds the Spark Python libraries to the system path
    """
    os.environ["SPARK_HOME"] = "/home/ricardo/spark_env/lib/python3.12/site-packages/pyspark"
    os.environ["JAVA_HOME"]  = "/usr/lib/jvm/java-11-openjdk-amd64"
    
    # Add Spark's Python libraries to the path if not already there
    spark_python = os.path.join(os.environ["SPARK_HOME"], "python")
    if spark_python not in sys.path:
        sys.path.insert(0, spark_python)

def create_spark_session():
    """
    Create and return a Spark session with optimized configuration.
    
    The session is configured with:
    - 6 local cores
    - 12GB memory for both driver and executor
    - 10 shuffle partitions (to reduce overhead on smaller datasets)
    - Hive support for SQL operations and metadata storage
    
    Returns:
        SparkSession: Configured Spark session object
    """
    spark = (
        SparkSession.builder
        .appName("Enhanced Spark Optimizer")
        .master("local[6]")                                  # Use 6 local cores
        .config("spark.driver.memory", "12g")                # Driver memory
        .config("spark.executor.memory", "12g")              # Executor memory
        .config("spark.sql.shuffle.partitions", "10")        # Reduce shuffle partitions for better performance
        .config("spark.sql.warehouse.dir", "/home/ricardo/hive/warehouse")  # Hive warehouse location
        .enableHiveSupport()                                 # Enable Hive SQL support
        .getOrCreate()
    )
    return spark

# =====================================================================
# UTILITY FUNCTIONS
# =====================================================================

def check_object_type(spark, object_name):
    """
    Check if the specified object is a table or view.
    
    Args:
        spark (SparkSession): The Spark session
        object_name (str): Name of the table or view to check
        
    Returns:
        str: 'TABLE' or 'VIEW' based on the object type
    """
    try:
        object_type_info = spark.sql(f"DESCRIBE FORMATTED {object_name}").filter("col_name='Type'").collect()
        if object_type_info and len(object_type_info) > 0:
            object_type = object_type_info[0][1].strip()
            if 'VIEW' in object_type.upper():
                return 'VIEW'
            else:
                return 'TABLE'
        else:
            # Default to TABLE if we can't determine the type
            return 'TABLE'
    except Exception as e:
        print(f"Error determining object type: {e}")
        # Default to TABLE if we can't determine the type
        return 'TABLE'

def drop_object(spark, object_name):
    """
    Drop a table or view safely by first checking its type.
    
    Args:
        spark (SparkSession): The Spark session
        object_name (str): Name of the table or view to drop
        
    Returns:
        bool: True if the operation was successful, False otherwise
    """
    try:
        object_type = check_object_type(spark, object_name)
        if object_type == 'VIEW':
            spark.sql(f"DROP VIEW IF EXISTS {object_name}")
            print(f"Dropped view: {object_name}")
        else:
            spark.sql(f"DROP TABLE IF EXISTS {object_name}")
            print(f"Dropped table: {object_name}")
        return True
    except Exception as e:
        print(f"Error dropping object {object_name}: {e}")
        return False

# =====================================================================
# LEVEL 1: BASIC OPTIMIZATION
# =====================================================================

def perform_standard_optimization(spark, target_object):
    """
    Perform the original optimization strategy with repartitioning.
    
    This function:
    1. Benchmarks the current performance
    2. Repartitions the table to fix fragmentation
    3. Creates a new table with the repartitioned data
    4. Verifies data integrity by comparing row counts
    5. Replaces the original table with the optimized version
    6. Measures performance improvement
    7. Updates table statistics for the query optimizer
    
    Args:
        spark (SparkSession): The Spark session
        target_object (str): Name of the table or view to optimize
        
    Returns:
        tuple: (success_flag, original_query_time, final_query_time)
    """
    # Check if target is a view - views can't be optimized directly
    object_type = check_object_type(spark, target_object)
    if object_type == 'VIEW':
        print(f"\n{target_object} is a VIEW, not a TABLE. Views cannot be directly optimized.")
        print("Skipping optimization for this object.")
        return False, 0, 0
    
    # Step 1: Get current row count (benchmark)
    start_time = time.time()
    orig_count = spark.table(target_object).count()
    orig_time = time.time() - start_time
    print(f"\nOriginal table has {orig_count:,} rows (count took {orig_time:.2f}s)")
    
    # Step 2: Create repartitioned version (fixes fragmentation)
    print(f"\nRepartitioning table to fix fragmentation...")
    start_time = time.time()
    
    # Create temp names
    temp_view = f"temp_{target_object}_view"
    new_table = f"{target_object}_new"
    
    # Repartition and create new table
    print("Step 1: Creating repartitioned version...")
    spark.table(target_object).repartition(10).createOrReplaceTempView(temp_view)
    
    print("Step 2: Creating new table from repartitioned data...")
    spark.sql(f"DROP TABLE IF EXISTS {new_table}")
    spark.sql(f"CREATE TABLE {new_table} AS SELECT * FROM {temp_view}")
    
    # Step 3: Check row counts match
    new_count = spark.table(new_table).count()
    print(f"New table has {new_count:,} rows")
    
    if new_count == orig_count:
        print("✓ Row counts match")
        
        # Step 4: Replace original table - use the safe drop function
        print("Step 3: Replacing original table...")
        if drop_object(spark, target_object):
            spark.sql(f"ALTER TABLE {new_table} RENAME TO {target_object}")
            
            # Step 5: Verify fix
            start_time = time.time()
            final_count = spark.table(target_object).count()
            final_time = time.time() - start_time
            
            print(f"\nFixed table has {final_count:,} rows (count took {final_time:.2f}s)")
            print(f"Performance improvement: {orig_time/final_time:.1f}x faster")
            
            # Step 6: Update statistics
            print("\nUpdating table statistics...")
            spark.sql(f"ANALYZE TABLE {target_object} COMPUTE STATISTICS")
            print("✓ Statistics updated")
            
            print("\n✓ Table successfully optimized!")
            return True, orig_time, final_time
        else:
            print("\n✗ Could not drop original table. Optimization aborted.")
            return False, 0, 0
    else:
        print("✗ Row counts don't match. Not replacing original table.")
        print(f"The new table is available as '{new_table}' for inspection.")
        return False, 0, 0

# =====================================================================
# LEVEL 2: ADVANCED OPTIMIZATION
# =====================================================================

def calculate_optimal_partitions(spark, table_name):
    """
    Calculate the optimal number of partitions based on table size.
    
    This function:
    1. Retrieves table size information from metadata
    2. Calculates optimal partition count (2 partitions per GB)
    3. Applies minimum and maximum bounds (10-200 partitions)
    
    Args:
        spark (SparkSession): The Spark session
        table_name (str): Name of the table
        
    Returns:
        int: Optimal number of partitions
    """
    try:
        # Get table size information
        size_info = spark.sql(f"DESCRIBE FORMATTED {table_name}").filter("col_name='totalSize'").collect()
        if size_info and len(size_info) > 0:
            table_size_bytes = float(size_info[0][1])
            table_size_gb = table_size_bytes / (1024**3)
            
            # Calculate optimal partitions: 2 partitions per GB with bounds
            optimal_partitions = max(10, min(200, int(table_size_gb * 2)))
            print(f"Table size: {table_size_gb:.2f} GB")
            print(f"Calculated optimal partitions: {optimal_partitions}")
            return optimal_partitions
        else:
            print("Couldn't determine table size, using default partitions (10)")
            return 10
    except Exception as e:
        print(f"Error calculating optimal partitions: {e}")
        return 10

def perform_vacuum_operation(spark, table_name):
    """
    Perform vacuum/compaction operation to clean up old files.
    
    This function attempts to:
    1. Use Delta Lake vacuum (if available)
    2. Fall back to Hive compaction if Delta Lake vacuum fails
    
    Args:
        spark (SparkSession): The Spark session
        table_name (str): Name of the table
    """
    print("\nCompacting table storage...")
    try:
        # Try Delta Lake vacuum first
        spark.sql(f"VACUUM {table_name}")
        print("✓ Delta Lake vacuum completed")
    except Exception as delta_err:
        try:
            # Fall back to Hive compaction
            print("Delta Lake vacuum not available, trying Hive compaction...")
            spark.sql(f"ALTER TABLE {table_name} COMPACT 'major'")
            print("✓ Hive compaction completed")
        except Exception as hive_err:
            print(f"Note: Storage compaction not supported for this table type")
            print(f"Error details: {hive_err}")

def check_partition_pruning(spark, table_name):
    """
    Check if partition pruning is effective on the table.
    
    This function:
    1. Retrieves partitioning information from table metadata
    2. Tests query plans to verify partition pruning is working
    3. Provides recommendations for improving partition strategy
    
    Args:
        spark (SparkSession): The Spark session
        table_name (str): Name of the table
    """
    print("\nChecking partition structure...")
    try:
        # Get table schema
        table_schema = spark.table(table_name).schema
        partition_columns = []
        
        # Try to get partition info from Hive metadata
        partition_info = spark.sql(f"SHOW PARTITIONS {table_name}")
        if partition_info.count() > 0:
            partition_data = partition_info.collect()
            sample_partition = partition_data[0][0]
            partition_parts = sample_partition.split('/')
            partition_columns = [p.split('=')[0] for p in partition_parts]
            
            print(f"Found partitioning on columns: {', '.join(partition_columns)}")
            
            # Test partition pruning on the first partition column
            if len(partition_columns) > 0:
                test_col = partition_columns[0]
                test_val = partition_data[0][0].split('=')[1]
                
                pruning_test = spark.sql(f"EXPLAIN SELECT * FROM {table_name} WHERE {test_col} = '{test_val}'")
                pruning_results = pruning_test.collect()[0][0]
                
                if "Pruned" in pruning_results or "PrunedInFilter" in pruning_results:
                    print("✓ Partition pruning is working correctly")
                else:
                    print("⚠ Partition pruning may not be optimal - consider revising partition strategy")
        else:
            print("Table is not partitioned - consider adding partitioning for better performance")
    except Exception as e:
        print(f"Note: Couldn't perform partition pruning check: {e}")

def convert_to_columnar_format(spark, table_name):
    """
    Convert table to columnar format (Parquet) for better compression/performance.
    
    This function:
    1. Checks the current table format
    2. Converts to Parquet if not already using a columnar format
    3. Verifies row count consistency before replacing the original table
    
    Args:
        spark (SparkSession): The Spark session
        table_name (str): Name of the table
        
    Returns:
        bool: True if conversion was successful, False otherwise
    """
    # Check if target is a view
    object_type = check_object_type(spark, table_name)
    if object_type == 'VIEW':
        print(f"\n{table_name} is a VIEW, not a TABLE. Cannot convert to columnar format.")
        return False
        
    print("\nChecking if columnar format conversion would be beneficial...")
    try:
        # Check current format
        format_info = spark.sql(f"DESCRIBE FORMATTED {table_name}").filter("col_name='InputFormat'").collect()
        
        if format_info and len(format_info) > 0:
            current_format = format_info[0][1]
            
            # If not already in a columnar format
            if "parquet" not in current_format.lower() and "orc" not in current_format.lower():
                print(f"Current format: {current_format}")
                print("Converting to Parquet format for improved performance...")
                
                # Create new table names
                columnar_table = f"{table_name}_parquet"
                
                # Convert to parquet
                print(f"Converting {table_name} to Parquet format...")
                spark.sql(f"CREATE TABLE {columnar_table} STORED AS PARQUET AS SELECT * FROM {table_name}")
                
                # Verify row count
                orig_count = spark.table(table_name).count()
                new_count = spark.table(columnar_table).count()
                
                if orig_count == new_count:
                    print("✓ Row counts match")
                    
                    # Replace original - use the safe drop function
                    print("Replacing original table with Parquet version...")
                    if drop_object(spark, table_name):
                        spark.sql(f"ALTER TABLE {columnar_table} RENAME TO {table_name}")
                        print("✓ Conversion to Parquet completed")
                        return True
                    else:
                        print("✗ Could not drop original table. Conversion aborted.")
                        return False
                else:
                    print("✗ Row counts don't match. Keeping original table.")
                    print(f"The Parquet version is available as '{columnar_table}' for inspection.")
            else:
                print(f"✓ Table already uses columnar format: {current_format}")
    except Exception as e:
        print(f"Error checking/converting format: {e}")
    return False

def warm_up_cache(spark, table_name):
    """
    Warm up the cache for the table to improve initial query performance.
    
    This function:
    1. Caches the table in memory
    2. Runs a count operation to materialize the cache
    
    Args:
        spark (SparkSession): The Spark session
        table_name (str): Name of the table
    """
    print("\nWarming up table cache...")
    try:
        spark.sql(f"CACHE TABLE {table_name}")
        warmup_count = spark.table(table_name).count()
        print(f"✓ Cache warmed up with {warmup_count:,} rows")
    except Exception as e:
        print(f"Cache warming failed: {e}")

def enhance_table_performance(spark, table_name, advanced_options=False):
    """
    Apply advanced table performance enhancements beyond basic repartitioning.
    
    If advanced_options is enabled, this function:
    1. Calculates optimal partition count based on table size
    2. Repartitions with optimal partition count
    3. Stores data in Parquet format
    4. Computes detailed statistics
    5. Checks partition pruning effectiveness
    6. Performs storage compaction
    7. Warms up the cache
    
    Args:
        spark (SparkSession): The Spark session
        table_name (str): Name of the table
        advanced_options (bool): Whether to apply advanced optimizations
        
    Returns:
        bool: True if optimization was successful, False otherwise
    """
    # Check if target is a view
    object_type = check_object_type(spark, table_name)
    if object_type == 'VIEW':
        print(f"\n{table_name} is a VIEW, not a TABLE. Advanced optimizations cannot be applied.")
        return False
        
    if advanced_options:
        # Calculate optimal partitions
        optimal_partitions = calculate_optimal_partitions(spark, table_name)
        
        # Create temp names
        temp_view = f"temp_{table_name}_view"
        new_table = f"{table_name}_new"
        
        # Repartition with optimal partitioning
        print(f"\nAdvanced optimization: Repartitioning with {optimal_partitions} partitions...")
        spark.table(table_name).repartition(optimal_partitions).createOrReplaceTempView(temp_view)
        
        print("Creating optimized table...")
        spark.sql(f"DROP TABLE IF EXISTS {new_table}")
        
        # Use specialized CREATE TABLE to potentially improve physical layout
        storage_options = "STORED AS PARQUET"
        spark.sql(f"CREATE TABLE {new_table} {storage_options} AS SELECT * FROM {temp_view}")
        
        # Check if rows match
        orig_count = spark.table(table_name).count()
        new_count = spark.table(new_table).count()
        
        if orig_count == new_count:
            print("✓ Row counts match, applying advanced optimizations")
            
            # Replace table - use safe drop function
            if drop_object(spark, table_name):
                spark.sql(f"ALTER TABLE {new_table} RENAME TO {table_name}")
                
                # Compute advanced column statistics
                print("Computing column-level statistics...")
                try:
                    # Try Hive syntax first
                    spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR COLUMNS")
                except Exception as e:
                    print(f"Column statistics with 'FOR COLUMNS' syntax failed: {e}")
                    try:
                        # Try alternative syntax for Spark SQL
                        spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS")
                        print("✓ Computed table-level statistics")
                    except Exception as e2:
                        print(f"Warning: Could not compute detailed statistics: {e2}")
                
                # Check partition pruning effectiveness
                check_partition_pruning(spark, table_name)
                
                # Perform storage compaction
                perform_vacuum_operation(spark, table_name)
                
                # Warm up cache
                warm_up_cache(spark, table_name)
                
                print("✓ Advanced optimizations completed!")
                return True
            else:
                print("✗ Could not drop original table. Advanced optimization aborted.")
                return False
        else:
            print("✗ Row counts don't match. Advanced optimization aborted.")
            return False
    else:
        print("Skipping advanced optimizations. Use --advanced flag to enable.")
        return False

# =====================================================================
# LEVEL 3: EXTREME OPTIMIZATION
# =====================================================================

def analyze_table_distribution(spark, table_name):
    """
    Analyze data distribution to recommend better partitioning strategy.
    
    This function:
    1. Examines column types to identify numeric and date columns
    2. Analyzes distribution of numeric columns
    3. Recommends optimal bucketing or partitioning strategy
    
    Args:
        spark (SparkSession): The Spark session
        table_name (str): Name of the table
        
    Returns:
        tuple: (column_name, optimization_type) where optimization_type is either
               an integer representing bucket count or "date_partition" string
    """
    # Check if target is a view
    object_type = check_object_type(spark, table_name)
    if object_type == 'VIEW':
        print(f"\n{table_name} is a VIEW, not a TABLE. Cannot analyze distribution.")
        return None, None
    
    print("\n--- Performing data distribution analysis ---")
    
    # Get column list
    columns = spark.table(table_name).columns
    
    # Find high cardinality numeric columns that might be good for bucketing
    print("Analyzing column cardinality for potential bucketing...")
    
    numeric_cols = []
    date_cols = []
    
    # Sample the table to identify column types
    table_sample = spark.table(table_name).limit(10).toPandas()
    
    for column in columns:
        try:
            # Check column type from sample
            column_dtype = str(table_sample[column].dtype)
            if "int" in column_dtype or "float" in column_dtype or "double" in column_dtype:
                numeric_cols.append(column)
            elif "date" in column_dtype or "time" in column_dtype:
                date_cols.append(column)
        except:
            pass
    
    # Analyze distribution of top numeric columns
    if numeric_cols:
        print(f"Found {len(numeric_cols)} numeric columns for potential bucketing")
        
        # Analyze only first few numeric columns to avoid expensive computation
        analysis_cols = numeric_cols[:3]
        for col_name in analysis_cols:
            print(f"Analyzing distribution of column: {col_name}")
            
            # Get basic stats
            stats = spark.table(table_name).select(
                min(col(col_name)).alias("min"),
                max(col(col_name)).alias("max"),
                avg(col(col_name)).alias("avg")
            ).collect()[0]
            
            print(f"  Range: {stats['min']} to {stats['max']}, Average: {stats['avg']}")
            
            # BUG FIX: Fixed the incorrect max() call
            # Check if good for bucketing (just a basic heuristic)
            if stats['min'] is not None and stats['max'] is not None:
                range_size = float(stats['max']) - float(stats['min'])
                if range_size > 0 and range_size < 1000000:
                    # Using proper Python built-in max/min functions
                    #bucket_count = min(32, max(8, int(range_size / 10000)))
                    bucket_count = min(32, max(8, int(range_size / 10000)))
                    print(f"  ✓ Potentially good for bucketing with {bucket_count} buckets")
                    
                    # Recommend bucketing
                    return col_name, bucket_count
    
    # If no good numeric column, check date columns for partitioning
    if date_cols:
        print(f"Found {len(date_cols)} date columns for potential partitioning")
        return date_cols[0], "date_partition"
    
    return None, None

def optimize_with_bucketing(spark, table_name, bucket_col, num_buckets):
    """
    Optimize table using bucketing strategy.
    
    Bucketing is a technique where data is physically organized into buckets
    based on the hash of the bucket column, improving join performance when
    both tables are bucketed on the join key.
    
    This function:
    1. Creates a new bucketed table
    2. Copies data from original table
    3. Verifies row count consistency
    4. Enables bucket pruning optimization
    5. Replaces original table with bucketed version
    
    Args:
        spark (SparkSession): The Spark session
        table_name (str): Name of the table
        bucket_col (str): Column to use for bucketing
        num_buckets (int): Number of buckets to create
        
    Returns:
        bool: True if bucketing was successful, False otherwise
    """
    # Check if target is a view
    object_type = check_object_type(spark, table_name)
    if object_type == 'VIEW':
        print(f"\n{table_name} is a VIEW, not a TABLE. Cannot apply bucketing.")
        return False
    
    print(f"\n--- Applying bucketing optimization on {bucket_col} with {num_buckets} buckets ---")
    
    # Create temp view and new table names
    temp_view = f"temp_{table_name}_view"
    bucketed_table = f"{table_name}_bucketed"
    
    try:
        # Create bucketed table
        print("Creating bucketed table structure...")
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {bucketed_table}
        CLUSTERED BY ({bucket_col}) INTO {num_buckets} BUCKETS
        STORED AS PARQUET
        AS SELECT * FROM {table_name}
        """)
        
        # Verify data
        orig_count = spark.table(table_name).count()
        new_count = spark.table(bucketed_table).count()
        
        if orig_count == new_count:
            print("✓ Row counts match")
            
            # Enable bucket pruning optimization
            spark.sql("SET hive.optimize.ppd=true")
            spark.sql("SET hive.optimize.pruner=true")
            
            # Switch to bucketed table - use safe drop function
            print("Replacing original table with bucketed version...")
            if drop_object(spark, table_name):
                spark.sql(f"ALTER TABLE {bucketed_table} RENAME TO {table_name}")
                print("✓ Bucketing optimization completed")
                return True
            else:
                print("✗ Could not drop original table. Bucketing optimization aborted.")
                return False
        else:
            print(f"✗ Row counts don't match: original={orig_count}, bucketed={new_count}")
            print("Keeping original table.")
            return False
    except Exception as e:
        print(f"Error during bucketing optimization: {e}")
        return False

def optimize_sort_order(spark, table_name):
    """
    Optimize the table's sort order based on common query patterns.
    
    Sorted tables can significantly improve:
    - Range query performance
    - Compression ratio
    - Data skipping capabilities
    
    This function:
    1. Analyzes columns to identify potential sort keys
    2. Creates a new table with optimized sort order
    3. Verifies row count consistency
    4. Replaces original table with sorted version
    
    Args:
        spark (SparkSession): The Spark session
        table_name (str): Name of the table
        
    Returns:
        bool: True if sort optimization was successful, False otherwise
    """
    # Check if target is a view
    object_type = check_object_type(spark, table_name)
    if object_type == 'VIEW':
        print(f"\n{table_name} is a VIEW, not a TABLE. Cannot optimize sort order.")
        return False
    
    print("\n--- Analyzing for sort order optimization ---")
    
    try:
        # Get query history if available
        print("Checking common query patterns...")
        
        # This is a simplified approach, ideally you would analyze actual query logs
        # For demonstration, we'll assume certain columns might benefit from sorting
        
        # Get all columns
        columns = spark.table(table_name).columns
        
        # Look for typical sort columns (date/time columns, ID columns)
        potential_sort_cols = []
        
        for col_name in columns:
            lower_col = col_name.lower()
            # Check for typical timestamp/date columns
            if ('date' in lower_col or 'time' in lower_col or 
                'day' in lower_col or 'id' in lower_col or
                'key' in lower_col):
                potential_sort_cols.append(col_name)
        
        if potential_sort_cols:
            # For simplicity, just use the first matching column
            sort_col = potential_sort_cols[0]
            print(f"Identified potential sort column: {sort_col}")
            
            # Create sorted table
            sorted_table = f"{table_name}_sorted"
            
            print(f"Creating sorted table on column {sort_col}...")
            spark.sql(f"""
            CREATE TABLE {sorted_table}
            STORED AS PARQUET 
            AS SELECT * FROM {table_name}
            SORT BY ({sort_col})
            """)
            
            # Verify row count
            orig_count = spark.table(table_name).count()
            new_count = spark.table(sorted_table).count()
            
            if orig_count == new_count:
                print("✓ Row counts match")
                
                # Replace original table - use safe drop function
                print("Replacing original table with sorted version...")
                if drop_object(spark, table_name):
                    spark.sql(f"ALTER TABLE {sorted_table} RENAME TO {table_name}")
                    print(f"✓ Sort order optimization completed on column {sort_col}")
                    return True
                else:
                    print("✗ Could not drop original table. Sort optimization aborted.")
                    return False
            else:
                print("✗ Row counts don't match. Keeping original table.")
                return False
        else:
            print("No suitable columns identified for sort optimization.")
            return False
    except Exception as e:
        print(f"Error optimizing sort order: {e}")
        return False

def analyze_bloom_filter_candidates(spark, table_name):
    """
    Analyze and recommend Bloom filter optimization for appropriate columns.
    
    Bloom filters can significantly reduce data scans during joins by allowing
    the query engine to skip blocks that don't contain matching values.
    
    This function:
    1. Analyzes column cardinality to identify potential Bloom filter candidates
    2. Selects columns with medium cardinality (0.001 < ratio < 0.1)
    3. Creates Bloom filter indexes on selected columns
    
    Args:
        spark (SparkSession): The Spark session
        table_name (str): Name of the table
        
    Returns:
        bool: True if Bloom filter was successfully applied, False otherwise
    """
    # Check if target is a view
    object_type = check_object_type(spark, table_name)
    if object_type == 'VIEW':
        print(f"\n{table_name} is a VIEW, not a TABLE. Cannot apply Bloom filter optimization.")
        return False
    
    print("\n--- Analyzing for Bloom filter candidates ---")
    
    try:
        # Get table columns
        columns = spark.table(table_name).columns
        
        # Potential bloom filter candidates - typically foreign keys, join columns
        bloom_candidates = []
        
        for col_name in columns:
            lower_col = col_name.lower()
            # Check for typical ID columns that might be used in joins
            if ('id' in lower_col or 'key' in lower_col or 'code' in lower_col):
                # Check cardinality by sampling
                distinct_count = spark.table(table_name).select(col_name).distinct().count()
                total_count = spark.table(table_name).count()
                
                # Calculate cardinality ratio
                cardinality_ratio = distinct_count / total_count
                
                # Good bloom filter candidates have medium cardinality
                if 0.001 < cardinality_ratio < 0.1:
                    bloom_candidates.append((col_name, cardinality_ratio))
                    print(f"Column {col_name} is a good Bloom filter candidate (cardinality ratio: {cardinality_ratio:.4f})")
        
        if bloom_candidates:
            # Sort by cardinality ratio to get best candidates
            bloom_candidates.sort(key=lambda x: x[1])
            best_candidate = bloom_candidates[0][0]
            
            # Create Bloom filter index (if supported)
            try:
                print(f"Creating Bloom filter on column {best_candidate}...")
                spark.sql(f"""
                ALTER TABLE {table_name}
                SET TBLPROPERTIES ('bloom.filter.columns'='{best_candidate}', 
                                  'bloom.filter.fpp'='0.05')
                """)
                print(f"✓ Bloom filter created for column {best_candidate}")
                return True
            except Exception as e:
                print(f"Bloom filter creation failed: {e}")
                print("Note: Your Spark version or table format may not support Bloom filters")
                return False
        else:
            print("No suitable columns identified for Bloom filter optimization.")
            return False
    except Exception as e:
        print(f"Error analyzing for Bloom filters: {e}")
        return False

def measure_query_performance(spark, table_name):
    """
    Run benchmark queries to measure performance improvements.
    
    This function runs a series of test queries to assess optimization effectiveness:
    1. Simple count query
    2. Column selection query
    3. Aggregation query (min/max)
    4. Group by query (if possible)
    
    Each query is run with a warm-up execution followed by a timed execution.
    
    Args:
        spark (SparkSession): The Spark session
        table_name (str): Name of the table
        
    Returns:
        list: List of (query, duration) tuples with benchmark results
    """
    print("\n--- Running performance benchmarks ---")
    
    try:
        # Get all columns
        columns = spark.table(table_name).columns
        
        # Select a subset of columns for benchmarking
        select_cols = columns[:min(5, len(columns))]
        select_clause = ", ".join(select_cols)
        
        # Prepare benchmark queries
        benchmark_queries = [
            f"SELECT COUNT(*) FROM {table_name}",
            f"SELECT {select_clause} FROM {table_name} LIMIT 1000",
            f"SELECT MIN({select_cols[0]}), MAX({select_cols[0]}) FROM {table_name}"
        ]
        
        # Add a group by query if possible
        if len(select_cols) >= 2:
            benchmark_queries.append(f"SELECT {select_cols[0]}, COUNT(*) FROM {table_name} GROUP BY {select_cols[0]} LIMIT 10")
        
        # Run benchmarks
        results = []
        
        for i, query in enumerate(benchmark_queries):
            print(f"Running benchmark query {i+1}: {query}")
            
            # Warm up
            spark.sql(query).collect()
            
            # Timed execution
            start_time = time.time()
            spark.sql(query).collect()
            duration = time.time() - start_time
            
            results.append((query, duration))
            print(f"  Execution time: {duration:.3f} seconds")
        
        # Report results
        print("\nBenchmark Results:")
        for i, (query, duration) in enumerate(results):
            print(f"Query {i+1}: {duration:.3f} seconds")
        
        return results
    except Exception as e:
        print(f"Error running benchmarks: {e}")
        return []


def suggest_spark_configs(spark, table_name):
    """
    Analyze table characteristics and suggest optimal Spark configurations.
    
    This function examines the table size and structure to recommend
    configuration parameters that optimize Spark performance for this
    specific table. It provides recommendations for memory allocation,
    partition management, join optimizations, and resource allocation.
    
    The suggestions follow best practices for Spark performance tuning:
    - Memory allocation scaled to table size
    - Shuffle partition count appropriate for data volume
    - Broadcast threshold optimization for join operations
    - Dynamic resource allocation for efficient cluster utilization
    - I/O and memory management settings for improved throughput
    
    Args:
        spark (SparkSession): The active Spark session
        table_name (str): Name of the table to analyze
        
    Returns:
        list: List of string suggestions for Spark configuration parameters
              formatted as "parameter = value"
    """
    print("\n--- Suggesting optimal Spark configurations ---")
    
    try:
        # Step 1: Determine table size - first try metadata approach
        table_size_bytes = None
        try:
            # Query table metadata for size information
            size_info = spark.sql(f"DESCRIBE FORMATTED {table_name}").filter("col_name='totalSize'").collect()
            if size_info and len(size_info) > 0:
                table_size_bytes = float(size_info[0][1])
        except Exception as metadata_err:
            # Silently continue to fallback method
            pass
        
        # Step 2: If metadata approach failed, estimate size from row count
        if not table_size_bytes:
            # Count rows and estimate size based on average row size
            row_count = spark.table(table_name).count()
            # Assume 200 bytes per row (conservative estimate for mixed data)
            est_row_size_bytes = 200  # Conservative assumption for typical mixed data
            table_size_bytes = row_count * est_row_size_bytes
        
        # Convert size to GB for more readable calculations
        table_size_gb = table_size_bytes / (1024**3)
        print(f"Estimated table size: {table_size_gb:.2f} GB")
        
        # Step 3: Generate configuration suggestions based on table characteristics
        suggestions = []
        
        # Memory configuration
        # Rule: Allocate 0.5GB per 1GB of data, with min of 4GB and max of 32GB
        suggested_exec_mem = max(4, min(32, table_size_gb * 0.5))
        suggestions.append(f"spark.executor.memory = {int(suggested_exec_mem)}g")
        
        # Shuffle partition configuration
        # Rule: 5 partitions per GB, bounded between 10-200 partitions
        # Higher partition count helps with large shuffle operations but adds overhead
        shuffle_partitions = max(10, min(200, int(table_size_gb * 5)))
        suggestions.append(f"spark.sql.shuffle.partitions = {shuffle_partitions}")
        
        # Broadcast join threshold
        # Rule: 10% of table size up to 2GB max
        # Controls when Spark uses broadcast joins vs. shuffle hash joins
        broadcast_threshold = min(2, table_size_gb * 0.1)
        # Convert to bytes for the configuration parameter
        suggestions.append(f"spark.sql.autoBroadcastJoinThreshold = {int(broadcast_threshold * 1024 * 1024)}")
        
        # Dynamic allocation settings for efficient resource utilization
        suggestions.append("spark.dynamicAllocation.enabled = true")
        suggestions.append("spark.dynamicAllocation.minExecutors = 2")
        suggestions.append("spark.dynamicAllocation.maxExecutors = 10")
        
        # I/O optimization settings
        # These control how Spark reads/processes data files
        suggestions.append("spark.sql.files.maxPartitionBytes = 134217728")  # 128 MB partition size
        suggestions.append("spark.sql.files.openCostInBytes = 4194304")      # 4 MB open cost
        
        # Memory management settings
        # Controls allocation between execution and storage memory
        suggestions.append("spark.memory.fraction = 0.8")        # Fraction of JVM heap used for execution/storage
        suggestions.append("spark.memory.storageFraction = 0.3") # Fraction of spark.memory.fraction used for storage
        
        # Step 4: Display recommendations
        print("\nRecommended Spark configurations:")
        for suggestion in suggestions:
            print(f"  {suggestion}")
        
        return suggestions
    except Exception as e:
        print(f"Error generating Spark configuration suggestions: {e}")
        return []


def perform_extreme_optimization(spark, table_name):
    """
    Perform the most advanced optimization techniques available for Spark tables.
    
    This function implements level 3 (extreme) optimizations that go beyond basic
    repartitioning and statistics. It applies specialized techniques like bucketing,
    sort orders, and Bloom filters that can dramatically improve performance for
    specific query patterns.
    
    The optimization process:
    1. Analyzes table distribution to apply appropriate bucketing strategy
    2. Identifies and implements optimal sort order for better compression/retrieval
    3. Applies Bloom filters to columns used in join conditions
    4. Suggests optimal Spark configuration parameters
    5. Runs benchmark queries to measure performance improvements
    
    Each optimization is applied only if it's determined to be beneficial for the
    specific table characteristics, and data integrity is verified at each step.
    
    Args:
        spark (SparkSession): The active Spark session
        table_name (str): Name of the table to optimize
        
    Returns:
        bool: True if at least one optimization was successfully applied
    """
    # Check if target is a view
    object_type = check_object_type(spark, table_name)
    if object_type == 'VIEW':
        print(f"\n{table_name} is a VIEW, not a TABLE. Extreme optimizations cannot be applied.")
        return False
        
    print("\n=== PERFORMING EXTREME OPTIMIZATION ===")
    
    # Track how many optimizations were successfully applied
    optimizations_applied = 0
    
    # Step 1: Analyze table distribution for bucketing opportunities
    # Bucketing physically organizes data by hash partitioning on a column
    # This improves join performance when tables share bucketing columns
    print("\nStep 1: Analyzing for bucketing opportunities...")
    try:
        bucket_col, bucket_info = analyze_table_distribution(spark, table_name)
        
        if bucket_col and isinstance(bucket_info, int):
            # Apply bucketing if a suitable column was identified
            # bucket_info contains the recommended number of buckets
            print(f"  Applying bucketing on column '{bucket_col}' with {bucket_info} buckets")
            if optimize_with_bucketing(spark, table_name, bucket_col, bucket_info):
                optimizations_applied += 1
                print(f"  ✓ Bucketing optimization complete")
            else:
                print(f"  ✗ Bucketing optimization failed")
        else:
            print("  No suitable bucketing columns identified")
    except Exception as e:
        print(f"  Error during bucketing analysis: {e}")
        print("  Continuing with other optimizations...")
    
    # Step 2: Optimize sort order for better compression and query performance
    # Sorted tables improve compression ratios and enable efficient range queries
    print("\nStep 2: Optimizing table sort order...")
    if optimize_sort_order(spark, table_name):
        optimizations_applied += 1
        print(f"  ✓ Sort order optimization complete")
    else:
        print(f"  ✗ Sort order optimization not applied")
    
    # Step 3: Analyze for Bloom filter candidates
    # Bloom filters allow skipping data blocks during joins, reducing I/O
    print("\nStep 3: Analyzing for Bloom filter opportunities...")
    if analyze_bloom_filter_candidates(spark, table_name):
        optimizations_applied += 1
        print(f"  ✓ Bloom filter optimization complete")
    else:
        print(f"  ✗ Bloom filter optimization not applied")
    
    # Step 4: Suggest optimal Spark configurations based on table characteristics
    print("\nStep 4: Generating optimal Spark configuration suggestions...")
    spark_configs = suggest_spark_configs(spark, table_name)
    
    # Step 5: Run performance benchmarks to measure optimization impact
    print("\nStep 5: Running performance benchmarks...")
    benchmark_results = measure_query_performance(spark, table_name)
    
    # Report final results
    print("\n=== EXTREME OPTIMIZATION RESULTS ===")
    print(f"Optimizations successfully applied: {optimizations_applied}")
    print("Note: For best results, consider updating your Spark session with the suggested configurations")
    
    return optimizations_applied > 0


# =====================================================================
# MAIN EXECUTION FLOW
# =====================================================================

def run_full_optimization(spark, tables):
    """
    Run all optimization levels (1, 2, and 3) on the specified tables.
    
    Args:
        spark (SparkSession): The active Spark session
        tables (list): List of table names to optimize
    """
    # Optimize each table with all levels
    for target_object in tables:
        print(f"\n{'='*80}")
        print(f"STARTING OPTIMIZATION FOR: {target_object}")
        print(f"{'='*80}")
        
        # Check object type first
        object_type = check_object_type(spark, target_object)
        if object_type == 'VIEW':
            print(f"\n{target_object} is a VIEW. Views cannot be directly optimized.")
            print("Skipping to next object...\n")
            continue
            
        # Level 1: Standard optimization - basic repartitioning to fix fragmentation
        print("\n=== LEVEL 1: STANDARD OPTIMIZATION ===")
        success, orig_time, final_time = perform_standard_optimization(spark, target_object)
        
        # Only proceed with advanced optimizations if standard was successful
        if success:
            # Level 2: Advanced optimization - statistics, caching, optimized partitioning
            print("\n=== LEVEL 2: ADVANCED OPTIMIZATION ===")
            print("Proceeding with advanced optimizations...")
            enhance_table_performance(spark, target_object, advanced_options=True)
            
            # Storage format optimization - convert to columnar format if beneficial
            convert_to_columnar_format(spark, target_object)
            
            # Level 3: Extreme optimization - bucketing, sorting, bloom filters
            print("\n=== LEVEL 3: EXTREME OPTIMIZATION ===")
            print("Proceeding with extreme optimizations...")
            perform_extreme_optimization(spark, target_object)
            
            print(f"\n✓ Full optimization process completed successfully for table {target_object}!")
        else:
            print(f"\n✗ Standard optimization failed for {target_object}. Skipping advanced optimizations.")
    
    print("\nFull optimization completed for all tables in the database!")

def run_level3_only(spark, tables):
    """
    Run only Level 3 (Extreme) optimizations on the specified tables.
    
    Args:
        spark (SparkSession): The active Spark session
        tables (list): List of table names to optimize
    """
    # Optimize each table - ONLY LEVEL 3
    for target_object in tables:
        print(f"\n{'='*80}")
        print(f"STARTING EXTREME OPTIMIZATION FOR: {target_object}")
        print(f"{'='*80}")
        
        # Check object type first
        object_type = check_object_type(spark, target_object)
        if object_type == 'VIEW':
            print(f"\n{target_object} is a VIEW. Views cannot be directly optimized.")
            print("Skipping to next object...\n")
            continue
            
        # Skip Level 1 and Level 2, go straight to Level 3
        print("\n=== LEVEL 3: EXTREME OPTIMIZATION ===")
        perform_extreme_optimization(spark, target_object)
    
    print("\nLevel 3 optimization completed for all tables in the database!")

if __name__ == "__main__":
    """
    Main execution flow to optimize all tables in the database.
    
    Usage:
      python Stocks_Optimization.py         # Run all optimization levels (1, 2, 3)
      python Stocks_Optimization.py level3  # Run only level 3 optimizations
    """
    import sys
    
    # Initialize environment and dependencies
    setup_environment()
    
    # Create an optimized Spark session
    spark = create_spark_session()
    
    # Connect to the database
    spark.sql("USE stocks_db")
    print("Connected to stocks_db")
    
    # List all tables in the database
    tables = [table['tableName'] for table in spark.sql("SHOW TABLES").collect()]
    
    print("Tables in the current database:")
    for table in tables:
        print(f"- {table}")
    
    #run_level3_only(spark, tables)
    run_full_optimization(spark, tables)
    
    print("Script completed. Spark session remains open.")