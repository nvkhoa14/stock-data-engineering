import sys
import duckdb
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, lit
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as fs
from urllib.parse import urlparse

def get_latest_parquet_file(hdfs_directory):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Get Latest Parquet File") \
        .getOrCreate()

    # List all files in the directory
    files_df = spark.read.format("binaryFile").load(hdfs_directory + "/*.parquet")

    # Extract file names and modification times
    files_df = files_df.withColumn("file_name", input_file_name())

    # Order files by modification time descending and get the latest file
    latest_file = files_df.orderBy("modificationTime", ascending=False).limit(1).collect()[0].file_name

    print(latest_file)
    # Stop Spark session
    spark.stop()

    return latest_file

def is_parquet_file_empty(hdfs_url, threshold_size=2000):
    # Extract the HDFS path from the URL
    hdfs_path = urlparse((hdfs_url)).path
    
    # Create an HDFS client
    hdfs = fs.HadoopFileSystem('0.0.0.0', port=9000, user='nvkhoa14')
    
    # Get the file status
    file_info = hdfs.get_file_info(hdfs_path)
    
    # Get the file size
    file_size = file_info.size
    
    # Check if the file size is greater than the threshold
    return file_size > threshold_size

def process(parquet_file_path):
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("Insert Parquet into DuckDB (dim_times, fact_candles)") \
        .config("spark.sql.caseSensitive", "true") \
        .getOrCreate()  
    
    # Read Parquet file into Spark DataFrame
    df_spark = spark.read.parquet(parquet_file_path)
    
    # Rename columns for clarity
    df_spark = df_spark.withColumnRenamed("T", "company_ticket") \
        .withColumnRenamed("v", "volume") \
        .withColumnRenamed("vw", "volume_weighted") \
        .withColumnRenamed("o", "open") \
        .withColumnRenamed("c", "close") \
        .withColumnRenamed("h", "high") \
        .withColumnRenamed("l", "low") \
        .withColumnRenamed("t", "time_stamp") \
        .withColumnRenamed("n", "num_of_trades") \
        .withColumnRenamed("otc", "is_otc")
    
    # Display schema and a few rows of data
    df_spark.printSchema()
    df_spark.show()
    
    # Convert PySpark DataFrame to Arrow Table
    arrow_table = pa.Table.from_pandas(df_spark.toPandas())

    # Get yesterday's date
    yesterday = datetime.now().date() - timedelta(days=1)
    print(f"Yesterday's date: {yesterday}")
    
    # Connect to DuckDB
    database_path = '/home/nvkhoa14/stock-data-engineering/datawarehouse.duckdb'
    conn = duckdb.connect(database=database_path)
    
    # Insert new time data into dim_time if it does not exist
    conn.execute(f'''
        INSERT INTO dim_time (date, day_of_week, month, quarter, year)
        SELECT
            '{yesterday}',
            '{yesterday.strftime("%A")}',
            '{yesterday.strftime("%B")}',
            '{((yesterday.month - 1) // 3) + 1}',
            {yesterday.year}
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_time WHERE date = '{yesterday}'
        )
    ''')
    
    # Get corresponding time_id from dim_time
    id_time_df = conn.execute(f'''
        SELECT time_id FROM dim_time WHERE date = '{yesterday}'
    ''').fetchdf()
    
    # Get corresponding company_id from dim_companies
    id_company_df = conn.execute(f'''
        SELECT company_id, company_ticket FROM dim_companies
    ''').fetchdf()
    print(id_company_df)
    
    # Create a new DataFrame containing company_id and company_ticket from dim_companies
    companies_df = id_company_df.drop_duplicates(subset=['company_ticket'], keep='last')
    print(companies_df)
    
    # Convert companies_df to PySpark DataFrame
    companies_spark_df = spark.createDataFrame(companies_df)
    
    # Join companies_spark_df to df_spark to get corresponding company_id
    df_spark = df_spark.join(companies_spark_df, on='company_ticket', how='left')
    df_spark = df_spark.filter(df_spark['company_id'].isNotNull())
    print(df_spark.show())
    
    # Add candles_time_id to DataFrame
    candles_time_id = id_time_df['time_id'][0]
    df_spark = df_spark.withColumn('candles_time_id', lit(candles_time_id))

    print(df_spark.show())
    
    # Convert PySpark DataFrame to Arrow Table
    arrow_table = pa.Table.from_pandas(df_spark.toPandas())
    
    # Register the Arrow Table in DuckDB
    conn.register("arrow_table", arrow_table)
    
    # Load DataFrame into fact_candles table
    conn.execute('''
        INSERT INTO fact_candles (
            candle_volume,
            candle_volume_weighted,
            candle_open,
            candle_close,
            candle_high,
            candle_low,
            candle_time_stamp,
            candle_num_of_trades,
            candle_is_otc,
            candles_time_id,
            candle_company_id
        ) SELECT
            volume,
            volume_weighted,
            open,
            close,
            high,
            low,
            time_stamp,
            num_of_trades,
            is_otc,
            candles_time_id,
            company_id
        FROM arrow_table
    ''')
    print("Data inserted into fact_candles successfully!")
    
    # Close DuckDB connection
    conn.close()
    
    # Stop Spark session
    spark.stop()

def transform_to_datawarehouse_2():
    hdfs_directory = "/user/nvkhoa14/datalake/ohlcs/"
    parquet_file_path = get_latest_parquet_file(hdfs_directory)
    # Process the Parquet file and insert data into DuckDB
    process(parquet_file_path)

transform_to_datawarehouse_2()