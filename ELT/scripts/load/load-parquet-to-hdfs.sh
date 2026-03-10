local_directory=/home/nvkhoa14/stock-data-engineering/ELT/data/.parquet/ohcls_to_dl
hdfs_directory=/user/nvkhoa14/datalake/ohlcs

# Find the latest Parquet file
latest_file=$(ls -t "$local_directory"/*.parquet | head -1)

# Check if a file is found
if [ -z "$latest_file" ]; then
    echo "No Parquet file found in the directory $local_directory."
else
    # Upload the file to HDFS
    hdfs dfs -put "$latest_file" "$hdfs_directory"

    # Append the file name and HDFS directory to the variable
    latest_files="$latest_files$hdfs_directory/$(basename $latest_file)\n"
fi

# Print the latest file names (for Airflow XCom)
echo -e "$latest_files"
