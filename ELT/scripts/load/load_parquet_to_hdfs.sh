ohlcs_directory=/home/nvkhoa14/stock-data-engineering/ELT/data/.parquet/ohlcs_to_dl
news_directory=/home/nvkhoa14/stock-data-engineering/ELT/data/.parquet/news_to_dl
companies_directory=/home/nvkhoa14/stock-data-engineering/ELT/data/.parquet/db_to_dl

hdfs_directory=/user/nvkhoa14/datalake

# Find the latest Parquet file
latest_file=$(ls -t "$ohlcs_directory"/*.parquet | head -1)

# Check if a file is found
if [ -z "$latest_file" ]; then
    echo "No Parquet file found in the directory $ohlcs_directory."
else
    # Upload the file to HDFS
    hdfs dfs -put "$latest_file" "$hdfs_directory"/ohlcs/

    # Append the file name and HDFS directory to the variable
    latest_files="$latest_files$hdfs_directory/ohlcs/$(basename $latest_file)\n"
fi

# Repeat the same process for the news directory
latest_file=$(ls -t "$news_directory"/*.parquet | head -1)
if [ -z "$latest_file" ]; then
    echo "No Parquet file found in the directory $news_directory."
else
    hdfs dfs -put "$latest_file" "$hdfs_directory"/news/
    latest_files="$latest_files$hdfs_directory/news/$(basename $latest_file)\n"
fi

# Repeat the same process for the companies directory
latest_file=$(ls -t "$companies_directory"/*.parquet | head -1)

if [ -z "$latest_file" ]; then
    echo "No Parquet file found in the directory $companies_directory."
else
    hdfs dfs -put "$latest_file" "$hdfs_directory"/companies/
    latest_files="$latest_files$hdfs_directory/companies/$(basename $latest_file)\n"
fi
# Print the latest files that were uploaded to HDFS
echo -e "$latest_files"