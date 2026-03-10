import pandas as pd
import datetime
import pathlib as Path
from sqlalchemy import create_engine

# Read SQL query from a file
def read_query_from_file(file_path):
    # Open and read the content of the SQL file
    with open(file_path, 'r') as file:
        query = file.read()
    return query

# Execute the query and save the result to a Parquet file
def query_to_parquet(query, conn, parquet_file_path):
    # Execute the query and fetch the result into a DataFrame
    df = pd.read_sql(query, conn)
    print(df['company_name'].unique())
    
    # Save the DataFrame to a Parquet file
    df.to_parquet(parquet_file_path, engine='pyarrow')
    
def load_db_to_parquet():
    # Database connection parameters
    username = 'nvkhoa14'
    password = 'secret'
    host = 'localhost'
    port = '5432'
    database = 'datasource'

    # Create an engine to connect to PostgreSQL
    engine = create_engine(f"postgresql://{username}:{password}@{host}:{port}/{database}")
    # Path to the SQL query file
    home = Path.Path(__file__).parent.parent.parent
    query_file_path = home / "scripts" / "extract" / "extract-db.sql"

    # Path to the output Parquet file
    date = datetime.date.today().strftime("%Y_%m_%d")
    parquet_file_path = home / "data" / ".parquet" / "db_to_dl"
    parquet_file_path.mkdir(parents=True, exist_ok=True)
    parquet_file_path = parquet_file_path / f"db_to_dl_{date}.parquet"

    # Read the SQL query from the file
    query = read_query_from_file(query_file_path)

    # Execute the query and save the result to a Parquet file
    query_to_parquet(query, engine, parquet_file_path)
    print(f"Saved data from database to parquet successfully at {parquet_file_path}")
    
load_db_to_parquet()