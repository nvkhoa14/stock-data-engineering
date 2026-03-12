import duckdb
import os

# path to DuckDB database file
database_path = '/home/nvkhoa14/stock-data-engineering/datawarehouse.duckdb'

# Remove the existing database file if it exists to ensure a fresh start
if os.path.exists(database_path):
    os.remove(database_path)
# Connect to DuckDB (this will create a new database file if it doesn't exist)
conn = duckdb.connect(database=database_path)

# Read SQL script from file
with open('/home/nvkhoa14/stock-data-engineering/SQL/dw_config/dw.sql', 'r') as file:
    sql_script = file.read()

# Run the SQL commands from the file
conn.execute(sql_script)

# Close the connection
conn.close()

print(f"Database has been created and saved to {database_path}")