import duckdb
import os

# Connect to your DuckDB database
con = duckdb.connect(r"flight_analysis\flight_dev.duckdb")

# Path to your raw data folder
raw_data_path = r"raw_data"

# Get all CSV files in the raw_data folder
csv_files = [f for f in os.listdir(raw_data_path) if f.endswith(".csv")]
print(f"Found {len(csv_files)} CSV files: {csv_files}")

# Load all CSVs into a single table
con.execute("""
    CREATE OR REPLACE TABLE raw_flights AS
    SELECT * FROM read_csv_auto('raw_data/*.csv', union_by_name=true)
""")

# Quick check
count = con.execute("SELECT COUNT(*) FROM raw_flights").fetchone()[0]
print(f"Successfully loaded {count:,} rows into raw_flights!")

con.close()