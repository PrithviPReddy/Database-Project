import pandas as pd
from sqlalchemy import create_engine
import time

# --- CONFIGURE YOUR DATABASE CONNECTION HERE ---
# Format: 'mysql+mysqlconnector://<user>:<password>@<host>/<database>'
# Example: 'mysql+mysqlconnector://root:my_password@localhost/stock_warehouse'
DB_USER = 'root'
DB_PASS = '123456789'
DB_HOST = 'localhost'
DB_NAME = 'stock_warehouse'
# ---

# 1. Create the database connection engine
try:
    engine = create_engine(f'mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}')
    print("Database connection successful.")
except Exception as e:
    print(f"Error connecting to database: {e}")
    exit()

# 2. Read your downloaded CSV file
print("Reading CSV file 'all_stock_data_20_years.csv'...")
try:
    df = pd.read_csv('all_stock_data_20_years.csv')
    # Convert date column to datetime objects for SQL
    df['date'] = pd.to_datetime(df['date'])
    print(f"Successfully read {len(df)} rows from CSV.")
except FileNotFoundError:
    print("Error: 'all_stock_data_20_years.csv' not found.")
    exit()

# 3. Load data into all three tables
tables_to_load = {
    'stock_data_baseline': df,
    'stock_data_indexed': df,
    'stock_data_partitioned': df
}

for table_name, data in tables_to_load.items():
    print(f"Loading data into '{table_name}'...")
    start_time = time.time()
    try:
        # Use pandas.to_sql to efficiently bulk-load the data
        # 'if_exists='append'' adds the data.
        data.to_sql(table_name, con=engine, if_exists='append', index=False, chunksize=10000)
        
        end_time = time.time()
        print(f"Successfully loaded data into '{table_name}'. Time taken: {end_time - start_time:.2f} seconds.")
    except Exception as e:
        print(f"Error loading data into {table_name}: {e}")

print("\nAll data loading complete.")
