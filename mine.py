import pandas as pd
from sqlalchemy import create_engine
import time

# --- CONFIGURE YOUR DATABASE CONNECTION HERE ---
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

# 2. Define the stock to analyze
ticker_to_analyze = 'MSFT'
print(f"Starting analysis for ticker: {ticker_to_analyze}...")

# 3. Define the function to fetch data
def fetch_stock_data(ticker):
    print(f"Fetching data for {ticker} from 'stock_data_partitioned'...")
    start_time = time.time()
    
    # This query uses our fast, partitioned table
    query = f"SELECT `date`, `close` FROM stock_data_partitioned WHERE ticker = '{ticker}' ORDER BY `date` ASC"
    
    try:
        # Read data directly into a pandas DataFrame
        df = pd.read_sql(query, con=engine)
        end_time = time.time()
        
        print(f"Successfully fetched {len(df)} rows. Time taken: {end_time - start_time:.4f} seconds.")
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

# 4. Define the data mining function (Calculate SMA)
def calculate_moving_averages(df):
    if df is None or df.empty:
        return None
        
    print("Calculating 20-day and 50-day Simple Moving Averages (SMA)...")
    # Set 'date' as the index for time-series calculations
    df.set_index('date', inplace=True)
    
    # Calculate SMAs
    df['sma_20'] = df['close'].rolling(window=20).mean()
    df['sma_50'] = df['close'].rolling(window=50).mean()
    
    # Add the ticker back in
    df['ticker'] = ticker_to_analyze
    
    # Reset the index so 'date' becomes a column again
    df.reset_index(inplace=True)
    
    print("Pattern calculation complete.")
    return df

# 5. Define function to save patterns
def save_patterns(df):
    if df is None:
        return
        
    # We only want the new patterns, not the raw 'close' price
    patterns_df = df[['date', 'ticker', 'sma_20', 'sma_50']].copy()
    
    # Drop rows where SMA is not calculated (the first 49 rows)
    patterns_df.dropna(inplace=True)
    
    print(f"Saving {len(patterns_df)} calculated patterns to 'patterns_warehouse'...")
    try:
        # 'if_exists='replace'' will create the table or overwrite it.
        # This is good for testing.
        patterns_df.to_sql('patterns_warehouse', con=engine, if_exists='replace', index=False)
        print("Successfully saved patterns to 'patterns_warehouse' table.")
    except Exception as e:
        print(f"Error saving patterns: {e}")

# --- Main execution ---
stock_df = fetch_stock_data(ticker_to_analyze)
patterns_df = calculate_moving_averages(stock_df)
save_patterns(patterns_df)

print("\nData mining script finished.")
