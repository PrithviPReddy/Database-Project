import pandas as pd
from sqlalchemy import create_engine
import time
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import numpy as np

DB_USER = 'root'
DB_PASS = '123456789'
DB_HOST = 'localhost'
DB_NAME = 'stock_warehouse'

# 1. Create the database connection engine
try:
    engine = create_engine(f'mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}')
    print("Database connection successful.")
except Exception as e:
    print(f"Error connecting to database: {e}")
    exit()

# 2. Define the stock to analyze

tickers_list = [
    'AAPL', 'MSFT', 'AMZN', 'GOOGL', 'NVDA', 'META', 'ORCL', 'CSCO', 'ADBE', 'INTC',
    'IBM', 'TXN', 'AMD', 'CRM', 'NOW', 'AVGO', 'QCOM', 'MU', 'SNPS', 'SHOP',
    'JPM', 'BAC', 'WFC', 'GS', 'MS', 'C', 'AXP', 'BLK', 'SPGI', 'SCHW',
    'PNC', 'TFC', 'USB', 'BK', 'COF', 'AIG', 'CB', 'ALL', 'MSCI', 'PAYX',
    'WMT', 'PG', 'KO', 'PEP', 'MCD', 'NKE', 'COST', 'SBUX', 'HD', 'TGT',
    'LOW', 'KHC', 'MDLZ', 'CL', 'EL', 'DG', 'YUM', 'KR', 'PM', 'MO',
    'JNJ', 'PFE', 'MRK', 'ABBV', 'UNH', 'ABT', 'BMY', 'CVS', 'LLY', 'AMGN',
    'GILD', 'TMO', 'DHR', 'SYK', 'MDT', 'ZTS', 'ISRG', 'REGN', 'VRTX', 'BDX',
    'XOM', 'CVX', 'CAT', 'GE', 'DE', 'UPS', 'FDX', 'BA', 'TSLA', 'GM',
    'HON', 'MMM', 'LMT', 'NOC', 'RTX', 'EMR', 'ETN', 'CLX', 'DOW', 'LIN',
    'VZ', 'T', 'CMCSA', 'DIS', 'NFLX', 'TMUS', 'ADP', 'INTU', 'PYPL', 'F'
]

while True:
    ticker_to_analyze = input("Please enter the stock name that you want to predict:  ").strip().upper()
    if ticker_to_analyze in tickers_list : break
    else: print("please enter a valid stock\n")


    
print(f"--- Starting Analysis for {ticker_to_analyze} ---")

# 3. Fetch data (same as before)
print(f"Choose the source table for {ticker_to_analyze}:")
print("1. stock_data_partitioned")
print("2. stock_data_baseline")

choice = int(input("Enter your choice (1 or 2): "))

match choice:
    case 1:
        print(f"Fetching data for {ticker_to_analyze} from 'stock_data_partitioned'...")
        query = f"SELECT `date`, `close` FROM stock_data_partitioned WHERE ticker = '{ticker_to_analyze}' ORDER BY `date` ASC"
    case 2:
        print(f"Fetching data for {ticker_to_analyze} from 'stock_data_baseline'...")
        query = f"SELECT `date`, `close` FROM stock_data_baseline WHERE ticker = '{ticker_to_analyze}' ORDER BY `date` ASC"
    case _:
        print("Invalid choice. Defaulting to 'stock_data_partitioned'.")
        query = f"SELECT `date`, `close` FROM stock_data_partitioned WHERE ticker = '{ticker_to_analyze}' ORDER BY `date` ASC"

start_time = time.time()
try:
    df = pd.read_sql(query, con=engine)
    end_time = time.time()
    print(f"Successfully fetched {len(df)} rows. Time taken: {end_time - start_time:.4f} seconds.")
    print("This proves the high-speed data access from our optimized warehouse.")
except Exception as e:
    print(f"Error fetching data: {e}")
    exit()

# --- 4. NEW: Feature Engineering (Preparing data for ML) ---
print("\n--- Starting ML Prediction Engine ---")
print("Preparing data for prediction model...")

# We will predict tomorrow's price using today's price.
df['X_today_close'] = df['close']
df['y_tomorrow_close'] = df['close'].shift(-1)

df.dropna(inplace=True)

# 5. Define our features (X) and target (y)
X = df[['X_today_close']]
y = df['y_tomorrow_close']

# 6. Split data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
print(f"Training model on {len(X_train)} data points...")

# 7. Train the Linear Regression Model
model = LinearRegression()
model.fit(X_train, y_train)

# 8. Evaluate the model (optional, but good for your report)
predictions = model.predict(X_test)
mse = mean_squared_error(y_test, predictions)
print(f"Model trained. Mean Squared Error: {mse:.4f}")

#  9. Make a Live Prediction 
print("\n--------------------------------- Prediction Result ---------------------------------")
last_known_price = df.iloc[-1]['X_today_close']
print(f"Last known closing price for {ticker_to_analyze} was: ${last_known_price:.2f}")
price_to_predict = pd.DataFrame([[last_known_price]], columns=['X_today_close'])
prediction = model.predict(price_to_predict)

print(f"** The model predicts the *next* closing price will be: ${prediction[0]:.2f} **")

