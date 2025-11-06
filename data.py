import yfinance as yf
import pandas as pd

# 1️⃣ Define the stock tickers


tickers = [
    # Technology
    'AAPL', 'MSFT', 'AMZN', 'GOOGL', 'NVDA', 'META', 'ORCL', 'CSCO', 'ADBE', 'INTC',
    'IBM', 'TXN', 'AMD', 'CRM', 'NOW', 'AVGO', 'QCOM', 'MU', 'SNPS', 'SHOP',

    # Finance
    'JPM', 'BAC', 'WFC', 'GS', 'MS', 'C', 'AXP', 'BLK', 'SPGI', 'SCHW',
    'PNC', 'TFC', 'USB', 'BK', 'COF', 'AIG', 'CB', 'ALL', 'MSCI', 'PAYX',

    # Consumer
    'WMT', 'PG', 'KO', 'PEP', 'MCD', 'NKE', 'COST', 'SBUX', 'HD', 'TGT',
    'LOW', 'KHC', 'MDLZ', 'CL', 'EL', 'DG', 'YUM', 'KR', 'PM', 'MO',

    # Healthcare
    'JNJ', 'PFE', 'MRK', 'ABBV', 'UNH', 'ABT', 'BMY', 'CVS', 'LLY', 'AMGN',
    'GILD', 'TMO', 'DHR', 'SYK', 'MDT', 'ZTS', 'ISRG', 'REGN', 'VRTX', 'BDX',

    # Energy / Industrials / Materials
    'XOM', 'CVX', 'CAT', 'GE', 'DE', 'UPS', 'FDX', 'BA', 'TSLA', 'GM',
    'HON', 'MMM', 'LMT', 'NOC', 'RTX', 'EMR', 'ETN', 'CLX', 'DOW', 'LIN',

    # Telecom / Utilities / Others
    'VZ', 'T', 'CMCSA', 'DIS', 'NFLX', 'TMUS', 'ADP', 'INTU', 'PYPL', 'F'
]



# 2 Define the date range (20 years)
start_date = '2005-01-01'
end_date = '2024-12-31'

#  3 Prepare a list to store all data
all_data = []

# 4 Download and clean each ticker's data
for ticker in tickers:
    try:
        print(f"Downloading data for {ticker}...")
        data = yf.download(ticker, start=start_date, end=end_date, auto_adjust=False)
        data.reset_index(inplace=True)

        data.columns = [col[0] if isinstance(col, tuple) else col for col in data.columns]

        required_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
        available_columns = [c for c in required_columns if c in data.columns]

        if len(available_columns) == len(required_columns):
            data_clean = data[available_columns].copy()
            data_clean['Ticker'] = ticker
            all_data.append(data_clean)
        else:
            missing = set(required_columns) - set(data.columns)
            print(f" Skipping {ticker}: Missing columns -> {missing}")

    except Exception as e:
        print(f" Error downloading {ticker}: {e}")

if all_data:
    combined_data = pd.concat(all_data, ignore_index=True)

    combined_data.columns = [
        "_".join(col).strip() if isinstance(col, tuple) else col
        for col in combined_data.columns
    ]

    print("\nColumns after flattening:", len(combined_data.columns))
    print("Columns:", combined_data.columns.tolist())

    col_map = {
        'Date': 'date',
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Close': 'close',
        'Adj Close': 'adj_close',
        'Volume': 'volume',
        'Ticker': 'ticker'
    }

    existing_cols = [c for c in col_map.keys() if c in combined_data.columns]
    combined_data = combined_data[existing_cols]
    combined_data.rename(columns=col_map, inplace=True)

    combined_data.to_csv('all_stock_data_20_years.csv', index=False)
    print("\nAll data downloaded and saved to 'all_stock_data_20_years.csv'")

else:
    print("\No valid data was downloaded. Please check ticker names or connection.")
