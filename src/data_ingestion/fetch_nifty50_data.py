import pandas as pd
from datetime import datetime
import yfinance as yf 
import os

# Paths
MASTER_FILE = '../../data/master/NIFTY50_Master.xlsx'
RAW_DATA_PATH = '../../data/raw/'
OUTPUT_FILE = os.path.join(RAW_DATA_PATH, 'nifty50_daily_raw.xlsx')

# Load master data 
master_df = pd.read_excel(MASTER_FILE)
print(master_df.head())

symbols = master_df['Symbol'].dropna().unique().tolist()
print(symbols)

# Date range
START_DATE = '2021-01-01'
END_DATE = datetime.today().strftime("%Y-%m-%d")

# Fetch data
all_data = []

for symbol in symbols:
    print(f'fetching data for {symbol}')

    df = yf.download(
        symbol,
        start=START_DATE,
        end=END_DATE,
        progress=False
    )

    if df.empty:
        print(f'No data for {symbol}')
        continue

    df.reset_index(inplace=True)
    df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
    df['symbol'] = symbol

    all_data.append(df)

# Combine & Save

if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    final_df.to_excel(OUTPUT_FILE,sheet_name = 'NIFTY50', index=False)
    print('Nifty 50 daily raw data saved successfully')
else:
    print('No data fetched')