import pandas as pd
from datetime import datetime,timedelta
import yfinance as yf 
import os
import logging

# Paths
BASE_DIR =  r'C:\Users\ankit\Desktop\Automated-NIFTY-50-Market-Performance-Analytics-Dashboard'
MASTER_FILE = os.path.join(BASE_DIR, "data","master",'NIFTY50_Master.xlsx')
RAW_DATA_PATH = os.path.join(BASE_DIR,'data','raw')
OUTPUT_FILE = os.path.join(RAW_DATA_PATH, 'nifty50_daily_raw.xlsx')
LOG_DIR = os.path.join(BASE_DIR, 'logs')

os.makedirs(RAW_DATA_PATH, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# Logging
logging.basicConfig(
    filename=os.path.join(LOG_DIR,'nifty50_pipeline.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'

)
logging.info('NIFTY50 pipeline started')


try:
  # Load master data 
    master_df = pd.read_excel(MASTER_FILE)
    symbols = master_df['Symbol'].dropna().unique().tolist()
    existing_df = pd.DataFrame()

    if os.path.exists(OUTPUT_FILE):
        existing_df = pd.read_excel(OUTPUT_FILE)
        last_date = pd.to_datetime(existing_df['Date']).max()
        START_DATE = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
        logging.info(f'Last date in Excel: {last_date}')
        logging.info(f'Start date {START_DATE}')
    else:
        START_DATE = '2021-01-01'
        logging.info('Full Historical load')
    
    END_DATE = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    logging.info(f'End date is {END_DATE}')
        
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
         logging.info('No data for {symbol}')
         continue

       df.reset_index(inplace=True)
       df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
       df['Symbol'] = symbol
       all_data.append(df)

# Combine & Save

    if all_data:
     new_data = pd.concat(all_data, ignore_index=True)

     final_df = (
        pd.concat([existing_df, new_data], ignore_index=True)
        if not existing_df.empty
        else new_data
     )

     final_df.drop_duplicates(subset=['Date','Symbol'],inplace = True)

     final_df.to_excel(OUTPUT_FILE, sheet_name='NIFTY50',index=False)
     logging.info('Nifty50 data update successfully')
    else:
       print('No data fetched')


except Exception as e:
 logging.error(f'pipeline failed: {e}')

logging.info('Nifty50 pipeline completed')
