import pandas as pd
import numpy as np 
import yfinance as yf 
import os

MASTER_FILE = '../../data/master/NIFTY50_Master.xlsx'

master_df = pd.read_excel(MASTER_FILE)

print(master_df.head())