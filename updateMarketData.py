from settrade_v2 import Investor
import pandas as pd
# import math
from datetime import datetime
import sys
import os
import pyodbc
import initialApp as cfg
from dotenv import load_dotenv
load_dotenv()

investor = Investor( **cfg.args_Investor )
equity = investor.Equity(account_no=os.getenv("account_no"))

market = investor.MarketData()

candles1 = market.get_candlestick(
                symbol="NOBLE",
                interval="1d",
                limit=30,
                start= "2025-10-01T00:00" ,  # "YYYY-mm-ddTHH:MM"
                end = "2025-10-07T23:59" ,
                normalized=True,
            )   

print(candles1)

info = equity.get_symbol_info("PTT")
print(info)