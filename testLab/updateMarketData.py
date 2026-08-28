from settrade_v2 import Investor
from settrade_v2 import MarketRep

import pandas as pd
# import math
from datetime import datetime
import sys
import os
# import pyodbc
import initialApp as cfg
from dotenv import load_dotenv
load_dotenv()

investor = Investor( **cfg.args_Investor )
# # กรณี Market Representative
# marketrep  = MarketRep.MarketData(
#             app_id=os.getenv("app_id"),
#             app_secret=os.getenv("app_secret"),
#             broker_id="023",
#             app_code="ALGO_EQ",
#             is_auto_queue = False
#             )

# deri = investor.Derivatives(account_no=os.getenv("account_no"))  
# account_info = deri.get_account_info()
# print(account_info)


equity = investor.Equity(account_no=os.getenv("account_no"))
account_info = equity.get_account_info()
print(account_info)

# equityTrep = marketrep.Equity()
# accountinfoTrep= equityTrep.get_account_info(account_no=os.getenv("account_no"))
# print(accountinfoTrep)


# กรณี Investor
market = investor.MarketData()

# res = market.get_candlestick(
# symbol="BBL",
# interval="1d",
# limit=1,
# normalized=True,
# )
candles1 = market.get_candlestick(
                symbol="NOBLE",
                interval="1d",
                limit=30,
                start= "2026-08-25T00:00" ,  # "YYYY-mm-ddTHH:MM"
                end = "2026-08-25T23:59" ,
                normalized=True,
            )   

print(candles1)

# info = equity.get_symbol_info("PTT")
# print(info)