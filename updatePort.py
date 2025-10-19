from settrade_v2 import Investor
import pandas as pd
# import math
from datetime import datetime
import sys
import os
import pyodbc

from PyN_Library import fncPostgres as fpg


import initialApp as cfg
from dotenv import load_dotenv
load_dotenv()

investor = Investor( **cfg.args_Investor )
equity = investor.Equity(account_no=os.getenv("account_no"))

dfPortfolioList = pd.DataFrame()


portfolio = equity.get_portfolios()
dfPortfolioList  = pd.DataFrame(portfolio['portfolioList'])
# print(dfPortfolioList.head())

# add account_no column
dfPortfolioList['account_no'] = os.getenv("account_no")
#move account_no column to first column
cols = dfPortfolioList.columns.tolist()
cols = [cols[-1]] + cols[:-1]
dfPortfolioList = dfPortfolioList[cols]

# add imported_at column
nowImport = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
dfPortfolioList['imported_at'] = nowImport
# move imported_at column to 2nd column
cols = dfPortfolioList.columns.tolist()
cols = [cols[0]] + [cols[-1]] + cols[1:-1]
dfPortfolioList = dfPortfolioList[cols]

# #print first 5 rows
# print(dfPortfolioList.head())

#print columns
# print(dfPortfolioList.columns)

#create table if not exists
conn_str = (
    f"DRIVER={{PostgreSQL Unicode}};"
    f"SERVER={os.getenv('posql_host')};"
    f"PORT={os.getenv('posql_port')};"
    f"DATABASE={os.getenv('posql_db')};"
    f"UID={os.getenv('posql_user')};"
    f"PWD={os.getenv('posql_password')};"
)
conn = pyodbc.connect(conn_str)
# columns:
# 'symbol', 'flag', 'nvdrFlag', 'marketPrice', 'amount',
#  'marketdescription', 'marketValue', 'profit', 'percentProfit',
#  'realizeProfit', 'startVolume', 'currentVolume', 'actualVolume',
#  'startPrice', 'averagePrice', 'showNA', 'portFlag', 'marginRate',
#  'liabilities', 'commissionRate', 'vatRate', 'imported_at'

# CREATE TABLE IF NOT EXISTS portfolio_stock (     symbol VARCHAR(255),     flag VARCHAR(255),     nvdrFlag VARCHAR(255),     marketPrice REAL,     amount REAL
# ,     marketdescription VARCHAR(255),     marketValue REAL,     profit REAL,     percentProfit REAL,     realizeProfit REAL,     startVolume INTEGER,     currentVolume INTEGER
# ,     actualVolume INTEGER,     startPrice REAL,     averagePrice REAL,     showNA BOOLEAN,     portFlag VARCHAR(255),     marginRate REAL,     liabilities INTEGER
# ,     commissionRate REAL,     vatRate REAL,     account_no VARCHAR(255),     imported_at VARCHAR(255) );
rename_map_cols = {
    'symbol': 'symbol', # สัญลักษณ์หุ้น
    'flag': 'flag',
    'nvdrFlag': 'nvdr_flag', 
    'marketPrice': 'market_price', # ราคาตลาด ต่อหน่วย
    'amount': 'amount', # มูลค่าตลาดรวม ณ เวลาที่ซื้อ 
    'marketdescription': 'market_description',
    'marketValue': 'market_value',  # มูลค่าตลาดรวม ณ เวลานั้น
    'profit': 'profit', # กำไร/ขาดทุน ณ เวลานั้น
    'percentProfit': 'percent_profit', # กำไร/ขาดทุน เป็น % ณ เวลานั้น
    'realizeProfit': 'realize_profit',
    'startVolume': 'start_volume',
    'currentVolume': 'current_volume', # ปริมาณหุ้นปัจจุบัน
    'actualVolume': 'actual_volume',
    'startPrice': 'start_price',
    'averagePrice': 'average_price', # ราคาทุนเฉลี่ย/ต่อหน่วย
    'showNA': 'show_na',
    'portFlag': 'port_flag',
    'marginRate': 'margin_rate',
    'liabilities': 'liabilities',
    'commissionRate': 'commission_rate',
    'vatRate': 'vat_rate',
    'account_no': 'account_no', # หมายเลขบัญชี
    'imported_at': 'imported_at' # เวลาที่นำเข้าข้อมูล
}
dfPortfolioList.rename(columns=rename_map_cols, inplace=True)

sqlCreateTable = fpg.generate_create_table_script(df=dfPortfolioList,table_name="portfolio_stock",use_index=False)
# print(sqlCreateTable)

cursor = conn.cursor()
cursor.execute(sqlCreateTable)
conn.commit()

#bulk insert dataframe to table
fpg.bulk_copy_dataframe_to_table(df=dfPortfolioList, table_name="portfolio_stock", conn_params=cfg.postgresqldb_args)