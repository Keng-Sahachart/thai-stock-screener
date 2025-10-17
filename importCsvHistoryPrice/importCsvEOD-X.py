
# import csv file to postgresql

# csv data example
# TICKER,DTYYYYMMDD,OPEN,HIGH,LOW,CLOSE,VOL
# SET,19750430,100,100,100,100,163310
# import to table stock_price_history
# symbol, date, open, high, low, close, volume

import pandas as pd
from datetime import datetime
import time


import psycopg2
from psycopg2 import sql
from psycopg2 import extras
import os
from dotenv import load_dotenv
load_dotenv()

# # Read CSV file into a pandas DataFrame
df = pd.read_csv('importCsvHistoryPrice\\PriceHistoryEOD1970-2024.csv',
    dtype={
        "TICKER": str,
        "DTYYYYMMDD": str,
        "OPEN": float,
        "HIGH": float,
        "LOW": float,
        "CLOSE": float,
        # "VOL": int
    }, na_values=['', 'NA']  # กำหนดค่า NA ให้เป็น NaN
    )

print(df.head())
df['date']  = pd.to_datetime(df['DTYYYYMMDD'], format='%Y%m%d').dt.date
df['VOL'] = pd.to_numeric(df['VOL'])
print(df.head())
###########################################################################
conn_str = (
    f"host={os.getenv('posql_host')} "
    f"port={os.getenv('posql_port')} "
    f"dbname={os.getenv('posql_db')} "
    f"user={os.getenv('posql_user')} "
    f"password={os.getenv('posql_password')}"
)

conn = psycopg2.connect(conn_str)
cursor = conn.cursor()

stratTime = time.time()
print(f"Start process at {stratTime}")

#***************************************************************************
# แบบที่ 1 ใช้ execute ทีละแถว
# Insert or update data into stock_price_history table
# for index, row in df.iterrows():
#     sql_insert = """
#     INSERT INTO stock_price_history (symbol, date, open, high, low, close, volume)
#     VALUES (%s, %s, %s, %s, %s, %s, %s)
#     ON CONFLICT (symbol, date) DO UPDATE SET
#     open = EXCLUDED.open,
#     high = EXCLUDED.high,
#     low = EXCLUDED.low,
#     close = EXCLUDED.close,
#     volume = EXCLUDED.volume;
#     """

#     params = (
#         str(row['TICKER']),
#         str(row['date']),      # แปลงเป็น string/date
#         float(row['OPEN']) if not pd.isna(row['OPEN']) else None,
#         float(row['HIGH']) if not pd.isna(row['HIGH']) else None,
#         float(row['LOW']) if not pd.isna(row['LOW']) else None,
#         float(row['CLOSE']) if not pd.isna(row['CLOSE']) else None,
#         int(row['VOL']) if not pd.isna(row['VOL']) else 0
#     )

#     cursor.execute(sql_insert,params)
#     if(index % 500 == 0):
#         conn.commit()
#     if(index % 100 == 0):    
#         percent = (index + 1) / len(df) * 100
#         timeRemain = (time.time() - stratTime) / (index + 1) * (len(df) - (index + 1))
#         print(f"Inserted/Updated row {index + 1}/{len(df)} ==> {percent:.2f}% , time remain {timeRemain/60:.2f} minutes")
#***************************************************************************       
# แบบที่ 2 ใช้ batch insert ด้วย execute_values
# Prepare data for batch insert
data_tuples = [
    (
        str(row['TICKER']),
        str(row['date']),   
        float(row['OPEN']) if not pd.isna(row['OPEN']) else None,
        float(row['HIGH']) if not pd.isna(row['HIGH']) else None,
        float(row['LOW']) if not pd.isna(row['LOW']) else None,
        float(row['CLOSE']) if not pd.isna(row['CLOSE']) else None,
        int(row['VOL']) if not pd.isna(row['VOL']) else 0
    )
    for index, row in df.iterrows()
]

# Use execute_values for batch insert
extras.execute_values(
    cursor,
    """
    INSERT INTO stock_price_history (symbol, date, open, high, low, close, volume)
    VALUES %s
    ON CONFLICT (symbol, date) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume;
    """,
    data_tuples
)


#***************************************************************************
# conn.commit()
#***************************************************************************

cursor.close()
conn.close()

endTime = time.time()
print(f"End process at {endTime} , using {endTime - stratTime} seconds")