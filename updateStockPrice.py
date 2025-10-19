# อัพเดต ราคาหุ้น จาก settrade_v2 โดยใช้ symbol list จาก postgresql table stocksettrade_stocklist
# แล้วอัพเดตลง postgresql table stock_price_history โดย insert เมื่อเป็นข้อมูลใหม่ ,merge เมื่อมีข้อมูล symbol+date ซ้ำ
# สร้างตาราง stock_price_history ด้วยตนเองก่อนรันสคริปต์นี้ ถ้ายยังไม่มี
 


import pyodbc
import initialApp as cfg
from dotenv import load_dotenv
load_dotenv()
import os
from settrade_v2 import Investor
import pandas as pd
from datetime import datetime, timedelta
import time
import sys

from PyN_Library import fncDateTime as fDtTm

def main():

    investor = Investor( **cfg.args_Investor )
    equity = investor.Equity(account_no=os.getenv("account_no"))
    market = investor.MarketData()
    # print(market)

    # date = datetime.now().strftime("%Y-%m-%d")
    startDate = datetime.now().strftime("%Y-%m-%d")
    endDate = datetime.now().strftime("%Y-%m-%d")
    # print(date)

    conn_str = (
        f"DRIVER={{PostgreSQL Unicode}};"
        f"SERVER={os.getenv('posql_host')};"
        f"PORT={os.getenv('posql_port')};"
        f"DATABASE={os.getenv('posql_db')};"
        f"UID={os.getenv('posql_user')};"
        f"PWD={os.getenv('posql_password')};"
    )
    # print(conn_str)
    # sys.exit()
    conn = pyodbc.connect(conn_str)


    sqlCreateTable = """
    CREATE TABLE IF NOT EXISTS stock_price_history (
        symbol VARCHAR(20) NOT NULL,
        date DATE NOT NULL, 
        open NUMERIC(18,6),
        high NUMERIC(18,6),
        low NUMERIC(18,6),
        close NUMERIC(18,6),
        volume BIGINT,
        PRIMARY KEY (symbol, date)
    );
    """
    cursor = conn.cursor()
    cursor.execute(sqlCreateTable)
    conn.commit()



    # cursor = conn.cursor()
    cursor.execute("SELECT symbol FROM settrade_stocklist  ORDER BY symbol ;")
    symbols = [row[0] for row in cursor.fetchall()]

    for symbol in symbols:
        print(f"Processing symbol: {symbol}")
        start_time = time.time()
        try:
            candles = market.get_candlestick(
                symbol=symbol,
                interval="1d",
                limit=1000,
                normalized=True,
                start=f"{startDate}T00:00",
                end=f"{endDate}T23:59",
            )
            if not candles:
                print(f"No data returned for symbol: {symbol}")
                continue

            df = pd.DataFrame(candles)
            # df.to_csv("debug_candles.csv", index=False)  # Debug: Save to CSV to inspect

            df['date'] = (pd.to_datetime(df['time'], unit='s')+ timedelta(hours=7)).dt.date # แปลง timestamp เป็น date
            # แปลงคอลัมน์ที่เป็นตัวเลขให้เป็นชนิดตัวเลข
            df['open'] = pd.to_numeric(df['open'])
            df['high'] = pd.to_numeric(df['high'])
            df['low'] = pd.to_numeric(df['low'])
            df['close'] = pd.to_numeric(df['close'])
            df['volume'] = pd.to_numeric(df['volume'])

            for index, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO stock_price_history (symbol, date, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (symbol, date) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume;
                """, symbol, row['date'], row['open'], row['high'], row['low'], row['close'], row['volume'])
            conn.commit()
            print(f"process in {time.time() - start_time} second , Updated data for symbol: {symbol}")
        except Exception as e:
            print(f"Error processing symbol {symbol}: {e}")
        time.sleep(1)  # เพื่อหลีกเลี่ยงการเรียก API เร็วเกินไป
    cursor.close()
    conn.close()
    print("Stock price update completed.")

if __name__ == "__main__":
    main()