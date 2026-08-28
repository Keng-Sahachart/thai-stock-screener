# อัพเดต ราคาหุ้น จาก settrade_v2 โดยใช้ symbol list จาก postgresql table stocksettrade_stocklist
# แล้วอัพเดตลง postgresql table stock_price_history โดย insert เมื่อเป็นข้อมูลใหม่ ,merge เมื่อมีข้อมูล symbol+date ซ้ำ
# สร้างตาราง stock_price_history ด้วยตนเองก่อนรันสคริปต์นี้ ถ้ายยังไม่มี
 

import random

import psycopg2
import pyodbc
import initialApp as cfg
from dotenv import load_dotenv
load_dotenv()
import os
from settrade_v2 import Investor
import pandas as pd
from datetime import date, datetime, timedelta
import time
import sys

from PyN_Library import fncDateTime as fDtTm

def get_last_stock_date(conn, symbol):
    """หาวันล่าสุดที่มีข้อมูลในตาราง stock_price_history ของหุ้นตัวนั้นๆ"""
    with conn.cursor() as cursor:
        query = "SELECT MAX(date) FROM stock_price_history WHERE symbol = %s;"
        cursor.execute(query, (symbol,))
        row = cursor.fetchone()
        if row and row[0]:
            # ถ้าเจอวันล่าสุด ให้เริ่มวันถัดไป (+1 day)
            return row[0] + timedelta(days=1)
        else:
            # ถ้าไม่เจอข้อมูลเลย ให้ย้อนหลังไป 12 เดือน
            return date.today() - timedelta(days=365)
        
def main():
    investor = Investor( **cfg.args_Investor )
    equity = investor.Equity(account_no=os.getenv("account_no"))
    market = investor.MarketData()
    # print(market)

    # date = datetime.now().strftime("%Y-%m-%d")
    # startDate = datetime.now().strftime("%Y-%m-%d") #"2026-03-31" # 
    endDate = datetime.now().strftime("%Y-%m-%d")
    # print(date)

    # เชื่อมต่อ PostgreSQL โดยตรงผ่าน psycopg2
    conn = psycopg2.connect(
        host=os.getenv("posql_host"),
        port=os.getenv("posql_port", "5432"),
        dbname=os.getenv("posql_db"),
        user=os.getenv("posql_user"),
        password=os.getenv("posql_password")
    )
    # print(conn_str)
    # sys.exit()


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

        #หาวันเริ่มของแต่ละตัว ---
        start_dt_obj = get_last_stock_date(conn, symbol)
        # ถ้าวันล่าสุดที่หาได้ คือวันนี้หรืออนาคต (กรณีข้อมูลอัพเดตแล้ว) ให้ข้าม
        if start_dt_obj >= date.today():
            print(f"Skipping {symbol}: Already up to date.")
            continue
        startDate = start_dt_obj.strftime("%Y-%m-%d")
        print(f"Processing {symbol}: Start from {startDate} to {endDate}")

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
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, date) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume;
                """, (symbol, row['date'], row['open'], row['high'], row['low'], row['close'], row['volume']))
            conn.commit()
            print(f"process in {time.time() - start_time} second , Updated data for symbol: {symbol}")
        except Exception as e:
            print(f"Error processing symbol {symbol}: {e}")
        delay = random.uniform(2, 5)  # Random delay between 2 to 5 seconds
        print(f"Sleeping for {delay:.2f} seconds to avoid hitting API rate limits.")
        time.sleep(delay)  # เพื่อหลีกเลี่ยงการเรียก API เร็วเกินไป
    cursor.close()
    conn.close()
    print("Stock price update completed.")

if __name__ == "__main__":
    main()