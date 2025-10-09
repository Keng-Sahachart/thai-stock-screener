

import requests
import pyodbc
import initialApp as cfg
import os
from dotenv import load_dotenv
load_dotenv()


def getStocklistFromTwelveData():
    r = requests.get("https://api.twelvedata.com/stocks?exchange=XBKK")
    stocks = r.json()["data"]
    # Example of a stock entry:
    #   {
    #       "symbol": "24CS",
    #       "name": "24 Construction and Supply Public Company Limited",
    #       "currency": "THB",
    #       "exchange": "SET",
    #       "mic_code": "XBKK",
    #       "country": "Thailand",
    #       "type": "Common Stock",
    #       "figi_code": "BBG018QNLZ74",
    #       "cfi_code": "ESVTFR",
    #       "isin": "request_access_via_add_ons",
    #       "cusip": "request_access_via_add_ons"
    #     }

    conn = pyodbc.connect(cfg.postgresqldb_args)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM stocklist_twelvedata")
    for stock in stocks:
        symbol = stock["symbol"]
        name = stock["name"].replace("'", "''")
        currency = stock["currency"]
        exchange = stock["exchange"]
        mic_code = stock["mic_code"]
        country = stock["country"]
        type_ = stock["type"]


        sqlInsert = f"""INSERT INTO stocklist_twelvedata (symbol, name, exchange, currency, mic_code, country, type_) 
                        VALUES ('{symbol}', '{name}', '{exchange}', '{currency}', '{mic_code}', '{country}', '{type_}')"""

        cursor.execute(sqlInsert)


    conn.commit()
    cursor.close()
    conn.close()
    print(f"Inserted {len(stocks)} records into stocklist_twelvedata")

def createTableIfNotExists():
    conn = pyodbc.connect(cfg.postgresqldb_args)
    cursor = conn.cursor()
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='stocklist_twelvedata' AND xtype='U')
        CREATE TABLE stocklist_twelvedata (
            id INT IDENTITY(1,1) PRIMARY KEY,
            symbol VARCHAR(20),
            name VARCHAR(255),
            exchange VARCHAR(50),
            currency VARCHAR(10),
            mic_code VARCHAR(20),
            country VARCHAR(50),
            type_ VARCHAR(50)
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()


createTableIfNotExists()
getStocklistFromTwelveData()