

import requests
import pyodbc
import initialApp as cfg
import os
from dotenv import load_dotenv
load_dotenv()

from PyN_Library import testFnc as tf

conn_str = (
    f"DRIVER={{PostgreSQL Unicode}};"
    f"SERVER={cfg.postgresqldb_args['host']};"
    f"PORT={cfg.postgresqldb_args['port']};"
    f"DATABASE={cfg.postgresqldb_args['database']};"
    f"UID={cfg.postgresqldb_args['user']};"
    f"PWD={cfg.postgresqldb_args['password']};"
)
# conn_str = (
# "DRIVER={PostgreSQL Unicode};"
# "SERVER=192.168.1.124;"
# "PORT=5432;"
# "DATABASE=stockEquity;"
# "UID=postgres;"
# "PWD=P@ssw0rd;"
# )


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

    conn = pyodbc.connect(conn_str)
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



    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stocklist_twelvedata (
        id SERIAL PRIMARY KEY,
        symbol VARCHAR(20) UNIQUE,
        name TEXT,
        exchange VARCHAR(10),
        currency VARCHAR(10),
        mic_code VARCHAR(10),
        country VARCHAR(50),
        type_ VARCHAR(50)
    );
    """)
    conn.commit()
    cursor.close()
    conn.close()


createTableIfNotExists()
getStocklistFromTwelveData()