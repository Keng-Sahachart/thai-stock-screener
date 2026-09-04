'''
เอาไว้ดึงข้อมูล Master Stock Classification ของหุ้นไทยจาก Yahoo Finance (yfinance) และบันทึกลงฐานข้อมูล PostgreSQL
- ดึงข้อมูลจากตาราง settrade_stocklist และ stock_list_info_siamchart
'''
import os
import time
import urllib.parse
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
import yfinance as yf
import random
load_dotenv()


def get_db_engine():
    user = os.getenv("posql_user")
    password = urllib.parse.quote_plus(os.getenv("posql_password", ""))
    host = os.getenv("posql_host")
    port = os.getenv("posql_port", "5432")
    dbname = os.getenv("posql_db")

    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(conn_str)


def init_database_tables(engine):
    """สร้างตาราง Master Stock Classification หากยังไม่มี"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS public.master_stock_classification (
        symbol VARCHAR(50) PRIMARY KEY,
        short_name VARCHAR(255),
        quote_type VARCHAR(50),      -- EQUITY, ETF, MUTUALFUND
        sector VARCHAR(100),
        industry VARCHAR(150),
        market_cap NUMERIC,
        currency VARCHAR(10),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_table_sql))
    print("✅ ตรวจสอบ/สร้างตาราง master_stock_classification เรียบร้อยแล้ว")


def get_target_symbols(engine) -> list[str]:
    """ดึงรายชื่อหุ้นทั้งหมดที่มีในฐานข้อมูล (จาก portfolio และ price history)"""
    query = """
    SELECT DISTINCT symbol FROM (
        SELECT symbol FROM public.settrade_stocklist
        UNION
        SELECT name as symbol FROM public.stock_list_info_siamchart
    ) AS all_symbols
    WHERE symbol IS NOT NULL AND symbol != ''
    ORDER BY symbol;
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
    return df["symbol"].tolist()


def fetch_stock_info(symbol: str) -> dict | None:
    """ยิง yfinance เพื่อดึง Metadata รายตัว (รองรับหุ้นไทย .BK)"""
    # หุ้นไทยใน yfinance ต้องต่อท้ายด้วย .BK เช่น PTT.BK, TDEX.BK
    ticker_symbol = symbol if symbol.endswith(".BK") else f"{symbol}.BK"

    try:
        ticker = yf.Ticker(ticker_symbol)
        info = ticker.info

        # ตรวจสอบว่ามีข้อมูลส่งกลับมาจริงหรือไม่
        if not info or "quoteType" not in info:
            print(f"⚠️ ไม่พบข้อมูล info สำหรับ: {symbol}")
            return None

        return {
            "symbol": symbol,
            "short_name": info.get("shortName") or info.get("longName"),
            "quote_type": info.get("quoteType"),
            "sector": info.get("sector") or "N/A",
            "industry": info.get("industry") or "N/A",
            "market_cap": info.get("marketCap"),
            "currency": info.get("currency", "THB"),
        }
    except Exception as e:
        print(f"❌ ดึงข้อมูล {symbol} ล้มเหลว: {e}")
        return None


def upsert_stock_classification(engine, records: list[dict]):
    """บันทึกข้อมูลเข้าตาราง (Upsert / ON CONFLICT DO UPDATE)"""
    if not records:
        return

    upsert_sql = """
    INSERT INTO public.master_stock_classification (
        symbol, short_name, quote_type, sector, industry, market_cap, currency, updated_at
    ) VALUES (
        :symbol, :short_name, :quote_type, :sector, :industry, :market_cap, :currency, CURRENT_TIMESTAMP
    )
    ON CONFLICT (symbol) DO UPDATE SET
        short_name = EXCLUDED.short_name,
        quote_type = EXCLUDED.quote_type,
        sector = EXCLUDED.sector,
        industry = EXCLUDED.industry,
        market_cap = EXCLUDED.market_cap,
        currency = EXCLUDED.currency,
        updated_at = CURRENT_TIMESTAMP;
    """
    with engine.begin() as conn:
        conn.execute(text(upsert_sql), records)
    print(f"💾 บันทึกข้อมูล {len(records)} รายการลง Database สำเร็จ")


def main():
    engine = get_db_engine()
    init_database_tables(engine)

    symbols = get_target_symbols(engine)
    print(f"🔍 พบหุ้นที่ต้องประมวลผลทั้งหมด: {len(symbols)} ตัว\n")

    batch_records = []
    for i, sym in enumerate(symbols, start=1):
        print(f"[{i}/{len(symbols)}] กำลังดึงข้อมูล: {sym} ...")
        info_data = fetch_stock_info(sym)

        if info_data:
            batch_records.append(info_data)

        # หน่วงเวลาเล็กน้อย 0.3 วินาที เพื่อไม่ให้โดน Rate Limit จาก Yahoo Finance
        time.sleep( random.uniform(0.5, 3.5))

        # ตัดรอบบันทึกลง Database ทุกๆ 20 ตัว
        if len(batch_records) >= 20:
            upsert_stock_classification(engine, batch_records)
            batch_records.clear()

    # บันทึกส่วนที่เหลือ
    if batch_records:
        upsert_stock_classification(engine, batch_records)

    print("\n🎉 ปรับปรุงข้อมูล Master Stock Classification เสร็จสมบูรณ์แล้ว!")


if __name__ == "__main__":
    main()