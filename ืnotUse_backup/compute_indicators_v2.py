import os
from datetime import date, timedelta
import numpy as np
import pandas as pd
from tqdm import tqdm
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

PG_CONN_STR = (
    f"host={os.getenv('posql_host','localhost')} "
    f"port={os.getenv('posql_port','5432')} "
    f"dbname={os.getenv('posql_db','stocks')} "
    f"user={os.getenv('posql_user','postgres')} "
    f"password={os.getenv('posql_password','postgres')}"
)

# ------------------------- Parameters -------------------------
LOOKBACK_DAYS = 1300       # ย้อนหลังกี่วันจากวันนี้ (คุณตั้งค่าได้)
UPDATE_BACK_DAYS = 1       # บังคับคำนวณเพิ่มอีก 1 วันก่อนหน้า เพื่อให้ EMA ต่อเนื่อง
BATCH_SIZE = 2000

# ------------------------- SQL Templates -------------------------
UPSERT_SQL = """
INSERT INTO stock_indicator_daily
(symbol, trade_date, ema20, ema50, ema200, rsi14, macd, macd_signal, macd_hist, volume_avg20, trend_status)
VALUES %s
ON CONFLICT (symbol, trade_date) DO UPDATE
SET
    ema20 = CASE WHEN stock_indicator_daily.ema20 IS DISTINCT FROM EXCLUDED.ema20 THEN EXCLUDED.ema20 ELSE stock_indicator_daily.ema20 END,
    ema50 = CASE WHEN stock_indicator_daily.ema50 IS DISTINCT FROM EXCLUDED.ema50 THEN EXCLUDED.ema50 ELSE stock_indicator_daily.ema50 END,
    ema200 = CASE WHEN stock_indicator_daily.ema200 IS DISTINCT FROM EXCLUDED.ema200 THEN EXCLUDED.ema200 ELSE stock_indicator_daily.ema200 END,
    rsi14 = CASE WHEN stock_indicator_daily.rsi14 IS DISTINCT FROM EXCLUDED.rsi14 THEN EXCLUDED.rsi14 ELSE stock_indicator_daily.rsi14 END,
    macd = CASE WHEN stock_indicator_daily.macd IS DISTINCT FROM EXCLUDED.macd THEN EXCLUDED.macd ELSE stock_indicator_daily.macd END,
    macd_signal = CASE WHEN stock_indicator_daily.macd_signal IS DISTINCT FROM EXCLUDED.macd_signal THEN EXCLUDED.macd_signal ELSE stock_indicator_daily.macd_signal END,
    macd_hist = CASE WHEN stock_indicator_daily.macd_hist IS DISTINCT FROM EXCLUDED.macd_hist THEN EXCLUDED.macd_hist ELSE stock_indicator_daily.macd_hist END,
    volume_avg20 = CASE WHEN stock_indicator_daily.volume_avg20 IS DISTINCT FROM EXCLUDED.volume_avg20 THEN EXCLUDED.volume_avg20 ELSE stock_indicator_daily.volume_avg20 END,
    trend_status = CASE WHEN stock_indicator_daily.trend_status IS DISTINCT FROM EXCLUDED.trend_status THEN EXCLUDED.trend_status ELSE stock_indicator_daily.trend_status END,
    updated_at = CASE WHEN (
        stock_indicator_daily.ema20 IS DISTINCT FROM EXCLUDED.ema20 OR
        stock_indicator_daily.ema50 IS DISTINCT FROM EXCLUDED.ema50 OR
        stock_indicator_daily.ema200 IS DISTINCT FROM EXCLUDED.ema200 OR
        stock_indicator_daily.rsi14 IS DISTINCT FROM EXCLUDED.rsi14 OR
        stock_indicator_daily.macd IS DISTINCT FROM EXCLUDED.macd OR
        stock_indicator_daily.macd_signal IS DISTINCT FROM EXCLUDED.macd_signal OR
        stock_indicator_daily.macd_hist IS DISTINCT FROM EXCLUDED.macd_hist OR
        stock_indicator_daily.volume_avg20 IS DISTINCT FROM EXCLUDED.volume_avg20 OR
        stock_indicator_daily.trend_status IS DISTINCT FROM EXCLUDED.trend_status
    ) THEN now() ELSE stock_indicator_daily.updated_at END;
"""

# ------------------------- Helper Functions -------------------------
def pg_conn():
    return psycopg2.connect(PG_CONN_STR)

def get_last_indicator_date():
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT max(trade_date) FROM stock_indicator_daily;")
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    
def get_last_price_date():
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT max(date) FROM stock_price_history;")
        row = cur.fetchone()
        return row[0] if row and row[0] else None

def get_active_symbols():
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT DISTINCT symbol FROM settrade_stocklist WHERE symbol IS NOT NULL;")
        return [r[0] for r in cur.fetchall()]

def fetch_prices(symbols, start_date):
    with pg_conn() as conn:
        q = """
        SELECT symbol, date AS trade_date, open, high, low, close, volume
        FROM stock_price_history
        WHERE date >= %s AND symbol = ANY(%s)
        ORDER BY symbol, trade_date;
        """
        df = pd.read_sql(q, conn, params=(start_date, symbols))
    return df

# ------------------------- Indicator Functions -------------------------
def ema(series, span): return series.ewm(span=span, adjust=False, min_periods=span).mean()

def macd_components(close, fast=12, slow=26, signal=9):
    ema_fast, ema_slow = ema(close, fast), ema(close, slow)
    macd = ema_fast - ema_slow
    signal_line = ema(macd, signal)
    return macd, signal_line, macd - signal_line

def rsi(series, period=14):
    delta = series.diff()
    gain, loss = delta.clip(lower=0), (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def classify_trend(c, e20, e50, e200, rsi):
    if pd.notna(c) and pd.notna(e200) and pd.notna(e20) and pd.notna(e50):
        if c > e200 and e20 > e50: return "uptrend"
        if c < e200 and e20 < e50: return "downtrend"
        if pd.notna(rsi) and abs(c - e50)/abs(e50) <= 0.02 and 40 <= rsi <= 60: return "sideway"
    return None

# ------------------------- Core Logic -------------------------
def compute_for_symbol(df_sym):
    df = df_sym.copy()
    df["ema20"], df["ema50"], df["ema200"] = ema(df["close"],20), ema(df["close"],50), ema(df["close"],200)
    df["rsi14"] = rsi(df["close"],14)
    macd, sig, hist = macd_components(df["close"])
    df["macd"], df["macd_signal"], df["macd_hist"] = macd, sig, hist
    df["volume_avg20"] = df["volume"].rolling(20, min_periods=20).mean()
    df["trend_status"] = df.apply(lambda r: classify_trend(r["close"], r["ema20"], r["ema50"], r["ema200"], r["rsi14"]), axis=1)
    return df

def upsert(rows):
    with pg_conn() as conn, conn.cursor() as cur:
        batch = []
        for r in rows:
            batch.append(r)
            if len(batch) >= BATCH_SIZE:
                execute_values(cur, UPSERT_SQL, batch, page_size=BATCH_SIZE)
                conn.commit()
                batch.clear()
        if batch:
            execute_values(cur, UPSERT_SQL, batch, page_size=BATCH_SIZE)
            conn.commit()

# ------------------------- Main -------------------------
def main():
    last_date = get_last_indicator_date() #get_last_price_date()  #
    symbols = get_active_symbols()

    if not symbols:
        print("❌ No symbols found.")
        return

    # วันที่เริ่มดึงข้อมูล
    if last_date:
        start_date = last_date - timedelta(days=UPDATE_BACK_DAYS)
    else:
        start_date = date.today() - timedelta(days=LOOKBACK_DAYS)

    print(f"📆 Fetching price data from {start_date} ...")
    df = fetch_prices(symbols, start_date)
    print(df)
    all_rows = []
    for sym, df_sym in tqdm(df.groupby("symbol"), total=df["symbol"].nunique(), desc="Computing"):
        if df_sym["close"].notna().sum() < 30:
            continue

        df_calc = compute_for_symbol(df_sym)
        for rec in df_calc.itertuples(index=False):
            all_rows.append((
                rec.symbol,
                rec.trade_date,
                None if pd.isna(rec.ema20) else float(rec.ema20),
                None if pd.isna(rec.ema50) else float(rec.ema50),
                None if pd.isna(rec.ema200) else float(rec.ema200),
                None if pd.isna(rec.rsi14) else float(rec.rsi14),
                None if pd.isna(rec.macd) else float(rec.macd),
                None if pd.isna(rec.macd_signal) else float(rec.macd_signal),
                None if pd.isna(rec.macd_hist) else float(rec.macd_hist),
                None if pd.isna(rec.volume_avg20) else float(rec.volume_avg20),
                rec.trend_status
            ))

    if all_rows:
        print(f"🧾 Upserting {len(all_rows):,} rows ...")
        upsert(all_rows)
        print("✅ Done.")
    else:
        print("No data to update.")

if __name__ == "__main__":
    main()
