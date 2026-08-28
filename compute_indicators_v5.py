#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
v5 (JSONB): เก็บ indicator ทั้งหมดใน field เดียว (indicators JSONB)
- เพิ่ม indicator ใหม่ได้โดยไม่ต้อง ALTER TABLE
- เขียนเฉพาะค่าที่เปลี่ยนจริง
- แต่ถ้าเพิ่ม indicator ใหม่ ต้องแก้ฟังก์ชัน compute_indicators เพื่อคำนวณค่าใหม่ ,อาจต้องแก้ฟังก์ชัน diff_json เพื่อเปรียบเทียบค่าใหม่ด้วย
แก้ไข ที่ indicators = {...} ใน main loop ด้วย แล้ว เรียกใช้ฟังก์ชัน call_function_recreate_view() เพื่อรีเฟรช view v_stock_indicators ที่ใช้ใน compute_signals และที่อื่นๆ ด้วย
"""

import os
from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd
from tqdm import tqdm
import psycopg2
from psycopg2.extras import execute_values, Json
from dotenv import load_dotenv

load_dotenv()

PG_CONN_STR = (
    f"host={os.getenv('posql_host','localhost')} "
    f"port={os.getenv('posql_port','5432')} "
    f"dbname={os.getenv('posql_db','stocks')} "
    f"user={os.getenv('posql_user','postgres')} "
    f"password={os.getenv('posql_password','postgres')}"
)

# ------- PARAM -------
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "1300"))
EPS = float(os.getenv("IND_EPS", "1e-6"))
BATCH_SIZE = 3000

# ------- SQL -------
DDL = """
CREATE TABLE IF NOT EXISTS stock_indicator_jsonb (
    symbol       TEXT NOT NULL,
    trade_date   DATE NOT NULL,
    indicators   JSONB NOT NULL,
    updated_at   TIMESTAMP DEFAULT now(),
    PRIMARY KEY (symbol, trade_date)
);
CREATE INDEX IF NOT EXISTS ix_stock_indicator_jsonb_symdate
ON stock_indicator_jsonb(symbol, trade_date);
"""

UPSERT_SQL = """
INSERT INTO stock_indicator_jsonb (symbol, trade_date, indicators)
VALUES %s
ON CONFLICT (symbol, trade_date)
DO UPDATE SET
    indicators = EXCLUDED.indicators,
    updated_at = now();
"""

# ------- DB util -------
def pg_conn():
    return psycopg2.connect(PG_CONN_STR)

def ensure_table():
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(DDL)
        conn.commit()

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
        return pd.read_sql(q, conn, params=(start_date, symbols))

def fetch_existing_json(symbols, start_date):
    with pg_conn() as conn:
        q = """
        SELECT symbol, trade_date, indicators
        FROM stock_indicator_jsonb
        WHERE trade_date >= %s AND symbol = ANY(%s);
        """
        return pd.read_sql(q, conn, params=(start_date, symbols))

# ------- indicator functions -------
def ema(series, span): 
    return series.ewm(span=span, adjust=False, min_periods=span).mean()

def macd_components(close, fast=12, slow=26, signal=9):
    ef, es = ema(close, fast), ema(close, slow)
    macd = ef - es
    sig = ema(macd, signal)
    return macd, sig, macd - sig

def rsi(series, period=14):
    delta = series.diff()
    gain, loss = delta.clip(lower=0), (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def classify_trend(c, e20, e50, e200, r):
    if pd.notna(c) and pd.notna(e200) and pd.notna(e20) and pd.notna(e50):
        if c > e200 and e20 > e50: return "uptrend"
        if c < e200 and e20 < e50: return "downtrend"
        if pd.notna(r) and e50 not in (None, 0) and abs(c - e50)/abs(e50) <= 0.02 and 40 <= r <= 60:
            return "sideway"
    return None

def atr(high, low, close, period=14):
    """คำนวณ Average True Range"""
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.ewm(alpha=1/period, adjust=False, min_periods=period).mean()

def bollinger_bands(close, period=20, std_dev=2):
    """คำนวณ Bollinger Bands (Upper, Middle, Lower)"""
    # เพื่อช่วยในการตั้งจุด Stop Loss และ Take Profit โดยดูจากความผันผวนของราคา
    sma = close.rolling(window=period).mean()
    rstd = close.rolling(window=period).std()
    upper = sma + (std_dev * rstd)
    lower = sma - (std_dev * rstd)
    return upper, sma, lower

# ------------------------------------------------
def compute_indicators(df):
    d = df.copy()
    # Exponential Moving Averages
    d["ema5"]    = ema(d["close"], 5)
    d["ema10"]   = ema(d["close"], 10)
    d["ema12"]   = ema(d["close"], 12)
    d["ema20"]   = ema(d["close"], 20)
    d["ema26"]   = ema(d["close"], 26)
    d["ema50"]   = ema(d["close"], 50)
    d["ema200"]  = ema(d["close"], 200)

    # Relative Strength Index
    d["rsi14"] = rsi(d["close"], 14)
    d["rsi21"]  = rsi(d["close"], 21)

    # MACD
    macd_12_26_9, sig_12_26_9, hist_12_26_9 = macd_components(d["close"])
    d["macd_12_26_9"], d["macd_12_26_9_signal"], d["macd_12_26_9_hist"] = macd_12_26_9, sig_12_26_9, hist_12_26_9
    macd_19_39_9, sig_19_39_9, hist_19_39_9 = macd_components(d["close"], fast=19, slow=39, signal=9)
    d["macd_19_39_9"], d["macd_19_39_9_signal"], d["macd_19_39_9_hist"] = macd_19_39_9, sig_19_39_9, hist_19_39_9

    # Volatility Indicators
    d["atr14"] = atr(d["high"], d["low"], d["close"], 14)
    bb_upper, bb_mid, bb_lower = bollinger_bands(d["close"], 20, 2)
    d["bb_upper"] = bb_upper
    d["bb_mid"] = bb_mid
    d["bb_lower"] = bb_lower

    # Volume Average
    d["volume_avg20"] = d["volume"].rolling(20, min_periods=20).mean()
    d["volume_ema5"] = ema(d["volume"], 5)
    d["volume_ema20"] = ema(d["volume"], 20)
    d["volume_ema50"] = ema(d["volume"], 50)
    
    # Trend Status
    d["trend_status"] = d.apply(lambda r: classify_trend(r["close"], r["ema20"], r["ema50"], r["ema200"], r["rsi14"]), axis=1)
    return d

# ------- compare -------
def diff_json(new, old, eps=EPS):
    if old is None:
        return True
    for k, v in new.items():
        ov = old.get(k)
        if isinstance(v, (int, float)) and isinstance(ov, (int, float)):
            if abs(float(v) - float(ov)) > eps:
                return True
        elif v != ov:
            return True
    return False

# ------- write -------
def upsert_batch(rows):
    if not rows:
        return
    with pg_conn() as conn, conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows, page_size=BATCH_SIZE, template="(%s,%s,%s)")
        conn.commit()

# ------- main -------
def main():
    ensure_table()
    symbols = get_active_symbols()
    if not symbols:
        print("❌ no symbols")
        return

    start_date = date.today() - timedelta(days=LOOKBACK_DAYS)
    print(f"📆 start from {start_date}")

    prices = fetch_prices(symbols, start_date)
    existing = fetch_existing_json(symbols, start_date)

    old_map = {(r.symbol, r.trade_date): r.indicators for r in existing.itertuples(index=False)}

    to_write = []
    for sym, df in tqdm(prices.groupby("symbol"), total=len(symbols), desc="Compute v5"):
        if df["close"].notna().sum() < 20:
            continue
        d = compute_indicators(df)
        for rec in d.itertuples(index=False):
            indicators = {
                "ema5": float(rec.ema5) if pd.notna(rec.ema5) else None,
                "ema10": float(rec.ema10) if pd.notna(rec.ema10) else None,
                "ema12": float(rec.ema12) if pd.notna(rec.ema12) else None,
                "ema20": float(rec.ema20) if pd.notna(rec.ema20) else None,
                "ema26": float(rec.ema26) if pd.notna(rec.ema26) else None,
                "ema50": float(rec.ema50) if pd.notna(rec.ema50) else None,
                "ema200": float(rec.ema200) if pd.notna(rec.ema200) else None,

                "rsi14": float(rec.rsi14) if pd.notna(rec.rsi14) else None,
                "rsi21": float(rec.rsi21) if pd.notna(rec.rsi21) else None,

                "macd_12_26_9": float(rec.macd_12_26_9) if pd.notna(rec.macd_12_26_9) else None,
                "macd_12_26_9_signal": float(rec.macd_12_26_9_signal) if pd.notna(rec.macd_12_26_9_signal) else None,
                "macd_12_26_9_hist": float(rec.macd_12_26_9_hist) if pd.notna(rec.macd_12_26_9_hist) else None,
                "macd_19_39_9": float(rec.macd_19_39_9) if pd.notna(rec.macd_19_39_9) else None,
                "macd_19_39_9_signal": float(rec.macd_19_39_9_signal) if pd.notna(rec.macd_19_39_9_signal) else None,
                "macd_19_39_9_hist": float(rec.macd_19_39_9_hist) if pd.notna(rec.macd_19_39_9_hist) else None,

                "volume_avg20": float(rec.volume_avg20) if pd.notna(rec.volume_avg20) else None,
                "volume_ema5": float(rec.volume_ema5) if pd.notna(rec.volume_ema5) else None,
                "volume_ema20": float(rec.volume_ema20) if pd.notna(rec.volume_ema20) else None,
                "volume_ema50": float(rec.volume_ema50) if pd.notna(rec.volume_ema50) else None,

                "atr14": float(rec.atr14) if pd.notna(rec.atr14) else None,
                "bb_upper": float(rec.bb_upper) if pd.notna(rec.bb_upper) else None,
                "bb_mid": float(rec.bb_mid) if pd.notna(rec.bb_mid) else None,
                "bb_lower": float(rec.bb_lower) if pd.notna(rec.bb_lower) else None,
                
                "trend_status": rec.trend_status
            }
            old = old_map.get((rec.symbol, rec.trade_date))
            if diff_json(indicators, old):
                to_write.append((rec.symbol, rec.trade_date, Json(indicators)))

        if len(to_write) >= 100000:
            print(f"🧾 upserting chunk: {len(to_write):,}")
            upsert_batch(to_write)
            to_write.clear()

    if to_write:
        print(f"🧾 upserting final: {len(to_write):,}")
        upsert_batch(to_write)

    print("✅ done v5 JSONB")

def call_function_recreate_view():
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("drop view if exists v_stock_indicators; SELECT public.refresh_indicator_view();")
        conn.commit()
    print("✅ refreshed view v_stock_indicators")

if __name__ == "__main__":
    main()
    call_function_recreate_view()