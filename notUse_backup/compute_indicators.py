#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Compute daily indicators (EMA/RSI/MACD/VolAvg) for active symbols
and upsert into stock_indicator_daily.
"""

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

# ปรับค่าได้
LOOKBACK_CAL_DAYS = int(os.getenv("IND_LOOKBACK_DAYS", "1000"))  # โหลดราคาย้อนหลังมากพอสำหรับ EMA200
BATCH_SIZE = int(os.getenv("IND_BATCH_SIZE", "2000"))            # ขนาด batch ตอน upsert

DDL = """
CREATE TABLE IF NOT EXISTS stock_indicator_daily (
    symbol          TEXT NOT NULL,
    trade_date      DATE NOT NULL,
    ema20           NUMERIC(18,6),
    ema50           NUMERIC(18,6),
    ema200          NUMERIC(18,6),
    rsi14           NUMERIC(18,6),
    macd            NUMERIC(18,6),
    macd_signal     NUMERIC(18,6),
    macd_hist       NUMERIC(18,6),
    volume_avg20    NUMERIC(18,2),
    trend_status    TEXT,
    updated_at      TIMESTAMP DEFAULT now(),
    PRIMARY KEY(symbol, trade_date)
);
CREATE INDEX IF NOT EXISTS ix_stock_indicator_daily_symdate ON stock_indicator_daily(symbol, trade_date);
"""

UPSERT_SQL = """
INSERT INTO stock_indicator_daily
(symbol, trade_date, ema20, ema50, ema200, rsi14, macd, macd_signal, macd_hist, volume_avg20, trend_status)
VALUES %s
ON CONFLICT (symbol, trade_date) DO UPDATE SET
  ema20        = EXCLUDED.ema20,
  ema50        = EXCLUDED.ema50,
  ema200       = EXCLUDED.ema200,
  rsi14        = EXCLUDED.rsi14,
  macd         = EXCLUDED.macd,
  macd_signal  = EXCLUDED.macd_signal,
  macd_hist    = EXCLUDED.macd_hist,
  volume_avg20 = EXCLUDED.volume_avg20,
  trend_status = EXCLUDED.trend_status,
  updated_at   = now();
"""

def pg_conn():
    return psycopg2.connect(PG_CONN_STR)

def ensure_table():
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(DDL)
        conn.commit()

def get_active_symbols():
    """
    ดึงรายชื่อหุ้นที่ active จาก settrade_stocklist
    ปรับ WHERE ให้ตรงสคีมาของคุณ:
      - ถ้ามีคอลัมน์ is_active ใช้ is_active = true
      - ถ้าไม่มี ให้ดึงทั้งหมดที่ไม่ว่าง
    """
    sql_candidates = [
        "SELECT DISTINCT symbol FROM settrade_stocklist WHERE is_active = true ORDER BY symbol",
        "SELECT DISTINCT symbol FROM settrade_stocklist WHERE status = 'A' ORDER BY symbol",
        "SELECT DISTINCT symbol FROM settrade_stocklist WHERE symbol IS NOT NULL ORDER BY symbol",
    ]
    with pg_conn() as conn, conn.cursor() as cur:
        for s in sql_candidates:
            try:
                cur.execute(s)
                rows = cur.fetchall()
                if rows:
                    return [r[0] for r in rows]
            except Exception:
                conn.rollback()
        # fallback: ไม่มีเงื่อนไข
        cur.execute("SELECT DISTINCT symbol FROM settrade_stocklist ORDER BY symbol")
        return [r[0] for r in cur.fetchall()]

def fetch_prices(symbols):
    """
    ดึงราคาจาก stock_price_history ย้อนหลัง LOOKBACK_CAL_DAYS
    คืนค่า DataFrame: columns = [symbol, trade_date, open, high, low, close, volume]
    """
    start_date = date.today() - timedelta(days=LOOKBACK_CAL_DAYS)
    with pg_conn() as conn:
        q = """
        SELECT symbol, date AS trade_date, open, high, low, close, volume
        FROM stock_price_history
        WHERE date >= %s AND symbol = ANY(%s)
        ORDER BY symbol, date
        """
        df = pd.read_sql(q, conn, params=(start_date, symbols))
    return df

# -------------------- Indicator functions (no TA-Lib) --------------------
def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False, min_periods=span).mean()

def macd_components(close: pd.Series, fast=12, slow=26, signal=9):
    ema_fast = ema(close, fast)
    ema_slow = ema(close, slow)
    macd_line = ema_fast - ema_slow
    signal_line = ema(macd_line, signal)
    hist = macd_line - signal_line
    return macd_line, signal_line, hist

def rsi(series: pd.Series, period=14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    # Wilder's smoothing = EMA(alpha=1/period)
    avg_gain = gain.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss
    rsi_val = 100 - (100 / (1 + rs))
    return rsi_val

def classify_trend(row) -> str | None:
    """
    กฎเบื้องต้น:
      - uptrend: close > ema200 และ ema20 > ema50
      - downtrend: close < ema200 และ ema20 < ema50
      - sideway: |close - ema50| <= 2% ของ ema50 และ 40 <= RSI <= 60
      - else: None
    """
    c = row["close"]
    e20 = row["ema20"]
    e50 = row["ema50"]
    e200 = row["ema200"]
    r = row["rsi14"]

    if pd.notna(c) and pd.notna(e200) and pd.notna(e20) and pd.notna(e50):
        if c > e200 and e20 > e50:
            return "uptrend"
        if c < e200 and e20 < e50:
            return "downtrend"
        if pd.notna(r) and pd.notna(e50) and e50 != 0:
            if abs(c - e50) / abs(e50) <= 0.02 and (40 <= r <= 60):
                return "sideway"
    return None

# -------------------- Main compute --------------------
def compute_for_symbol(df_symbol: pd.DataFrame) -> pd.DataFrame:
    """
    รับราคาของ symbol เดียว (sorted) คืน DataFrame อินดิเคเตอร์พร้อม trade_date
    """
    df = df_symbol.copy()
    df["ema20"] = ema(df["close"], 20)
    df["ema50"] = ema(df["close"], 50)
    df["ema200"] = ema(df["close"], 200)
    df["rsi14"] = rsi(df["close"], 14)
    macd_line, sig_line, hist = macd_components(df["close"], 12, 26, 9)
    df["macd"] = macd_line
    df["macd_signal"] = sig_line
    df["macd_hist"] = hist
    df["volume_avg20"] = df["volume"].rolling(20, min_periods=20).mean()

    # จัดชุดคอลัมน์ผลลัพธ์
    out = df[[
        "symbol","trade_date","close","ema20","ema50","ema200",
        "rsi14","macd","macd_signal","macd_hist","volume","volume_avg20"
    ]].copy()

    # trend_status
    out["trend_status"] = out.apply(lambda r: classify_trend({
        "close": r["close"], "ema20": r["ema20"], "ema50": r["ema50"], "ema200": r["ema200"], "rsi14": r["rsi14"]
    }), axis=1)

    return out

def upsert_indicators(rows):
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

def main():
    ensure_table()
    symbols = get_active_symbols()
    if not symbols:
        print("No active symbols found in settrade_stocklist.")
        return

    prices = fetch_prices(symbols)
    if prices.empty:
        print("No price data fetched.")
        return

    # ทำความสะอาด
    prices = prices.sort_values(["symbol","trade_date"]).reset_index(drop=True)

    all_rows = []
    for sym, df_sym in tqdm(prices.groupby("symbol"), total=prices["symbol"].nunique(), desc="Compute indicators"):
        # ป้องกันข้อมูลกรณีไม่มี close/volume
        if df_sym["close"].notna().sum() == 0:
            continue
        res = compute_for_symbol(df_sym)

        # เตรียมแถวสำหรับ upsert (เฉพาะวันที่ที่มีข้อมูล)
        for rec in res.itertuples(index=False):
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

        # อัปเสิร์ตเป็นช่วง ๆ เพื่อลด RAM
        if len(all_rows) >= 100_000:
            upsert_indicators(all_rows)
            all_rows.clear()

    # ส่วนคงค้าง
    if all_rows:
        upsert_indicators(all_rows)

    print("Done.")

if __name__ == "__main__":
    main()
