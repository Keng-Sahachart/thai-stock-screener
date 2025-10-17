#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
v3: คำนวณเต็มช่วงย้อนหลังที่กำหนด แล้ว "เขียนเฉพาะที่เปลี่ยนจริง"
- START_DATE (YYYY-MM-DD) หรือ LOOKBACK_DAYS (เช่น 1300)
- เปรียบเทียบกับค่าที่มีใน stock_indicator_daily ด้วย epsilon เพื่อลด write I/O
"""

import os
from datetime import date, datetime, timedelta
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

# ------- พารามิเตอร์หลัก -------
START_DATE_STR = os.getenv("START_DATE")        # เช่น '2020-01-01'; ถ้าไม่ตั้ง จะใช้ LOOKBACK_DAYS
LOOKBACK_DAYS  = int(os.getenv("LOOKBACK_DAYS", "1300"))
BATCH_SIZE     = int(os.getenv("IND_BATCH_SIZE", "3000"))
EPS            = float(os.getenv("IND_EPS", "1e-6"))  # tolerance สำหรับ float comparison

# ------- SQL -------
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
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT DISTINCT symbol FROM settrade_stocklist WHERE symbol IS NOT NULL;")
        return [r[0] for r in cur.fetchall()]

def resolve_start_date():
    if START_DATE_STR:
        return datetime.strptime(START_DATE_STR, "%Y-%m-%d").date()
    return date.today() - timedelta(days=LOOKBACK_DAYS)

def fetch_prices(symbols, start_date):
    with pg_conn() as conn:
        q = """
        SELECT symbol, date AS trade_date, open, high, low, close, volume
        FROM stock_price_history
        WHERE date >= %s AND symbol = ANY(%s)
        ORDER BY symbol, trade_date;
        """
        return pd.read_sql(q, conn, params=(start_date, symbols))

def fetch_existing_indicators(symbols, start_date):
    """ดึง indicator ที่มีอยู่แล้วในช่วงเดียวกัน เพื่อใช้เทียบค่า (ลดการเขียน)"""
    with pg_conn() as conn:
        q = """
        SELECT symbol, trade_date, ema20, ema50, ema200, rsi14, macd, macd_signal, macd_hist, volume_avg20, trend_status
        FROM stock_indicator_daily
        WHERE trade_date >= %s AND symbol = ANY(%s)
        ORDER BY symbol, trade_date;
        """
        return pd.read_sql(q, conn, params=(start_date, symbols))

# -------- indicator functions (no TA-Lib) --------
def ema(series, span): 
    return series.ewm(span=span, adjust=False, min_periods=span).mean()

def macd_components(close, fast=12, slow=26, signal=9):
    ef, es = ema(close, fast), ema(close, slow)
    macd = ef - es
    sig  = ema(macd, signal)
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

def compute_for_symbol(df_sym):
    d = df_sym.copy()
    d["ema20"]  = ema(d["close"], 20)
    d["ema50"]  = ema(d["close"], 50)
    d["ema200"] = ema(d["close"], 200)
    d["rsi14"]  = rsi(d["close"], 14)
    macd, sig, hist = macd_components(d["close"])
    d["macd"], d["macd_signal"], d["macd_hist"] = macd, sig, hist
    d["volume_avg20"] = d["volume"].rolling(20, min_periods=20).mean()
    d["trend_status"] = d.apply(lambda r: classify_trend(r["close"], r["ema20"], r["ema50"], r["ema200"], r["rsi14"]), axis=1)

    return d[["symbol","trade_date","ema20","ema50","ema200","rsi14","macd","macd_signal","macd_hist","volume_avg20","trend_status"]]

# -------- compare & selective write --------
FLOAT_COLS = ["ema20","ema50","ema200","rsi14","macd","macd_signal","macd_hist","volume_avg20"]
STR_COLS   = ["trend_status"]

def is_diff(a, b, eps=EPS):
    # เทียบ float: True ถ้าแตกต่างเกิน eps; เทียบ None/NaN ให้เท่ากัน
    if pd.isna(a) and pd.isna(b): return False
    if (a is None and b is None): return False
    if (a is None) ^ (b is None): return True
    try:
        return abs(float(a) - float(b)) > eps
    except Exception:
        return str(a) != str(b)

def need_update(row_new, row_old):
    """True ถ้าควรเขียน (ไม่เคยมี หรือค่าเปลี่ยน)"""
    if row_old is None:
        return True
    # เปรียบเทียบทุกคอลัมน์อินดิเคเตอร์
    for col in FLOAT_COLS:
        if is_diff(row_new.get(col), row_old.get(col)):
            return True
    for col in STR_COLS:
        if (row_new.get(col) or None) != (row_old.get(col) or None):
            return True
    return False

def upsert_rows(rows):
    if not rows: 
        return
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
        print("❌ No symbols.")
        return

    start_date = resolve_start_date()
    print(f"📆 Start date = {start_date} (mode={'START_DATE' if START_DATE_STR else f'LOOKBACK {LOOKBACK_DAYS} days'})")

    # 1) โหลดราคาช่วงที่กำหนด
    price = fetch_prices(symbols, start_date)
    if price.empty:
        print("No price rows.")
        return
    price = price.sort_values(["symbol","trade_date"]).reset_index(drop=True)

    # 2) โหลด indicators เดิมในช่วงเดียวกัน (ไว้เทียบ)
    existing = fetch_existing_indicators(symbols, start_date)
    # ทำเป็น dict เร็ว ๆ: key = (symbol, date) → dict ของค่าเดิม
    old_map = {}
    if not existing.empty:
        for rec in existing.itertuples(index=False):
            old_map[(rec.symbol, rec.trade_date)] = {
                "ema20": rec.ema20, "ema50": rec.ema50, "ema200": rec.ema200,
                "rsi14": rec.rsi14, "macd": rec.macd, "macd_signal": rec.macd_signal,
                "macd_hist": rec.macd_hist, "volume_avg20": rec.volume_avg20,
                "trend_status": rec.trend_status
            }

    # 3) คำนวณทั้งหมดในช่วงที่กำหนด
    to_write = []
    for sym, df_sym in tqdm(price.groupby("symbol"), total=price["symbol"].nunique(), desc="Compute v3"):
        if df_sym["close"].notna().sum() < 30:
            continue
        calc = compute_for_symbol(df_sym)

        for rec in calc.itertuples(index=False):
            new_row = {
                "ema20": rec.ema20, "ema50": rec.ema50, "ema200": rec.ema200,
                "rsi14": rec.rsi14, "macd": rec.macd, "macd_signal": rec.macd_signal,
                "macd_hist": rec.macd_hist, "volume_avg20": rec.volume_avg20,
                "trend_status": rec.trend_status
            }
            old_row = old_map.get((rec.symbol, rec.trade_date))
            if need_update(new_row, old_row):
                to_write.append((
                    rec.symbol, rec.trade_date,
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

        # flush เป็นช่วง ๆ เพื่อลด RAM
        if len(to_write) >= 120_000:
            print(f"🧾 Upserting chunk: {len(to_write):,} rows ...")
            upsert_rows(to_write)
            to_write.clear()

    # 4) เขียนส่วนคงค้าง
    if to_write:
        print(f"🧾 Upserting final: {len(to_write):,} rows ...")
        upsert_rows(to_write)
    else:
        print("No changed rows to write.")

    print("✅ Done v3")

if __name__ == "__main__":
    main()
