#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Compute trading signals from stock_indicator_daily
- Rule-based logic (BUY, SELL, SIDEWAY, HOLD)
- เขียนเฉพาะเมื่อมีการเปลี่ยนสถานะจริง
"""

import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import date, timedelta
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

PG_CONN_STR = (
    f"host={os.getenv('posql_host','localhost')} "
    f"port={os.getenv('posql_port','5432')} "
    f"dbname={os.getenv('posql_db','stocks')} "
    f"user={os.getenv('posql_user','postgres')} "
    f"password={os.getenv('posql_password','postgres')}"
)

BATCH_SIZE = int(os.getenv("SIGNAL_BATCH_SIZE", "2000"))
LOOKBACK_DAYS = int(os.getenv("SIGNAL_LOOKBACK_DAYS", "10"))  # ดึงอินดิเคเตอร์ย้อนหลังกี่วันเพื่อตัดสิน cross


DDL = """
CREATE TABLE IF NOT EXISTS stock_signal (
    symbol          TEXT NOT NULL,
    trade_date      DATE NOT NULL,
    signal_type     TEXT,
    priority        INT,
    reason          TEXT,
    created_at      TIMESTAMP DEFAULT now(),
    PRIMARY KEY(symbol, trade_date)
);
CREATE INDEX IF NOT EXISTS ix_stock_signal_symdate ON stock_signal(symbol, trade_date);
"""

# ตารางสัญญาณที่คำนวณได้ จะถูกเขียนทับทุกวัน (upsert) โดยมีเงื่อนไขว่า ถ้าสัญญาณไม่เปลี่ยนแปลงจากเดิม จะไม่เขียนซ้ำ
UPSERT_SQL = """
INSERT INTO stock_signal (symbol, trade_date, signal_type, priority, reason)
VALUES %s
ON CONFLICT (symbol, trade_date) DO UPDATE
SET signal_type = EXCLUDED.signal_type,
    priority    = EXCLUDED.priority,
    reason      = EXCLUDED.reason,
    created_at  = now();
"""

# ------------------------------------------------
def pg_conn():
    return psycopg2.connect(PG_CONN_STR)

def ensure_table():
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(DDL)
        conn.commit()

def fetch_recent_indicators(days_back=LOOKBACK_DAYS):
    start = date.today() - timedelta(days=days_back)
    with pg_conn() as conn:
        # ตาราง stock_indicator_daily มีข้อมูล indicator รายวันที่คำนวณจาก compute_indicators_v3 หรือ v5
        q = """
        SELECT
            symbol, trade_date,
            ema20, ema50, ema200,
            rsi14, macd, macd_signal, macd_hist,
            trend_status
        FROM stock_indicator_daily
        WHERE trade_date >= %s
        ORDER BY symbol, trade_date;
        """
        return pd.read_sql(q, conn, params=(start,))


def fetch_existing_signals():
    """โหลดสัญญาณล่าสุด เพื่อเทียบว่าควรเขียนใหม่ไหม"""
    with pg_conn() as conn:
        q = "SELECT symbol, trade_date, signal_type FROM stock_signal;"
        df = pd.read_sql(q, conn)
        return {(r.symbol, r.trade_date): r.signal_type for r in df.itertuples(index=False)}

# ------------------------------------------------
def detect_signals(df_sym: pd.DataFrame):
    """คืนค่า list ของสัญญาณสำหรับ symbol เดียว"""
    out = []
    df_sym = df_sym.sort_values("trade_date").reset_index(drop=True)
    for i, r in df_sym.iterrows():
        sig, pri, reason = None, 0, None

        # skip incomplete data
        if pd.isna(r["ema20"]) or pd.isna(r["ema50"]) or pd.isna(r["rsi14"]) or pd.isna(r["macd_signal"]):
            continue

        prev_macd, prev_sigline = (None, None)
        if i > 0:
            prev_macd, prev_sigline = df_sym.loc[i-1, ["macd", "macd_signal"]]

        macd_cross_up = prev_macd is not None and prev_sigline is not None and (prev_macd < prev_sigline) and (r["macd"] > r["macd_signal"])
        macd_cross_down = prev_macd is not None and prev_sigline is not None and (prev_macd > prev_sigline) and (r["macd"] < r["macd_signal"])

        # --- RULES ---
        if r["ema20"] > r["ema50"] and macd_cross_up and r["rsi14"] > 45:
            sig, pri, reason = "BUY", 3, "EMA20>EMA50 & MACD↑ & RSI>45" # แนวโน้มขาขึ้น + MACD ตัดขึ้น + RSI ไม่ต่ำเกิน
        elif r["ema20"] > r["ema50"] and r["rsi14"] < 40:
            sig, pri, reason = "BUY-WATCH", 2, "EMA20>EMA50 & RSI<40 (pullback)" # แนวโน้มขาขึ้น แต่ RSI ต่ำ
        elif r["ema20"] < r["ema50"] and macd_cross_down and r["rsi14"] < 55:
            sig, pri, reason = "SELL", 3, "EMA20<EMA50 & MACD↓ & RSI<55" # แนวโน้มขาลง + MACD ตัดลง + RSI ไม่สูงเกิน
        elif r["ema20"] < r["ema50"] and r["rsi14"] > 60:
            sig, pri, reason = "SELL-WATCH", 2, "EMA20<EMA50 & RSI>60 (bounce)" # แนวโน้มขาลง แต่ RSI สูง
        elif abs(r["ema20"] - r["ema50"]) / r["ema50"] < 0.01 and 40 <= r["rsi14"] <= 60:
            sig, pri, reason = "SIDEWAY", 1, "EMA20≈EMA50 & RSI neutral" # แนวโน้มแกว่งตัว
        else:
            sig, pri, reason = "HOLD", 0, "No signal change"# ถือสถานะเดิม

        out.append((r["symbol"], r["trade_date"], sig, pri, reason))

    return out

# ------------------------------------------------
def upsert(rows):
    if not rows: return
    with pg_conn() as conn, conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows, page_size=BATCH_SIZE)
        conn.commit()

# ------------------------------------------------
def main():
    ensure_table() # สร้างตารางถ้ายังไม่มี

    ind = fetch_recent_indicators()
    if ind.empty:
        print("❌ No indicator data found.")
        return

    existing = fetch_existing_signals()

    all_rows = []
    for sym, df_sym in tqdm(ind.groupby("symbol"), total=ind["symbol"].nunique(), desc="Compute signals"):
        sigs = detect_signals(df_sym)
        for (symb, dt, sig, pri, reason) in sigs:
            old = existing.get((symb, dt))
            if old != sig:  # เขียนเฉพาะวันที่สัญญาณเปลี่ยน
                all_rows.append((symb, dt, sig, pri, reason))

    if all_rows:
        print(f"🧾 Writing {len(all_rows):,} updated signals ...")
        upsert(all_rows)
    else:
        print("No signal changes to write.")

    print("✅ Done compute_signals.")

if __name__ == "__main__":
    main()
