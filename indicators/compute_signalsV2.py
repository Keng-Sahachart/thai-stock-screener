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
    created_at  = timezone('Asia/Bangkok', now());
"""

# ------------------------------------------------
def pg_conn():
    return psycopg2.connect(PG_CONN_STR)

def ensure_table():
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(DDL)
        conn.commit()

def fetch_recent_indicators(days_back=LOOKBACK_DAYS):
    # ดึงข้อมูล indicator ย้อนหลังจาก v_stock_indicators (หรือ stock_indicator_daily) เพื่อใช้คำนวณสัญญาณ
    start = date.today() - timedelta(days=days_back)
    with pg_conn() as conn:
        # ตาราง stock_indicator_daily มีข้อมูล indicator รายวันที่คำนวณจาก compute_indicators_v3 หรือ v5
        q = """
        SELECT
            v.symbol, v.trade_date,
            v.ema20, v.ema50, v.ema200,
            v.rsi14, 
            v.macd_12_26_9 AS macd, 
            v.macd_12_26_9_signal AS macd_signal,
            v.atr14, v.bb_lower, v.bb_upper, -- ดึงค่าความผันผวนที่เราเพิ่มใน v5
            p.close  -- <--- ต้องดึงคอลัมน์นี้เพิ่มเข้ามา
        FROM mv_stock_indicators v
        JOIN stock_price_history p ON v.symbol = p.symbol AND v.trade_date = p.date
        WHERE v.trade_date >= %s
        ORDER BY v.symbol, v.trade_date;
        """
        return pd.read_sql(q, conn, params=(start,))


def fetch_existing_signals():
    """โหลดสัญญาณล่าสุด เพื่อเทียบว่าควรเขียนใหม่ไหม"""
    with pg_conn() as conn: 
        q = "SELECT symbol, trade_date, signal_type FROM stock_signal;"
        df = pd.read_sql(q, conn)
        return {(r.symbol, r.trade_date): r.signal_type for r in df.itertuples(index=False)}

def check_rsi_divergence(df_window):
    """
    ตรวจหา Divergence ภายใน window ของข้อมูลที่ส่งมา (เช่น 10 วันล่าสุด)
    df_window ต้องมีคอลัมน์ 'close' และ 'rsi14'
    """
    if len(df_window) < 5: return None

    # หาจุดต่ำสุด/สูงสุดของราคาและ RSI ในช่วง window
    price_min_idx = df_window['close'].idxmin()
    price_max_idx = df_window['close'].idxmax()
    rsi_min_idx = df_window['rsi14'].idxmin()
    rsi_max_idx = df_window['rsi14'].idxmax()

    # --- Bullish Divergence (สัญญาณกลับตัวขึ้น) ---
    # ราคาทำ New Low แต่ RSI ไม่ทำ New Low (ยกตัวสูงขึ้น)
    first_half = df_window.iloc[:len(df_window)//2]
    second_half = df_window.iloc[len(df_window)//2:]
    
    if second_half['close'].min() < first_half['close'].min(): # ราคาทำ Low ใหม่
        if second_half['rsi14'].min() > first_half['rsi14'].min(): # RSI ยกตัว
            return "BULLISH_DIVERGENCE"

    # --- Bearish Divergence (สัญญาณกลับตัวลง) ---
    # ราคาทำ New High แต่ RSI ไม่ทำ New High (ลดตัวลง)
    if second_half['close'].max() > first_half['close'].max(): # ราคาทำ High ใหม่
        if second_half['rsi14'].max() < first_half['rsi14'].max(): # RSI ลดลง
            return "BEARISH_DIVERGENCE"

    return None

# ------------------------------------------------
def detect_signals(df_sym: pd.DataFrame):
    """คืนค่า list ของสัญญาณสำหรับ symbol เดียว แบบ Independent Checks (Multi-tagging)"""
    out = []
    df_sym = df_sym.sort_values("trade_date").reset_index(drop=True)
    DIV_WINDOW = 10

    for i, r in df_sym.iterrows():
        # ข้ามถ้า Indicator หลักยังไม่พร้อม
        if (
            pd.isna(r["ema20"]) 
            or pd.isna(r["ema50"]) 
            or pd.isna(r["rsi14"]) 
            or pd.isna(r["macd_signal"]) 
            or pd.isna(r["atr14"])
        ):
            continue

        # --- 1. คำนวณ Crossover และ Divergence ---
        macd_cross_up = False
        macd_cross_down = False
        if i > 0:
            prev_macd, prev_sigline = df_sym.loc[i - 1, ["macd", "macd_signal"]]
            if prev_macd is not None and prev_sigline is not None:
                macd_cross_up = (prev_macd < prev_sigline) and (r["macd"] > r["macd_signal"])
                macd_cross_down = (prev_macd > prev_sigline) and (r["macd"] < r["macd_signal"])

        div_type = None
        if i >= DIV_WINDOW - 1:
            window_df = df_sym.iloc[i - DIV_WINDOW + 1 : i + 1]
            div_type = check_rsi_divergence(window_df)

        # --- 2. Independent Tagging (ตรวจสอบทุกเงื่อนไขแบบอิสระ) ---
        buy_tags = []
        sell_tags = []
        watch_tags = []
        neutral_tags = []

        # [หมวด BUY Signals]
        if div_type == "BULLISH_DIVERGENCE":
            buy_tags.append("Bullish Div")
        if macd_cross_up:
            buy_tags.append("MACD Cross Up")
        if r["ema20"] > r["ema50"]:
            buy_tags.append("EMA Bullish (20>50)")
        if r["rsi14"] > 45:
            buy_tags.append("RSI Bullish (>45)")

        # [หมวด SELL Signals]
        if div_type == "BEARISH_DIVERGENCE":
            sell_tags.append("Bearish Div")
        if macd_cross_down:
            sell_tags.append("MACD Cross Down")
        if pd.notna(r.get("bb_lower")) and r["close"] < r["bb_lower"]:
            sell_tags.append("Break BB Lower")
        if r["ema20"] < r["ema50"]:
            sell_tags.append("EMA Bearish (20<50)")
        if r["rsi14"] < 55:
            sell_tags.append("RSI Bearish (<55)")

        # [หมวด WATCH / SIDEWAY]
        if r["ema20"] > r["ema50"] and r["rsi14"] < 40:
            watch_tags.append("Pullback (EMA Uptrend & RSI<40)")
        if r["ema20"] < r["ema50"] and r["rsi14"] > 60:
            watch_tags.append("Technical Bounce (EMA Downtrend & RSI>60)")
        if abs(r["ema20"] - r["ema50"]) / r["ema50"] < 0.01 and 40 <= r["rsi14"] <= 60:
            neutral_tags.append("Sideway (EMA Flat & RSI Neutral)")

        # --- 3. การประเมิน Action หลัก (Synthesis & Prioritization) ---
        sig, pri = "HOLD", 0
        stop_loss = r["close"] - (r["atr14"] * 2)

        # จัดลำดับความสำคัญของ Decision
        if "Bearish Div" in sell_tags or "Break BB Lower" in sell_tags or ("MACD Cross Down" in sell_tags and "EMA Bearish (20<50)" in sell_tags):
            sig, pri = "SELL", 4 if "Bearish Div" in sell_tags else 3
        elif "Bullish Div" in buy_tags and "MACD Cross Up" in buy_tags:
            sig, pri = "BUY-STRONG", 5
        elif "EMA Bullish (20>50)" in buy_tags and "MACD Cross Up" in buy_tags and "RSI Bullish (>45)" in buy_tags:
            sig, pri = "BUY", 3
        elif watch_tags:
            if "Pullback (EMA Uptrend & RSI<40)" in watch_tags:
                sig, pri = "BUY-WATCH", 2
            else:
                sig, pri = "SELL-WATCH", 2
        elif neutral_tags:
            sig, pri = "SIDEWAY", 1

        # รวมเหตุผลทั้งหมดที่เกิดขึ้นในวันนั้น (Multi-tag string)
        all_detected = buy_tags + sell_tags + watch_tags + neutral_tags
        if all_detected:
            tag_summary = ", ".join(all_detected)
            reason = f"[{sig}] {tag_summary}" + (f" | SL: {stop_loss:.2f}" if "BUY" in sig else "")
        else:
            reason = "HOLD: No active technical tags"

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

    ind = fetch_recent_indicators()  # ดึงข้อมูล indicator ย้อนหลังจาก v_stock_indicators (หรือ stock_indicator_daily) เพื่อใช้คำนวณสัญญาณ
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
    main()  #['TDEX'] ตัวอย่างรันสำหรับ symbol 'TDEX'
