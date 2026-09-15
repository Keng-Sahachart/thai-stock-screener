#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Recalculate trailing stops for open positions using Highest Close instead of Highest High.
"""

import os
import sys
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

# เพิ่ม root directory ลงใน sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

load_dotenv(ROOT_DIR / ".env")

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("posql_host", "localhost"),
        port=os.getenv("posql_port", "5432"),
        dbname=os.getenv("posql_db", "stocks"),
        user=os.getenv("posql_user", "postgres"),
        password=os.getenv("posql_password", "postgres")
    )

def main():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # ดึงไม้ที่ OPEN ทั้งหมด เพื่อหา highest close ตั้งแต่วัน entry_date
            cur.execute("""
                SELECT 
                    b.id, b.symbol, b.entry_price, b.initial_stop_loss,
                    b.max_price_reached as old_max, b.trailing_stop_loss as old_ts,
                    COALESCE(p_max.max_close, b.entry_price) as new_max_close,
                    i.atr14
                FROM bot_active_positions b
                LEFT JOIN LATERAL (
                    SELECT MAX(close) as max_close
                    FROM stock_price_history sph
                    WHERE sph.symbol = b.symbol AND sph.date >= b.entry_date
                ) p_max ON true
                LEFT JOIN LATERAL (
                    SELECT atr14
                    FROM mv_stock_indicators mvi
                    WHERE mvi.symbol = b.symbol
                    ORDER BY mvi.trade_date DESC LIMIT 1
                ) i ON true
                WHERE b.status = 'OPEN';
            """)
            rows = cur.fetchall()

            print("================================================================================")
            print("🔄 ปรับปรุงระดับ Trailing Stop ด้วยเกณฑ์ Highest Close (ราคาปิดสูงสุด)")
            print("================================================================================")
            print(f"{'Symbol':<8} | {'ทุน':<6} | {'Init SL':<7} | {'Old Max':<7} | {'Old TS':<7} | {'New Max':<7} | {'New TS':<7}")
            print("-" * 80)

            for r in rows:
                pos_id, sym, entry_p, init_sl, old_max, old_ts, new_max_close, atr = r
                entry_p = float(entry_p)
                init_sl = float(init_sl)
                new_max = max(entry_p, float(new_max_close or entry_p))
                atr = float(atr) if atr else None

                new_ts = None
                if atr and new_max > entry_p:
                    cand_ts = round(new_max - (atr * 2.0), 4)
                    if cand_ts > init_sl:
                        new_ts = cand_ts

                cur.execute("""
                    UPDATE bot_active_positions
                    SET max_price_reached = %s,
                        trailing_stop_loss = %s,
                        updated_at = timezone('Asia/Bangkok', now())
                    WHERE id = %s;
                """, (new_max, new_ts, pos_id))

                old_ts_str = f"{float(old_ts):.2f}" if old_ts is not None else "-"
                new_ts_str = f"{new_ts:.2f}" if new_ts is not None else "-"
                print(f"{sym:<8} | {entry_p:<6.2f} | {init_sl:<7.2f} | {float(old_max):<7.2f} | {old_ts_str:<7} | {new_max:<7.2f} | {new_ts_str:<7}")

            conn.commit()
            print("=" * 80)
            print("✅ บันทึกข้อมูล Trailing Stop ลงฐานข้อมูลเรียบร้อยแล้ว")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
