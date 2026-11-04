'''
สคริปต์คำนวณและอัปเดตสถานะพอร์ต
ซิงค์หุ้นใหม่เข้า bot_active_positions พร้อมคำนวณ Initial Stop Loss ตาม Safety Clamps (Min 4.0% / Max 8.0%)  
ขยับค่า max_price_reached และยกจุด trailing_stop_loss ขึ้นตามกำไร  
'''
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "bot_config.json")

def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("posql_host", "localhost"),
        port=os.getenv("posql_port", "5432"),
        dbname=os.getenv("posql_db", "stocks"),
        user=os.getenv("posql_user", "postgres"),
        password=os.getenv("posql_password", "postgres")
    )

def calculate_initial_stop_loss(entry_price: float, atr14: float, quote_type: str, config: dict):
    """คำนวณ Initial Stop Loss โดยมี Safety Clamp ควบคุมกรอบ % ขาดทุน"""
    risk_cfg = config["risk_management"]
    multipliers = risk_cfg.get("atr_multiplier", {})
    mult = multipliers.get(quote_type, multipliers.get("DEFAULT", 2.0))
    
    min_stop_pct = risk_cfg["safety_clamp"]["min_stop_pct"]
    max_stop_pct = risk_cfg["safety_clamp"]["max_stop_pct"]

    if atr14 and atr14 > 0:
        calculated_sl = entry_price - (atr14 * mult)
        loss_pct = ((entry_price - calculated_sl) / entry_price) * 100.0
        
        # ปรับให้อยู่ในกรอบ Safety Clamp
        if loss_pct < min_stop_pct:
            final_sl = entry_price * (1.0 - (min_stop_pct / 100.0))
        elif loss_pct > max_stop_pct:
            final_sl = entry_price * (1.0 - (max_stop_pct / 100.0))
        else:
            final_sl = calculated_sl
    else:
        # กรณีไม่มี ATR ให้ใช้ค่า Default Clamp (5%)
        final_sl = entry_price * 0.95

    return round(final_sl, 4)

def sync_active_positions():
    config = load_config()
    excluded_symbols = set(config.get("excluded_symbols", []))

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 1. ดึงพอร์ตล่าสุดพร้อมข้อมูล Indicators
            cur.execute("""
                SELECT 
                    p.symbol,
                    p.current_volume,
                    p.average_price,
                    p.market_price,
                    c.quote_type,
                    i.atr14,
                    b.id AS position_id,
                    b.initial_stop_loss,
                    b.max_price_reached,
                    b.trailing_stop_loss
                FROM portfolio_stock p
                LEFT JOIN master_stock_classification c ON p.symbol = c.symbol
                LEFT JOIN bot_active_positions b ON p.symbol = b.symbol AND b.status = 'OPEN'
                LEFT JOIN mv_stock_indicators i ON p.symbol = i.symbol 
                    AND i.trade_date = (SELECT MAX(trade_date) FROM mv_stock_indicators)
                WHERE p.imported_at = (SELECT MAX(imported_at) FROM portfolio_stock);
            """)
            rows = cur.fetchall()

            for row in rows:
                sym = row["symbol"]
                curr_vol = row["current_volume"]
                mkt_price = float(row["market_price"] or 0)
                avg_price = float(row["average_price"] or 0)
                atr = float(row["atr14"]) if row["atr14"] else None
                q_type = row["quote_type"] or "EQUITY"

                # ข้ามหุ้นที่อยู่ในรายชื่อยกเว้น (หุ้นติดดอยเดิม)
                if sym in excluded_symbols:
                    continue

                # [Case A] เป็นหุ้นที่บอทเปิดสถานะไว้แล้ว -> อัปเดต Max Price และ Trailing Stop
                if row["position_id"]:
                    pos_id = row["position_id"]
                    prev_max = float(row["max_price_reached"] or mkt_price)
                    prev_trailing = float(row["trailing_stop_loss"]) if row["trailing_stop_loss"] else None
                    initial_sl = float(row["initial_stop_loss"])

                    new_max = max(prev_max, mkt_price)
                    new_trailing = prev_trailing

                    # 1. ตรวจสอบ Breakeven Stop
                    be_cfg = config.get("risk_management", {}).get("breakeven_stop", {})
                    be_enabled = be_cfg.get("enabled", True)
                    be_trigger_pct = float(be_cfg.get("trigger_profit_pct", 2.0))
                    be_buffer_pct = float(be_cfg.get("buffer_pct", 0.3))

                    entry_p = float(row["average_price"])
                    max_gain_pct = ((new_max - entry_p) / entry_p) * 100.0 if entry_p > 0 else 0.0

                    potential_candidates = []
                    if prev_trailing:
                        potential_candidates.append(prev_trailing)

                    # เมื่อราคาทำ New High กำไรถึงเกณฑ์ Breakeven ให้ยก SL มาล็อกเสมอตัวทันที
                    if be_enabled and max_gain_pct >= be_trigger_pct and entry_p > 0:
                        be_stop_price = round(entry_p * (1.0 + (be_buffer_pct / 100.0)), 4)
                        if be_stop_price > initial_sl:
                            potential_candidates.append(be_stop_price)

                    # 2. Dynamic Trailing Stop จาก ATR
                    if atr and new_max > entry_p:
                        mult = config.get("risk_management", {}).get("atr_multiplier", {}).get(q_type, 2.0)
                        potential_trailing = round(new_max - (atr * mult), 4)
                        if potential_trailing > initial_sl:
                            potential_candidates.append(potential_trailing)

                    if potential_candidates:
                        new_trailing = max(potential_candidates)

                    cur.execute("""
                        UPDATE bot_active_positions
                        SET max_price_reached = %s,
                            trailing_stop_loss = %s,
                            current_volume = %s,
                            updated_at = timezone('Asia/Bangkok', now())
                        WHERE id = %s;
                    """, (new_max, new_trailing, curr_vol, pos_id))

                # [Case B] มีหุ้นใหม่เข้ามาในพอร์ตและยังไม่มีใน bot_active_positions
                elif curr_vol > 0:
                    # ตรวจสอบก่อนว่าเป็นหุ้นที่ซื้อผ่านคำสั่ง Bot จริงหรือไม่
                    cur.execute("""
                        SELECT order_id FROM public.bot_orders
                        WHERE symbol = %s AND side = 'BUY' AND status = 'FILLED'
                        LIMIT 1;
                    """, (sym,))
                    bot_order = cur.fetchone()
                    if not bot_order:
                        # หุ้นนี้ซื้อแบบ Manual และผู้ใช้ยังไม่ได้สั่ง /adopt -> ข้าม ไม่ auto-insert
                        continue

                    init_sl = calculate_initial_stop_loss(avg_price, atr, q_type, config)
                    cur.execute("""
                        INSERT INTO bot_active_positions (
                            symbol, entry_date, entry_price, entry_atr14,
                            initial_stop_loss, max_price_reached, current_volume,
                            is_managed_by_bot, is_adopted, status
                        ) VALUES (
                            %s, CURRENT_DATE, %s, %s,
                            %s, %s, %s,
                            TRUE, FALSE, 'OPEN'
                        )
                        ON CONFLICT (symbol) WHERE (status = 'OPEN') DO UPDATE
                        SET current_volume = EXCLUDED.current_volume,
                            updated_at = timezone('Asia/Bangkok', now());
                    """, (sym, avg_price, atr, init_sl, mkt_price, curr_vol))
                    print(f"Registered new bot position: {sym} @ {avg_price} (SL: {init_sl})")

            conn.commit()
            print("Position tracker synced successfully.")
    finally:
        conn.close()

if __name__ == "__main__":
    sync_active_positions()