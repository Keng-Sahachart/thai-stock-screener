#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : aggregator.py
LOCATION    : core/signals/
OCCASION    : รันช่วงเย็นหลังตลาดปิด (EOD) หลังเสร็จสิ้น taskUpdate.py
              หรือรันช่วงเช้าก่อนตลาดเปิด (08:30 - 09:30 น.)
              สั่งรันผ่าน Telegram (/scan)
DESCRIPTION : Signal Aggregator
              ทำหน้าที่รวบรวมสัญญาณจากทุก Adapter, กรองหุ้นที่ถืออยู่แล้วหรืออยู่ใน Excluded List, คำนวณขนาดไม้และ Stop Loss แผนการเทรด
              , แล้วบันทึกรายการลงตาราง bot_trade_signals เพื่อส่งต่อให้บอทใน Phase ถัดไป:
             
              รวบรวมสัญญาณซื้อจากทุก Signal Engine, กรองหุ้นเดิม/หุ้นติดดอย,
              คำนวณ Position Sizing และ Stop Loss แผนการเทรด แล้วบันทึกลง DB
              รวบรวมสัญญาณ, ดึงเงินสดจริง, คัดกรองหุ้น และคำนวณขนาดไม้เริ่มต้น
==============================================================================
"""
import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

import risk_manager as rm
from core.signals.macd_signal import MacdMomentumSignal
from core.signals.hybrid_signal import HybridSelectionSignal

load_dotenv()
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "config", "bot_config.json")

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

def run_signal_aggregator():
    config = load_config()
    risk_cfg = config.get("risk_management", {})
    budget_cfg = config.get("budget_limits", {})
    excluded_symbols = set(config.get("excluded_symbols", []))

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # ดึงยอดเงินสดและพอร์ต
            cur.execute("""
                SELECT line_available, cash_balance 
                FROM public.account_info_history 
                WHERE is_disabled = FALSE 
                ORDER BY import_date DESC LIMIT 1;
            """)
            acc = cur.fetchone()
            line_available = float(acc["line_available"]) if acc else 0.0
            cash_balance = float(acc["cash_balance"]) if acc else 0.0

            cur.execute("SELECT COALESCE(SUM(market_value), 0.0) AS port_val FROM public.portfolio_stock;")
            port_val = float(cur.fetchone()["port_val"])
            total_equity = port_val + cash_balance if (port_val + cash_balance) > 0 else max(line_available, 100000.0)

            cur.execute("""
                SELECT symbol FROM public.portfolio_stock 
                WHERE imported_at = (SELECT MAX(imported_at) FROM public.portfolio_stock) 
                  AND current_volume > 0;
            """)
            owned_symbols = {r["symbol"] for r in cur.fetchall()}

        engines = [MacdMomentumSignal(), HybridSelectionSignal()]
        raw_signals = []
        for eng in engines:
            raw_signals.extend(eng.scan(conn))

        merged_signals = {}
        for s in raw_signals:
            sym = s["symbol"]
            if sym in excluded_symbols or sym in owned_symbols:
                continue
            if sym not in merged_signals or s["priority"] > merged_signals[sym]["priority"]:
                merged_signals[sym] = s

        with conn.cursor() as cur:
            target_dates = list({sig["trade_date"] for sig in merged_signals.values()})
            if target_dates:
                cur.execute("DELETE FROM public.bot_trade_signals WHERE trade_date = ANY(%s) AND status = 'PENDING';", (target_dates,))

            saved = 0
            for sym, sig in merged_signals.items():
                p = sig["trigger_price"]
                shares, sl = rm.calculate_position_size(
                    symbol=sym,
                    entry_price=p,
                    atr14=sig["atr14"],
                    total_equity=total_equity,
                    available_cash=line_available,
                    risk_percent=risk_cfg.get("risk_per_trade_pct", 0.01),
                    max_amount_per_trade=budget_cfg.get("max_amount_per_trade_thb", 500.0),
                    max_shares_per_trade=budget_cfg.get("max_shares_per_trade", 200),
                    fee_buffer_pct=budget_cfg.get("fee_buffer_pct", 0.0025)
                )

                # ถ้าเงินสดไม่พอขั้นต่ำ 100 หุ้น ให้ตั้ง Default ไว้ที่ 100 เพื่อให้เห็นบนหน้าจอ
                recommended = shares if shares >= 100 else 100

                cur.execute("""
                    INSERT INTO public.bot_trade_signals (
                        symbol, trade_date, signal_type, signal_source,
                        trigger_price, stop_loss_plan, recommended_shares,
                        reason, status, priority, expired_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'PENDING', %s, timezone('Asia/Bangkok', now()) + INTERVAL '1 day');
                """, (sym, sig["trade_date"], sig["signal_type"], sig["signal_source"], p, sl, recommended, sig["reason"], sig["priority"]))
                saved += 1

            conn.commit()
            print(f"✅ Aggregator บันทึกสัญญาณรออนุมัติเสร็จสิ้น {saved} ตัว")
    finally:
        conn.close()

if __name__ == "__main__":
    run_signal_aggregator()