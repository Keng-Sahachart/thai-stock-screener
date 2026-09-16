#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : dry_run_executor.py
LOCATION    : execution/
OCCASION    : ถูกเรียกใช้งานโดย order_manager.py เมื่อระบบทำงานในโหมด dry_run: true
DESCRIPTION : จำลองการจับคู่คำสั่งซื้อ-ขายเสมือนจริง (Paper Trading):
              - บันทึกประวัติคำสั่งสถานะ FILLED ลงใน bot_orders
              - สร้างหรือปิดสถานะใน bot_active_positions โดยไม่ส่งคำสั่งเข้าตลาดจริง
==============================================================================
"""

import os
import json
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from dotenv import load_dotenv

from core.tick_utils import adjust_price_by_ticks

load_dotenv()
CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "bot_config.json"

def load_config():
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("posql_host", "localhost"),
        port=os.getenv("posql_port", "5432"),
        dbname=os.getenv("posql_db", "stocks"),
        user=os.getenv("posql_user", "postgres"),
        password=os.getenv("posql_password", "postgres")
    )

def execute_dry_run_buy(signal_id: int, symbol: str, volume: int, target_price: float, stop_loss_plan: float, atr14: float = None, buy_ticks: int = None, is_fixed_price: bool = None):
    """จำลองการซื้อ: ตรวจเงินสดคงเหลือ สร้าง Order FILLED และเปิด Position ทันที"""
    if buy_ticks is None:
        cfg_data = load_config()
        buy_ticks = cfg_data.get("tick_execution", {}).get("buy_ticks", 0)

    if is_fixed_price is None:
        is_fixed_price = (signal_id is None and target_price is not None and float(target_price) > 0)

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # คำนวณราคาซื้อ
            if is_fixed_price and target_price and float(target_price) > 0:
                buy_price = round(float(target_price), 2)
                if stop_loss_plan and float(stop_loss_plan) > 0:
                    final_sl = round(float(stop_loss_plan), 4)
                else:
                    final_sl = round(buy_price * 0.95, 4)
            else:
                cur.execute("SELECT close FROM public.stock_price_history WHERE symbol = %s ORDER BY date DESC LIMIT 1;", (symbol,))
                row = cur.fetchone()
                base_p = float(row["close"]) if row else (target_price or 1.0)
                buy_price = adjust_price_by_ticks(base_p, ticks=buy_ticks)
                if atr14 and atr14 > 0:
                    candidate_sl = buy_price - (atr14 * 2.0)
                    loss_pct = ((buy_price - candidate_sl) / buy_price) * 100.0
                    if loss_pct < 4.0:
                        candidate_sl = buy_price * 0.96
                    elif loss_pct > 8.0:
                        candidate_sl = buy_price * 0.92
                    final_sl = round(candidate_sl, 4)
                else:
                    final_sl = round(buy_price * 0.95, 4)

            # 0. ตรวจสอบ Line Available ล่าสุดจากตาราง account_info_history
            cur.execute("""
                SELECT line_available 
                FROM public.account_info_history 
                WHERE is_disabled = FALSE 
                ORDER BY import_date DESC LIMIT 1;
            """)
            acc = cur.fetchone()
            line_avail = float(acc["line_available"]) if acc else 0.0
            cost = round(volume * buy_price * 1.0025, 2)

            if cost > line_avail:
                diff = cost - line_avail
                err_msg = (
                    f"❌ ยอดเงินสดจำลองไม่พอซื้อ {symbol}!\n"
                    f"• ต้องการ: {cost:,.2f} THB\n"
                    f"• วงเงินซื้อคงเหลือ (Line): {line_avail:,.2f} THB\n"
                    f"• ขาดอีก: {diff:,.2f} THB"
                )
                print(f"[DRY RUN REJECTED] {err_msg}")
                return {"success": False, "error": err_msg}

            # 1. บันทึกคำสั่งซื้อลง bot_orders สถานะ FILLED
            cur.execute("""
                INSERT INTO public.bot_orders (
                    signal_id, symbol, side, order_type, volume,
                    target_price, executed_price, status, broker_order_no, executed_at
                ) VALUES (
                    %s, %s, 'BUY', 'LIMIT', %s,
                    %s, %s, 'FILLED', 'SIM_BUY_' || to_char(now(), 'YYYYMMDDHH24MISS'), timezone('Asia/Bangkok', now())
                ) RETURNING order_id, broker_order_no;
            """, (signal_id, symbol, volume, buy_price, buy_price))
            order_res = cur.fetchone()

            # 2. บันทึกเข้า bot_active_positions (Snapshot ไม้แรก)
            cur.execute("""
                INSERT INTO public.bot_active_positions (
                    symbol, entry_date, entry_price, entry_atr14,
                    initial_stop_loss, max_price_reached, current_volume,
                    is_managed_by_bot, status
                ) VALUES (
                    %s, CURRENT_DATE, %s, %s,
                    %s, %s, %s,
                    TRUE, 'OPEN'
                )
                ON CONFLICT (symbol) WHERE status = 'OPEN'
                DO UPDATE SET
                    current_volume = bot_active_positions.current_volume + EXCLUDED.current_volume,
                    updated_at = timezone('Asia/Bangkok', now())
                RETURNING id;
            """, (symbol, buy_price, atr14, final_sl, buy_price, volume))
            pos_res = cur.fetchone()

            conn.commit()
            print(f"[DRY RUN BUY] {symbol} {volume:,} หุ้น @ {buy_price:.2f} THB | Order #{order_res['order_id']} | Pos #{pos_res['id']}")
            return {
                "success": True,
                "order_id": order_res["order_id"],
                "broker_order_no": order_res["broker_order_no"],
                "executed_price": buy_price,
                "buy_price": buy_price,
                "stop_loss": final_sl,
                "status": "FILLED"
            }
    except Exception as e:
        conn.rollback()
        print(f"[DRY RUN BUY ERROR] {e}")
        return {"success": False, "error": str(e)}
    finally:
        conn.close()

def execute_dry_run_sell(symbol: str, volume: int, exit_price: float, exit_reason: str, profit_ticks: int = None):
    """จำลองการขาย: ปิดสถานะใน bot_active_positions และบันทึกคำสั่งขาย"""
    if profit_ticks is None:
        cfg_data = load_config()
        profit_ticks = cfg_data.get("tick_execution", {}).get("sell_profit_ticks", 1)

    if exit_reason in ("HARD_CUT_LOSS", "INITIAL_SL_HIT", "PANIC_CIRCUIT_BREAKER"):
        final_sell_price = round(exit_price, 2)
    else:
        final_sell_price = adjust_price_by_ticks(exit_price, ticks=profit_ticks)

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 1. บันทึกคำสั่งขาย
            cur.execute("""
                INSERT INTO public.bot_orders (
                    symbol, side, order_type, volume,
                    target_price, executed_price, status, broker_order_no, executed_at
                ) VALUES (
                    %s, 'SELL', 'MARKET', %s,
                    %s, %s, 'FILLED', 'SIM_SELL_' || to_char(now(), 'YYYYMMDDHH24MISS'), timezone('Asia/Bangkok', now())
                ) RETURNING order_id;
            """, (symbol, volume, final_sell_price, final_sell_price))
            order_res = cur.fetchone()

            # 2. ตรวจสอบจำนวนหุ้นใน position ถ้าขายบางส่วนให้ลดยอด ถ้าขายหมดให้ปิดสถานะ
            cur.execute("""
                SELECT id, current_volume FROM public.bot_active_positions
                WHERE symbol = %s AND status = 'OPEN'
                ORDER BY id DESC LIMIT 1;
            """, (symbol,))
            pos = cur.fetchone()

            if pos and pos.get("current_volume") and int(pos["current_volume"]) > volume:
                # ขายบางส่วน (Partial sell) -> ลดยอดหุ้นคงเหลือ ยังคงสถานะ OPEN
                cur.execute("""
                    UPDATE public.bot_active_positions
                    SET current_volume = current_volume - %s,
                        updated_at = timezone('Asia/Bangkok', now())
                    WHERE id = %s;
                """, (volume, pos["id"]))
            else:
                # ขายหมด (Full sell) -> ปิดสถานะ
                cur.execute("""
                    UPDATE public.bot_active_positions
                    SET status = 'CLOSED',
                        closed_date = CURRENT_DATE,
                        closed_price = %s,
                        exit_reason = %s,
                        updated_at = timezone('Asia/Bangkok', now())
                    WHERE symbol = %s AND status = 'OPEN';
                """, (final_sell_price, exit_reason, symbol))

            conn.commit()
            print(f"[DRY RUN SELL] {symbol} {volume:,} หุ้น @ {final_sell_price:.2f} THB ({exit_reason})")
            return {
                "success": True,
                "order_id": order_res["order_id"],
                "executed_price": final_sell_price,
                "status": "FILLED"
            }
    except Exception as e:
        conn.rollback()
        print(f"[DRY RUN SELL ERROR] {e}")
        return {"success": False, "error": str(e)}
    finally:
        conn.close()