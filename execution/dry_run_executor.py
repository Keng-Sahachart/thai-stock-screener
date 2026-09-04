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
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("posql_host", "localhost"),
        port=os.getenv("posql_port", "5432"),
        dbname=os.getenv("posql_db", "stocks"),
        user=os.getenv("posql_user", "postgres"),
        password=os.getenv("posql_password", "postgres")
    )

def execute_dry_run_buy(signal_id: int, symbol: str, volume: int, target_price: float, stop_loss_plan: float, atr14: float = None):
    """จำลองการซื้อ: สร้าง Order FILLED และเปิด Position ทันที"""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 1. บันทึกคำสั่งซื้อลง bot_orders สถานะ FILLED
            cur.execute("""
                INSERT INTO public.bot_orders (
                    signal_id, symbol, side, order_type, volume,
                    target_price, executed_price, status, broker_order_no, executed_at
                ) VALUES (
                    %s, %s, 'BUY', 'LIMIT', %s,
                    %s, %s, 'FILLED', 'SIM_BUY_' || to_char(now(), 'YYYYMMDDHH24MISS'), timezone('Asia/Bangkok', now())
                ) RETURNING order_id, broker_order_no;
            """, (signal_id, symbol, volume, target_price, target_price))
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
            """, (symbol, target_price, atr14, stop_loss_plan, target_price, volume))
            pos_res = cur.fetchone()

            conn.commit()
            print(f"[DRY RUN BUY] {symbol} {volume:,} หุ้น @ {target_price:.2f} THB | Order #{order_res['order_id']} | Pos #{pos_res['id']}")
            return {
                "success": True,
                "order_id": order_res["order_id"],
                "broker_order_no": order_res["broker_order_no"],
                "executed_price": target_price,
                "status": "FILLED"
            }
    except Exception as e:
        conn.rollback()
        print(f"[DRY RUN BUY ERROR] {e}")
        return {"success": False, "error": str(e)}
    finally:
        conn.close()

def execute_dry_run_sell(symbol: str, volume: int, exit_price: float, exit_reason: str):
    """จำลองการขาย: ปิดสถานะใน bot_active_positions และบันทึกคำสั่งขาย"""
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
            """, (symbol, volume, exit_price, exit_price))
            order_res = cur.fetchone()

            # 2. อัปเดตสถานะใน bot_active_positions เป็น CLOSED
            cur.execute("""
                UPDATE public.bot_active_positions
                SET status = 'CLOSED',
                    closed_date = CURRENT_DATE,
                    closed_price = %s,
                    exit_reason = %s,
                    updated_at = timezone('Asia/Bangkok', now())
                WHERE symbol = %s AND status = 'OPEN'
                RETURNING id;
            """, (exit_price, exit_reason, symbol))

            conn.commit()
            print(f"[DRY RUN SELL] {symbol} {volume:,} หุ้น @ {exit_price:.2f} THB ({exit_reason})")
            return {
                "success": True,
                "order_id": order_res["order_id"],
                "executed_price": exit_price,
                "status": "FILLED"
            }
    except Exception as e:
        conn.rollback()
        print(f"[DRY RUN SELL ERROR] {e}")
        return {"success": False, "error": str(e)}
    finally:
        conn.close()