#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : settrade_executor.py
LOCATION    : execution/
OCCASION    : ถูกเรียกใช้งานโดย order_manager.py เมื่อระบบทำงานในโหมด dry_run: false
DESCRIPTION : ส่งคำสั่งซื้อ-ขายหลักทรัพย์จริงเข้าตลาดหลักทรัพย์ผ่าน Settrade Open API
==============================================================================
"""

import datetime
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from settrade_v2 import Investor
from dotenv import load_dotenv

import initialApp as cfg

load_dotenv()
ACCOUNT_NO = os.getenv("account_no")

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("posql_host", "localhost"),
        port=os.getenv("posql_port", "5432"),
        dbname=os.getenv("posql_db", "stocks"),
        user=os.getenv("posql_user", "postgres"),
        password=os.getenv("posql_password", "postgres")
    )

def execute_real_buy(signal_id: int, symbol: str, volume: int, target_price: float, stop_loss_plan: float, atr14: float = None):
    """ส่งคำสั่งซื้อจริงผ่าน Settrade Open API"""
    conn = get_db_connection()
    try:
        investor = Investor(**cfg.args_Investor)
        equity = investor.Equity(account_no=ACCOUNT_NO)

        # ส่งคำสั่ง Limit Buy เข้า Broker
        res = equity.place_order(
            symbol=symbol,
            side="BUY",
            position="OPEN",
            price_type="LIMIT",
            price=target_price,
            volume=volume,
            validity_type="DAY"
        )

        # current_date = datetime.now().strftime('%Y-%m-%d')
        # res = equity.place_order(
        #                     side= "Buy",
        #                     symbol= symbol,
        #                     trustee_id_type= "Local",
        #                     volume= volume,
        #                     qty_open= 0,
        #                     price= target_price,
        #                     price_type= "Limit",
        #                     validity_type= "Day",
        #                     bypass_warning= False,
        #                     valid_till_date= current_date,
        #                     pin= "190628"
        #                     )



        broker_order_no = str(res.get("orderNo", ""))

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO public.bot_orders (
                    signal_id, symbol, side, order_type, volume,
                    target_price, status, broker_order_no
                ) VALUES (
                    %s, %s, 'BUY', 'LIMIT', %s,
                    %s, 'SENT', %s
                ) RETURNING order_id;
            """, (signal_id, symbol, volume, target_price, broker_order_no))
            order_id = cur.fetchone()["order_id"]

            # เปิดรอรับสถานะไว้ใน bot_active_positions
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
                    current_volume = bot_active_positions.current_volume + EXCLUDED.current_volume;
            """, (symbol, target_price, atr14, stop_loss_plan, target_price, volume))

            conn.commit()
            return {"success": True, "order_id": order_id, "broker_order_no": broker_order_no, "status": "SENT"}

    except Exception as e:
        conn.rollback()
        print(f"[SETTRADE BUY ERROR] {e}")
        return {"success": False, "error": str(e)}
    finally:
        conn.close()

def execute_real_sell(symbol: str, volume: int, exit_price: float, exit_reason: str):
    """ส่งคำสั่งขายจริงผ่าน Settrade Open API"""
    conn = get_db_connection()
    try:
        investor = Investor(**cfg.args_Investor)
        equity = investor.Equity(account_no=ACCOUNT_NO)

        # ส่งคำสั่งขายที่ราคาตลาด (MP-MTL หรือ LIMIT ตามราคาปัจจุบัน)
        res = equity.place_order(
            symbol=symbol,
            side="SELL",
            position="CLOSE",
            price_type="LIMIT",
            price=exit_price,
            volume=volume,
            validity_type="DAY"
        )

        # current_date = datetime.now().strftime('%Y-%m-%d')
        # res = equity.place_order(
        #                     side= "Sell",
        #                     symbol= symbol,
        #                     trustee_id_type= "Local",
        #                     volume= volume,
        #                     qty_open= 0,
        #                     price= exit_price,
        #                     price_type= "Limit",
        #                     validity_type= "Day",
        #                     bypass_warning= False,
        #                     valid_till_date= current_date,
        #                     pin= "190628"
        #                     )

        broker_order_no = str(res.get("orderNo", ""))

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO public.bot_orders (
                    symbol, side, order_type, volume,
                    target_price, status, broker_order_no
                ) VALUES (
                    %s, 'SELL', 'LIMIT', %s,
                    %s, 'SENT', %s
                ) RETURNING order_id;
            """, (symbol, volume, exit_price, broker_order_no))
            order_id = cur.fetchone()["order_id"]

            cur.execute("""
                UPDATE public.bot_active_positions
                SET status = 'CLOSED',
                    closed_date = CURRENT_DATE,
                    closed_price = %s,
                    exit_reason = %s,
                    updated_at = timezone('Asia/Bangkok', now())
                WHERE symbol = %s AND status = 'OPEN';
            """, (exit_price, exit_reason, symbol))

            conn.commit()
            return {"success": True, "order_id": order_id, "broker_order_no": broker_order_no, "status": "SENT"}
    except Exception as e:
        conn.rollback()
        print(f"[SETTRADE SELL ERROR] {e}")
        return {"success": False, "error": str(e)}
    finally:
        conn.close()