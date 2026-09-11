#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : order_manager.py
LOCATION    : execution/
OCCASION    : ถูกเรียกใช้งานทันทีเมื่อมีการกด Approve บน Telegram หรือเมื่อ Trigger ฝั่งขายทำงาน
DESCRIPTION : ผู้จัดการวงจรชีวิตคำสั่งซื้อขาย (Order Lifecycle Manager):
              - ตรวจสอบโหมดระบบ (DRY_RUN vs LIVE) จาก config/bot_config.json
              - ทำ Pre-flight Check (ตรวจเงินสดคงเหลือและสถานะซ้ำซ้อน)
              - มอบหมายงานให้ dry_run_executor หรือ settrade_executor ดำเนินการ
==============================================================================
"""

import os
import json

import psycopg2
from execution.dry_run_executor import execute_dry_run_buy, execute_dry_run_sell
from execution.settrade_executor import execute_real_buy, execute_real_sell, cancel_real_order
from settrade_v2 import Investor
import initialApp as cfg
from psycopg2.extras import RealDictCursor
from core.audit_logger import log_event
from risk_manager import is_market_trading_time

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "bot_config.json")
ACCOUNT_NO = os.getenv("account_no")

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("posql_host", "localhost"),
        port=os.getenv("posql_port", "5432"),
        dbname=os.getenv("posql_db", "stocks"),
        user=os.getenv("posql_user", "postgres"),
        password=os.getenv("posql_password", "postgres")
    )

def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def check_daily_budget_limits(conn, symbol: str, volume: int, target_price: float, config: dict) -> tuple[bool, str]:
    """
    ตรวจสอบเพดานรายวัน (Daily Budget Limits):
    1. จำนวนไม้ซื้อต่อวัน (max_trades_per_day)
    2. ยอดเงินซื้อสะสมต่อวัน (max_amount_per_day_thb)
    """
    budget_cfg = config.get("budget_limits", {})
    max_trades = budget_cfg.get("max_trades_per_day", 3)
    max_amount_per_day = budget_cfg.get("max_amount_per_day_thb", 8000.0)
    fee_buffer = budget_cfg.get("fee_buffer_pct", 0.0025)

    current_cost = round(volume * target_price * (1.0 + fee_buffer), 2)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT COUNT(*) AS trade_count,
                   COALESCE(SUM(volume * COALESCE(executed_price, target_price) * (1.0 + %s)), 0.0) AS spent_today
            FROM public.bot_orders
            WHERE side = 'BUY'
              AND (created_at::date = CURRENT_DATE OR executed_at::date = CURRENT_DATE)
              AND status NOT IN ('REJECTED', 'CANCELLED');
        """, (fee_buffer,))
        row = cur.fetchone()

        trade_count = int(row["trade_count"]) if row else 0
        spent_today = float(row["spent_today"]) if row else 0.0

        if trade_count >= max_trades:
            msg = f"⚠️ วันนี้ส่งคำสั่งซื้อครบโควตาสูงสุดแล้ว ({trade_count}/{max_trades} ไม้)"
            return False, msg

        if (spent_today + current_cost) > max_amount_per_day:
            msg = (
                f"⚠️ ยอดซื้อสะสมเกินเพดานรายวัน!\n"
                f"• ใช้ไปแล้ววันนี้: {spent_today:,.2f} THB\n"
                f"• คำสั่งนี้: {current_cost:,.2f} THB (รวมเป็น {spent_today + current_cost:,.2f} THB)\n"
                f"• เพดานสูงสุด: {max_amount_per_day:,.2f} THB"
            )
            return False, msg

    return True, ""

def place_buy_order(signal_id: int, symbol: str, volume: int, target_price: float, stop_loss_plan: float, atr14: float = None, buy_ticks: int = None, is_fixed_price: bool = None) -> dict:
    """ตรวจสอบการตั้งค่าและเลือกช่องทางการยิงคำสั่งซื้อ พร้อมตรวจเพดานงบประมาณรายวัน"""
    config = load_config()
    is_dry_run = config.get("trading_mode", {}).get("dry_run", True)
    mode_label = "DRY_RUN" if is_dry_run else "LIVE"

    if buy_ticks is None:
        buy_ticks = config.get("tick_execution", {}).get("buy_ticks", 0)

    # 1. ตรวจสอบเพดานงบประมาณรายวันก่อนดำเนินการ
    conn = get_db_connection()
    try:
        ok, budget_err = check_daily_budget_limits(conn, symbol, volume, target_price, config)
        if not ok:
            log_event(
                event_type="REJECTED_BUDGET",
                message=f"ปฏิเสธคำสั่งซื้อ {symbol}: {budget_err}",
                symbol=symbol,
                level="WARNING",
                raw_payload={"volume": volume, "target_price": target_price},
                conn=conn
            )
            return {"success": False, "error": budget_err}
    finally:
        conn.close()

    # 2. ตรวจสอบเวลาเปิดทำการของตลาด (สำหรับ LIVE Mode)
    check_market = config.get("risk_management", {}).get("check_market_hours", True)
    if not is_dry_run and check_market:
        is_open, mkt_reason = is_market_trading_time()
        if not is_open:
            err_msg = f"ไม่สามารถส่งคำสั่งซื้อจริงได้: {mkt_reason}"
            print(f"[ORDER MGR REJECTED] {err_msg}")
            return {"success": False, "error": err_msg}

    # 3. ส่งคำสั่งตามโหมด
    if is_dry_run:
        print(f"[ORDER MGR] Processing BUY order for {symbol} in DRY_RUN mode")
        res = execute_dry_run_buy(signal_id, symbol, volume, target_price, stop_loss_plan, atr14, buy_ticks, is_fixed_price)
    else:
        print(f"[ORDER MGR] Processing BUY order for {symbol} in LIVE mode (Real Trade)")
        res = execute_real_buy(signal_id, symbol, volume, target_price, stop_loss_plan, atr14, buy_ticks, is_fixed_price)

    # 4. บันทึกผลลัพธ์ลง Audit Log
    actual_buy_price = res.get("buy_price", target_price) if res.get("buy_price") else target_price
    if res.get("success"):
        log_event(
            event_type="ORDER_BUY",
            message=f"ส่งคำสั่งซื้อ {symbol} สำเร็จ [{mode_label}] {volume:,} หุ้น @ {actual_buy_price:.2f} THB (Order #{res.get('broker_order_no')})",
            symbol=symbol,
            level="SUCCESS",
            raw_payload=res
        )
    else:
        log_event(
            event_type="ORDER_BUY_FAILED",
            message=f"ส่งคำสั่งซื้อ {symbol} ไม่สำเร็จ [{mode_label}]: {res.get('error')}",
            symbol=symbol,
            level="ERROR",
            raw_payload=res
        )

    return res

def place_sell_order(symbol: str, volume: int, exit_price: float, exit_reason: str, profit_ticks: int = None) -> dict:
    """ตรวจสอบการตั้งค่าและเลือกช่องทางการยิงคำสั่งขาย พร้อมบันทึก Audit Log"""
    config = load_config()
    is_dry_run = config.get("trading_mode", {}).get("dry_run", True)
    mode_label = "DRY_RUN" if is_dry_run else "LIVE"

    # 1. ป้องกันการส่งคำสั่งขายซ้ำ หากมี Order ขายค้างรอคิวอยู่ในตลาด
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT order_id, broker_order_no, status, volume 
                FROM public.bot_orders 
                WHERE symbol = %s AND side = 'SELL' AND status IN ('SENT', 'QUEUING', 'PARTIAL');
            """, (symbol,))
            pending_sell = cur.fetchone()
            if pending_sell:
                err_msg = f"มีคำสั่งขาย {symbol} ค้างรออยู่ในตลาดแล้ว (Order #{pending_sell['broker_order_no']} สถานะ {pending_sell['status']})"
                print(f"[ORDER MGR REJECTED] {err_msg}")
                return {"success": False, "error": err_msg}
    finally:
        conn.close()

    # 2. ตรวจสอบเวลาเปิดทำการของตลาด (สำหรับ LIVE Mode)
    check_market = config.get("risk_management", {}).get("check_market_hours", True)
    if not is_dry_run and check_market:
        is_open, mkt_reason = is_market_trading_time()
        if not is_open:
            err_msg = f"ไม่สามารถส่งคำสั่งขายจริงได้: {mkt_reason}"
            print(f"[ORDER MGR REJECTED] {err_msg}")
            return {"success": False, "error": err_msg}

    if profit_ticks is None:
        profit_ticks = config.get("tick_execution", {}).get("sell_profit_ticks", 1)

    if is_dry_run:
        print(f"[ORDER MGR] Processing SELL order for {symbol} in DRY_RUN mode ({exit_reason})")
        res = execute_dry_run_sell(symbol, volume, exit_price, exit_reason, profit_ticks)
    else:
        print(f"[ORDER MGR] Processing SELL order for {symbol} in LIVE mode ({exit_reason})")
        res = execute_real_sell(symbol, volume, exit_price, exit_reason, profit_ticks)

    actual_sell_price = res.get("executed_price", exit_price) if res.get("executed_price") else exit_price
    if res.get("success"):
        log_event(
            event_type="ORDER_SELL",
            message=f"ส่งคำสั่งขาย {symbol} สำเร็จ [{mode_label}] {volume:,} หุ้น @ {actual_sell_price:.2f} THB ({exit_reason})",
            symbol=symbol,
            level="INFO",
            raw_payload=res
        )
    else:
        log_event(
            event_type="ORDER_SELL_FAILED",
            message=f"ส่งคำสั่งขาย {symbol} ไม่สำเร็จ [{mode_label}]: {res.get('error')}",
            symbol=symbol,
            level="ERROR",
            raw_payload=res
        )

    return res

def cancel_order(order_identifier) -> dict:
    """
    ยกเลิกคำสั่งซื้อขาย:
    :param order_identifier: order_id (int/str) หรือ broker_order_no
    :return: dict (success, order_id, broker_order_no, error)
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            order_id = int(order_identifier) if str(order_identifier).isdigit() else None
            if order_id:
                cur.execute("SELECT * FROM public.bot_orders WHERE order_id = %s;", (order_id,))
            else:
                cur.execute("SELECT * FROM public.bot_orders WHERE broker_order_no = %s;", (str(order_identifier),))
            order = cur.fetchone()

            if not order:
                return {"success": False, "error": f"ไม่พบ Order หมายเลข {order_identifier}"}

            oid = order["order_id"]
            sym = order["symbol"]
            status = order["status"]
            b_no = order["broker_order_no"]
            side = order["side"]

            cancellable_statuses = ("SENT", "QUEUING", "PARTIAL", "OPEN")
            if status not in cancellable_statuses:
                return {
                    "success": False,
                    "error": f"Order #{oid} ({sym}) ไม่สามารถยกเลิกได้ (สถานะปัจจุบัน: {status})"
                }

            is_sim = str(b_no or "").startswith("SIM_") or (status == "DRY_RUN")
            already_cancelled_on_broker = False

            if not is_sim:
                real_res = cancel_real_order(b_no)
                if not real_res.get("success"):
                    # ตรวจสอบว่า Order นี้ถูกยกเลิกผ่าน Streaming / Broker ไปก่อนหน้านี้แล้วหรือไม่
                    err_str = str(real_res.get("error", ""))
                    try:
                        investor = Investor(**cfg.args_Investor)
                        equity = investor.Equity(account_no=ACCOUNT_NO)
                        ord_info = equity.get_order(order_no=str(b_no))
                        if ord_info:
                            st_code = str(ord_info.get("status", "")).upper()
                            sh_status = str(ord_info.get("showOrderStatus", "")).upper()
                            sh_meaning = str(ord_info.get("showOrderStatusMeaning", "")).upper()
                            canc_vol = int(ord_info.get("cancelled", 0) or 0)
                            bal_vol = int(ord_info.get("balance", 0) or 0)
                            match_vol = int(ord_info.get("matched", 0) or 0)
                            if (
                                "CANCEL" in sh_status
                                or "CANCEL" in sh_meaning
                                or st_code in ("C", "CS", "CX", "CR")
                                or st_code.startswith("C")
                                or (canc_vol > 0 and bal_vol == 0 and match_vol == 0)
                            ):
                                already_cancelled_on_broker = True
                    except Exception as chk_ex:
                        print(f"[CHECK ORDER STATUS FAILED] {chk_ex}")

                    if not already_cancelled_on_broker:
                        return {"success": False, "error": f"โบรกเกอร์ปฏิเสธการยกเลิก: {real_res.get('error')}"}

            cur.execute("""
                UPDATE public.bot_orders
                SET status = 'CANCELLED'
                WHERE order_id = %s;
            """, (oid,))

            if side == "BUY":
                cur.execute("""
                    UPDATE public.bot_active_positions
                    SET current_volume = current_volume - %s,
                        updated_at = timezone('Asia/Bangkok', now())
                    WHERE symbol = %s AND status = 'OPEN' AND current_volume > %s;
                """, (order["volume"], sym, order["volume"]))
                if cur.rowcount == 0:
                    cur.execute("""
                        DELETE FROM public.bot_active_positions 
                        WHERE symbol = %s AND status = 'OPEN' AND current_volume <= %s;
                    """, (sym, order["volume"]))

            conn.commit()

            log_event(
                event_type="ORDER_CANCELLED",
                message=f"ยกเลิก Order #{oid} ({sym} {order['volume']:,} หุ้น) สำเร็จ" + (" (ตรวจพบว่าถูกยกเลิกผ่านสตรีมมิ่งไปก่อนแล้ว)" if already_cancelled_on_broker else ""),
                symbol=sym,
                level="INFO",
                raw_payload={"order_id": oid, "broker_order_no": b_no, "already_cancelled": already_cancelled_on_broker}
            )

            return {
                "success": True,
                "order_id": oid,
                "symbol": sym,
                "broker_order_no": b_no,
                "already_cancelled": already_cancelled_on_broker
            }
    except Exception as e:
        conn.rollback()
        print(f"[CANCEL ORDER ERROR] {e}")
        return {"success": False, "error": str(e)}
    finally:
        conn.close()



