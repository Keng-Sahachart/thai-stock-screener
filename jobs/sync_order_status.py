#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : sync_order_status.py
LOCATION    : jobs/
OCCASION    : รันอัตโนมัติทุกๆ 3 - 5 นาที ระหว่างตลาดเปิดทำการ
              และรันอีก 1 ครั้งหลังตลาดปิด (16:40 น.)
DESCRIPTION : ตรวจสอบสถานะคำสั่งซื้อและขายจริงจาก Settrade:
              - อัปเดตสถานะ FILLED, QUEUING, PARTIAL, EXPIRED, CANCELLED, REJECTED
              - ปรับปรุงต้นทุนจริง (executed_price) ใน bot_active_positions
              - ลบ Position ทิ้งหากคำสั่งซื้อ Expired หรือ Cancelled โดยไม่มีการ Match
              - ส่งแจ้งเตือนสรุปผลการจับคู่คำสั่งเข้า Telegram
==============================================================================
"""
import os
import asyncio
import json
from settrade_v2 import Investor
import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import Bot
from telegram.request import HTTPXRequest
from settrade_v2 import Investor
from dotenv import load_dotenv

import sys
from pathlib import Path

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# ถอยกลับไป 1 โฟลเดอร์เพื่อชี้ไปที่ root (thai-stock-screener)
sys.path.append(str(Path(__file__).resolve().parent.parent))

import time
import initialApp as cfg
from core.job_notifier import notify_job_start, notify_job_finish
load_dotenv()
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "bot_config.json")
ACCOUNT_NO = os.getenv("account_no")
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("posql_host", "localhost"),
        port=os.getenv("posql_port", "5432"),
        dbname=os.getenv("posql_db", "stocks"),
        user=os.getenv("posql_user", "postgres"),
        password=os.getenv("posql_password", "postgres")
    )

def revert_sell_position_to_open(cur, symbol: str, reason: str) -> bool:
    """
    คืนสถานะ Position ฝั่งขายที่ถูกปิด (CLOSED) ให้กลับเป็น OPEN อย่างปลอดภัย
    โดยป้องกัน Duplicate Key ใน idx_bot_active_positions_open อย่างรัดกุม:
    1. หากมีสถานะ OPEN อยู่แล้ว จะไม่ทำอะไร (เพื่อป้องกัน duplicate symbol ใน OPEN)
    2. จะ Revert เฉพาะแถวที่เป็น MAX(id) ของ symbol นั้นเท่านั้น (ไม่ Revert ทุกแถว)
    3. ตรวจสอบว่ายังมีหุ้นเหลืออยู่ใน portfolio_stock ล่าสุดจริง
    """
    cur.execute("""
        UPDATE public.bot_active_positions
        SET status = 'OPEN',
            closed_date = NULL,
            closed_price = NULL,
            exit_reason = %s,
            updated_at = timezone('Asia/Bangkok', now())
        WHERE id = (
            SELECT MAX(id) FROM public.bot_active_positions
            WHERE symbol = %s AND status = 'CLOSED'
        )
        AND NOT EXISTS (
            SELECT 1 FROM public.bot_active_positions
            WHERE symbol = %s AND status = 'OPEN'
        )
        AND %s IN (
            SELECT symbol FROM public.portfolio_stock
            WHERE imported_at = (SELECT MAX(imported_at) FROM public.portfolio_stock)
              AND current_volume > 0
        );
    """, (reason, symbol, symbol, symbol))
    return cur.rowcount > 0

async def reconcile_orphaned_positions(cur, bot=None) -> dict:
    """
    ตรวจสอบและกวาดล้าง Orphaned/Ghost Positions ใน bot_active_positions:
    1. ไม้ซื้อที่ค้างสถานะ OPEN แต่:
       - ไม่มีหุ้นในพอร์ตจริง (portfolio_stock)
       - ไม่ใช่ไม้จำลอง (broker_order_no LIKE 'SIM_%')
       - ไม่มีคำสั่งซื้อที่กำลังรอคิว (SENT, QUEUING, PARTIAL) ของวันนี้
       - คำสั่งซื้อจริงล่าสุดสิ้นสุดแล้ว (EXPIRED, CANCELLED, REJECTED) หรือไม่มีคำสั่งซื้อที่ Match
       -> ลบออกจาก bot_active_positions ทันที
    2. ไม้ขายที่ถูกตั้งเป็น CLOSED แต่คำสั่งขายกลายเป็น EXPIRED/CANCELLED/REJECTED และหุ้นยังอยู่ในพอร์ตจริง
       -> Revert กลับเป็น OPEN เพื่อให้บอทดูแลต่อ
    """
    cleaned_count = 0
    reverted_count = 0

    # 1. ตรวจสอบ Ghost BUY positions ที่ไม่มีในพอร์ตจริง
    cur.execute("""
        SELECT b.id, b.symbol, b.current_volume, b.entry_price, b.created_at
        FROM public.bot_active_positions b
        WHERE b.status = 'OPEN'
          AND b.symbol NOT IN (
              SELECT symbol FROM public.portfolio_stock
              WHERE imported_at = (SELECT MAX(imported_at) FROM public.portfolio_stock)
                AND current_volume > 0
          );
    """)
    unbacked_positions = cur.fetchall()

    for unbacked in unbacked_positions:
        sym = unbacked["symbol"]
        pos_id = unbacked["id"]
        vol = unbacked["current_volume"]

        # ตรวจสอบว่ามีคำสั่งซื้อจริงที่กำลังรอคิว หรือเพิ่ง Match สำเร็จ (FILLED) ในช่วง 48 ชม. หรือไม่
        cur.execute("""
            SELECT order_id, broker_order_no, status
            FROM public.bot_orders
            WHERE symbol = %s
              AND side = 'BUY'
              AND (
                  status IN ('SENT', 'QUEUING', 'PARTIAL')
                  OR (status = 'FILLED' AND created_at >= (NOW() - INTERVAL '48 hours'))
              );
        """, (sym,))
        today_active = cur.fetchall()
        if today_active:
            continue  # มีคำสั่งรอ Match หรือเพิ่ง Match สำเร็จ (รอรอบซิงค์พอร์ต) -> ข้าม ไม่ลบเด็ดขาด

        # ตรวจสอบว่าเป็นไม้จำลองแท้หรือไม่ (SIM_%)
        cur.execute("""
            SELECT order_id, broker_order_no, status
            FROM public.bot_orders
            WHERE symbol = %s
              AND side = 'BUY'
              AND broker_order_no LIKE 'SIM_%%'
            ORDER BY order_id DESC LIMIT 1;
        """, (sym,))
        sim_order = cur.fetchone()
        if sim_order:
            continue  # เป็นไม้จำลองแท้ -> ข้าม ไม่ลบ

        # หากไม่มีคำสั่งรอคิววันนี้ และไม่ใช่ SIM แสดงว่าเป็น Ghost Position จากคำสั่งซื้อที่ Expired/Cancelled/Failed
        cur.execute("DELETE FROM public.bot_active_positions WHERE id = %s;", (pos_id,))
        cleaned_count += 1
        print(f"🧹 [RECONCILE] ลบ Ghost Position: {sym} ({vol:,} หุ้น) สำเร็จ (ไม่มีในพอร์ตและคำสั่งซื้อสิ้นสุดแล้ว)")
        if bot:
            try:
                await bot.send_message(
                    chat_id=CHAT_ID,
                    text=f"🧹 <b>[RECONCILE] ล้าง Position ตกค้างสำเร็จ</b>\n• หุ้น: <b>{sym}</b> ({vol:,} หุ้น)\n• สาเหตุ: คำสั่งซื้อหมดอายุ/ยกเลิกแล้ว และไม่มีในพอร์ตจริง",
                    parse_mode="HTML"
                )
            except Exception as e:
                print(f"⚠️ ส่งแจ้งเตือน Reconcile ลบ Position ไม่สำเร็จ: {e}")

    # 2. ตรวจสอบหุ้นที่ยังอยู่ในพอร์ตจริงแต่สถานะใน bot_active_positions ถูกปิดไปแล้วเพราะคำสั่งขาย Expired/Cancelled
    cur.execute("""
        SELECT b.id, b.symbol, b.current_volume
        FROM public.bot_active_positions b
        WHERE b.status = 'CLOSED'
          AND b.symbol IN (
              SELECT symbol FROM public.portfolio_stock
              WHERE imported_at = (SELECT MAX(imported_at) FROM public.portfolio_stock)
                AND current_volume > 0
          )
          AND b.id IN (
              SELECT MAX(id) FROM public.bot_active_positions GROUP BY symbol
          );
    """)
    closed_held = cur.fetchall()

    for ch in closed_held:
        sym = ch["symbol"]
        pos_id = ch["id"]
        # เช็คคำสั่งขายล่าสุด
        cur.execute("""
            SELECT order_id, status, side
            FROM public.bot_orders
            WHERE symbol = %s AND side = 'SELL'
            ORDER BY order_id DESC LIMIT 1;
        """, (sym,))
        last_sell = cur.fetchone()
        if last_sell and last_sell["status"] in ("EXPIRED", "CANCELLED", "REJECTED"):
            reverted = revert_sell_position_to_open(cur, sym, f"ORDER_{last_sell['status']}_REVERTED")
            if reverted:
                reverted_count += 1
                print(f"🔄 [RECONCILE] คืนสถานะ {sym} เป็น OPEN (คำสั่งขายล่าสุด {last_sell['status']} แต่ยังมีหุ้นในพอร์ต)")
                if bot:
                    try:
                        await bot.send_message(
                            chat_id=CHAT_ID,
                            text=f"🔄 <b>[RECONCILE] คืนสถานะพอร์ตเฝ้าระวัง</b>\n• หุ้น: <b>{sym}</b> (กลับสู่สถานะ OPEN)\n• สาเหตุ: คำสั่งขายเดิม {last_sell['status']} แต่ยังมีหุ้นในพอร์ตจริง",
                            parse_mode="HTML"
                        )
                    except Exception as e:
                        print(f"⚠️ ส่งแจ้งเตือน Reconcile คืนสถานะไม่สำเร็จ: {e}")

    return {"cleaned": cleaned_count, "reverted": reverted_count}

async def sync_live_orders(bot=None) -> int:
    conn = get_db_connection()
    if bot is None:
        bot = Bot(token=TOKEN, request=HTTPXRequest(connect_timeout=20.0, read_timeout=60.0))
    investor = Investor(**cfg.args_Investor)
    equity = investor.Equity(account_no=ACCOUNT_NO)

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 0. เคลียร์คำสั่งข้ามวันที่ค้างในสถานะรอคิว (คำสั่ง SET เป็น Day Order หมดอายุสิ้นวันเสมอ)
            cur.execute("""
                SELECT order_id, broker_order_no, symbol, side, volume
                FROM public.bot_orders
                WHERE status IN ('SENT', 'QUEUING')
                  AND created_at::date < CURRENT_DATE;
            """)
            past_orders = cur.fetchall()
            for p_ord in past_orders:
                p_sym = p_ord["symbol"]
                p_side = p_ord["side"]
                p_vol = p_ord["volume"]
                if p_side == "BUY":
                    cur.execute("""
                        UPDATE public.bot_active_positions
                        SET current_volume = current_volume - %s,
                            updated_at = timezone('Asia/Bangkok', now())
                        WHERE symbol = %s AND status = 'OPEN' AND current_volume > %s;
                    """, (p_vol, p_sym, p_vol))
                    if cur.rowcount == 0:
                        cur.execute("""
                            DELETE FROM public.bot_active_positions
                            WHERE symbol = %s AND status = 'OPEN' AND current_volume <= %s;
                        """, (p_sym, p_vol))
                elif p_side == "SELL":
                    reverted = revert_sell_position_to_open(cur, p_sym, "DAY_ORDER_EXPIRED_REVERTED")
                    if reverted:
                        print(f"🔄 [DAY_ORDER_EXPIRED] คืนสถานะ {p_sym} เป็น OPEN")

            if past_orders:
                cur.execute("""
                    UPDATE public.bot_orders
                    SET status = 'EXPIRED',
                        error_message = COALESCE(error_message, 'Day order expired at end of trade date')
                    WHERE status IN ('SENT', 'QUEUING')
                      AND created_at::date < CURRENT_DATE;
                """)
                print(f"🧹 เคลียร์คำสั่งข้ามวันที่หมดอายุไปแล้ว {len(past_orders)} รายการ -> EXPIRED")
            conn.commit()

            # ดึงคำสั่งเทรดจริงที่ยังไม่อยู่ในสถานะสิ้นสุด (Terminal State)
            cur.execute("""
                SELECT order_id, broker_order_no, symbol, side, volume, target_price, created_at
                FROM public.bot_orders 
                WHERE status IN ('SENT', 'QUEUING', 'PARTIAL') 
                  AND broker_order_no IS NOT NULL 
                  AND broker_order_no NOT LIKE 'SIM_%';
            """)
            active_orders = cur.fetchall()

            if not active_orders:
                print("ℹ️ ไม่มี Order ค้างที่ต้องตรวจสอบสถานะกับ Settrade")
            else:
                for ord_row in active_orders:
                    b_order_no = ord_row["broker_order_no"]
                    sym = ord_row["symbol"]
                    side = ord_row["side"]

                    try:
                        order_info = equity.get_order(order_no=b_order_no)
                    except Exception as ex:
                        print(f"❌ ดึงข้อมูล Order #{b_order_no} ไม่สำเร็จ: {ex}")
                        # หากเกิด Order not found และเป็นคำสั่งเก่า ให้ปรับเป็น EXPIRED และจัดการ Position
                        if "not found" in str(ex).lower():
                            cur.execute("""
                                UPDATE public.bot_orders 
                                SET status = 'EXPIRED', error_message = %s 
                                WHERE order_id = %s;
                            """, (f"Settrade: {ex}", ord_row["order_id"]))
                            if side == "BUY":
                                cur.execute("""
                                    UPDATE public.bot_active_positions
                                    SET current_volume = current_volume - %s,
                                        updated_at = timezone('Asia/Bangkok', now())
                                    WHERE symbol = %s AND status = 'OPEN' AND current_volume > %s;
                                """, (ord_row["volume"], sym, ord_row["volume"]))
                                if cur.rowcount == 0:
                                    cur.execute("""
                                        DELETE FROM public.bot_active_positions
                                        WHERE symbol = %s AND status = 'OPEN' AND current_volume <= %s;
                                    """, (sym, ord_row["volume"]))
                            elif side == "SELL":
                                revert_sell_position_to_open(cur, sym, "ORDER_NOT_FOUND_REVERTED")
                            conn.commit()
                        continue

                    if not order_info:
                        continue

                    status_code = str(order_info.get("status", "")).upper()
                    show_status = str(order_info.get("showOrderStatus", "")).upper()
                    status_meaning = str(order_info.get("showOrderStatusMeaning", "")).upper()
                    matched_vol = int(order_info.get("matched", 0) or 0)
                    balance_vol = int(order_info.get("balance", 0) or 0)
                    cancelled_vol = int(order_info.get("cancelled", 0) or 0)
                    total_vol = int(order_info.get("vol", 0) or 0)
                    match_amount = float(order_info.get("matchAmount", 0.0) or 0.0)
                    order_price = float(order_info.get("price", 0.0) or 0.0)
                    reject_reason = order_info.get("rejectReason")

                    # 1. จัดหมวดหมู่สถานะตาม Schema ของ bot_orders (ครอบคลุมสถานะ Settrade ทุกรูปแบบ)
                    new_status = "QUEUING"
                    if (
                        "CANCEL" in show_status
                        or "CANCEL" in status_meaning
                        or status_code in ("C", "CS", "CX", "CR")
                        or status_code.startswith("C")
                        or (cancelled_vol > 0 and balance_vol == 0 and matched_vol == 0)
                    ):
                        new_status = "CANCELLED"
                    elif (
                        "EXPIRE" in show_status
                        or "EXPIRE" in status_meaning
                        or status_code in ("E", "EXP")
                        or status_code.startswith("E")
                    ):
                        new_status = "EXPIRED"
                    elif (
                        "REJECT" in show_status
                        or "REJECT" in status_meaning
                        or status_code in ("R", "REJ")
                        or status_code.startswith("R")
                    ):
                        new_status = "REJECTED"
                    elif (
                        "MATCH" in show_status
                        or "MATCH" in status_meaning
                        or status_code in ("M", "MS")
                        or (matched_vol > 0 and balance_vol == 0)
                    ):
                        new_status = "FILLED"
                    elif matched_vol > 0 and balance_vol > 0:
                        new_status = "PARTIAL"
                    elif status_code in ("Q", "O", "PO") or "QUEU" in show_status or "OPEN" in show_status:
                        new_status = "QUEUING"

                    executed_price = round(match_amount / matched_vol, 4) if matched_vol > 0 else (order_price if new_status == "FILLED" else None)

                    # อัปเดตตาราง bot_orders
                    cur.execute("""
                        UPDATE public.bot_orders
                        SET status = %s,
                            executed_price = COALESCE(%s, executed_price),
                            executed_at = CASE WHEN %s IN ('FILLED', 'PARTIAL') THEN timezone('Asia/Bangkok', now()) ELSE executed_at END,
                            error_message = %s
                        WHERE order_id = %s;
                    """, (new_status, executed_price, new_status, reject_reason, ord_row["order_id"]))

                    # จัดการผลกระทบต่อ bot_active_positions
                    if side == "BUY":
                        if new_status == "FILLED":
                            cur.execute("""
                                UPDATE public.bot_active_positions
                                SET entry_price = %s,
                                    current_volume = %s,
                                    max_price_reached = GREATEST(max_price_reached, %s),
                                    updated_at = timezone('Asia/Bangkok', now())
                                WHERE symbol = %s AND status = 'OPEN';
                            """, (executed_price, matched_vol, executed_price, sym))
                            if cur.rowcount == 0:
                                cur.execute("""
                                    INSERT INTO public.bot_active_positions (
                                        symbol, entry_date, entry_price, entry_atr14,
                                        initial_stop_loss, max_price_reached, current_volume,
                                        is_managed_by_bot, status
                                    ) VALUES (
                                        %s, CURRENT_DATE, %s, NULL,
                                        %s, %s, %s,
                                        TRUE, 'OPEN'
                                    )
                                    ON CONFLICT (symbol) WHERE (status = 'OPEN') DO UPDATE
                                    SET entry_price = EXCLUDED.entry_price,
                                        current_volume = EXCLUDED.current_volume,
                                        max_price_reached = GREATEST(bot_active_positions.max_price_reached, EXCLUDED.max_price_reached),
                                        updated_at = timezone('Asia/Bangkok', now());
                                """, (sym, executed_price, round(executed_price * 0.92, 4), executed_price, matched_vol))

                            try:
                                import update.updatePort as uport
                                uport.UpdatePortfolio()
                            except Exception as up_err:
                                print(f"⚠️ ซิงค์พอร์ตหลัง Match ไม่สำเร็จ: {up_err}")

                            await bot.send_message(
                                chat_id=CHAT_ID,
                                text=f"🎉 <b>[BUY MATCHED] จับคู่คำสั่งซื้อสำเร็จ</b>\n• หุ้น: <b>{sym}</b>\n• จำนวน: <code>{matched_vol:,}</code> หุ้น\n• ราคาจริง: <code>{executed_price:.2f}</code> THB\n• Order No: <code>{b_order_no}</code>",
                                parse_mode="HTML"
                            )
                        elif new_status in ("EXPIRED", "CANCELLED", "REJECTED") and matched_vol == 0:
                            # ลด volume ลงอย่างปลอดภัยตาม volume ของออเดอร์ หรือลบออกถ้าหมด
                            cur.execute("""
                                UPDATE public.bot_active_positions
                                SET current_volume = current_volume - %s,
                                    updated_at = timezone('Asia/Bangkok', now())
                                WHERE symbol = %s AND status = 'OPEN' AND current_volume > %s;
                            """, (ord_row["volume"], sym, ord_row["volume"]))
                            if cur.rowcount == 0:
                                cur.execute("""
                                    DELETE FROM public.bot_active_positions 
                                    WHERE symbol = %s AND status = 'OPEN' AND current_volume <= %s;
                                """, (sym, ord_row["volume"]))

                            await bot.send_message(
                                chat_id=CHAT_ID,
                                text=f"⚠️ <b>[BUY {new_status}] คำสั่งซื้อไม่สำเร็จ</b>\n• หุ้น: <b>{sym}</b> ({new_status})\n• Order No: <code>{b_order_no}</code>\n• สาเหตุ: <code>{reject_reason or show_status or 'ยกเลิกคำสั่งแล้ว'}</code>\n<i>(ล้าง/ปรับปรุงยอดเฝ้าระวังใน bot_active_positions แล้ว)</i>",
                                parse_mode="HTML"
                            )

                    elif side == "SELL":
                        if new_status in ("EXPIRED", "CANCELLED", "REJECTED") and matched_vol == 0:
                            reverted = revert_sell_position_to_open(cur, sym, f"ORDER_{new_status}_REVERTED")
                            if bot and reverted:
                                await bot.send_message(
                                    chat_id=CHAT_ID,
                                    text=f"🔄 <b>[SELL {new_status}] การขายไม่สำเร็จ</b>\n• หุ้น: <b>{sym}</b> ({new_status})\n<i>(ดึงหุ้นกลับมาเฝ้าระวังในพอร์ตสถานะ OPEN ตามเดิม)</i>",
                                    parse_mode="HTML"
                                )

            # รัน Reconcile Position เสมอ (ทั้งกรณีมีและไม่มี active_orders)
            reconcile_stats = await reconcile_orphaned_positions(cur, bot=bot)
            conn.commit()

            total_actions = len(active_orders) + reconcile_stats["cleaned"] + reconcile_stats["reverted"]
            print(f"✅ ซิงค์สถานะ Order กับ Settrade เรียบร้อย (Actions: {total_actions})")
            return total_actions

    finally:
        conn.close()

async def main():
    start_t = time.time()
    await notify_job_start("Sync Order Status", "ซิงค์สถานะคำสั่งซื้อขายจริงกับ Settrade API")
    print("🔄 กำลังซิงค์สถานะคำสั่งซื้อขายจริงจาก Settrade...")
    try:
        count = await sync_live_orders()
        summary = f"ซิงค์สถานะเรียบร้อย ({count} รายการ)" if count > 0 else "ไม่มี Order ค้างและพอร์ตสอดคล้องสมบูรณ์"
        await notify_job_finish("Sync Order Status", elapsed_seconds=time.time() - start_t, summary=summary)
        print("✅ ซิงค์สถานะคำสั่งซื้อขายเรียบร้อยแล้ว")
    except Exception as e:
        await notify_job_finish("Sync Order Status", elapsed_seconds=time.time() - start_t, success=False, error=str(e))
        raise e

if __name__ == "__main__":
     asyncio.run(main())