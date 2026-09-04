#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : callback_handlers.py
LOCATION    : bot/
OCCASION    : ทำงานเบื้องหลังเมื่อมีผู้ใช้กดปุ่ม Inline Keyboard บน Telegram
              เมื่อมีการกดปุ่มผ่าน Telegram โมดูลนี้จะอัปเดตสถานะของสัญญาณในตาราง bot_trade_signals และเตรียมคำสั่งส่งเข้า bot_orders:
DESCRIPTION : ตัวจัดการปุ่มกด Approve / Reject (bot/callback_handlers.py)
              จัดการ Event:
              - ปรับเพิ่ม/ลดจำนวนหุ้น (adj) พร้อมตรวจเงินสดคงเหลือสดๆ
              - ยืนยันการอนุมัติ (app) บันทึก Order ลง bot_orders
              - ปฏิเสธสัญญาณ (rej)
==============================================================================
"""

import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import Update
from telegram.ext import ContextTypes
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

def build_trade_keyboard(signal_id: int, current_shares: int, price: float):
    from jobs.run_buy_scanner import build_trade_keyboard as b_kbd
    return b_kbd(signal_id, current_shares, price)

async def handle_signal_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data.split(":")
    action = data[0]

    if action == "noop":
        await query.answer()
        return

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 1. การปรับจำนวนหุ้น (adj:signal_id:new_shares)
            if action == "adj":
                signal_id = int(data[1])
                target_shares = int(data[2])

                # กฎขั้นต่ำ: ห้ามต่ำกว่า 100 หุ้น
                if target_shares < 100:
                    await query.answer("⚠️ จำนวนหุ้นขั้นต่ำคือ 100 หุ้น", show_alert=True)
                    return

                config = load_config()
                max_shares = config.get("budget_limits", {}).get("max_shares_per_trade", 200)
                if target_shares > max_shares:
                    await query.answer(f"⚠️ เกินเพดานสูงสุด ({max_shares:,} หุ้น)", show_alert=True)
                    return

                cur.execute("SELECT trigger_price, symbol FROM public.bot_trade_signals WHERE id = %s;", (signal_id,))
                sig = cur.fetchone()
                p = float(sig["trigger_price"])
                est_cost = target_shares * p * 1.0025

                # เช็คกับ Line Available ล่าสุด
                cur.execute("SELECT line_available FROM public.account_info_history WHERE is_disabled = FALSE ORDER BY import_date DESC LIMIT 1;")
                acc = cur.fetchone()
                line_avail = float(acc["line_available"]) if acc else 0.0

                if est_cost > line_avail:
                    await query.answer(f"❌ เงินสดไม่พอ! ต้องการ {est_cost:,.0f} บ. (มี {line_avail:,.0f} บ.)", show_alert=True)
                    return

                await query.answer()
                new_kbd = build_trade_keyboard(signal_id, target_shares, p)
                await query.edit_message_reply_markup(reply_markup=new_kbd)

            # 2. กดอนุมัติการซื้อ (app:signal_id:shares)
            elif action == "app":
                signal_id = int(data[1])
                final_shares = int(data[2])

                cur.execute("""
                    SELECT s.*, i.atr14 
                    FROM public.bot_trade_signals s
                    LEFT JOIN public.mv_stock_indicators i 
                        ON s.symbol = i.symbol AND s.trade_date = i.trade_date
                    WHERE s.id = %s;
                """, (signal_id,))
                sig = cur.fetchone()

                if not sig or sig["status"] != "PENDING":
                    await query.answer("⚠️ สัญญาณนี้ไม่อยู่ในสถานะรอดำเนินการ", show_alert=True)
                    return

                p = float(sig["trigger_price"])
                sl = float(sig["stop_loss_plan"])
                atr = float(sig["atr14"]) if sig.get("atr14") else None

                # 1. อัปเดตสถานะสัญญาณเป็น APPROVED
                cur.execute("UPDATE public.bot_trade_signals SET status = 'APPROVED', recommended_shares = %s WHERE id = %s;", (final_shares, signal_id))
                conn.commit()

                # 2. ส่งคำสั่งผ่าน Order Manager (Dry-run หรือ Real Trade ตาม Config)
                from execution.order_manager import place_buy_order
                exec_result = place_buy_order(
                    signal_id=signal_id,
                    symbol=sig["symbol"],
                    volume=final_shares,
                    target_price=p,
                    stop_loss_plan=sl,
                    atr14=atr
                )

                if exec_result.get("success"):
                    mode_label = "DRY_RUN (จำลอง)" if "SIM_" in exec_result.get("broker_order_no", "") else "LIVE (ส่งจริง)"
                    await query.edit_message_caption(
                        caption=f"✅ <b>อนุมัติและเปิดสถานะสำเร็จ [{mode_label}]</b>\n"
                                f"• หุ้น: <b>{sig['symbol']}</b>\n"
                                f"• จำนวน: <code>{final_shares:,}</code> หุ้น\n"
                                f"• ราคาจับคู่: <code>{p:.2f}</code> THB\n"
                                f"• Initial SL: <code>{sl:.2f}</code> THB\n"
                                f"• Order No: <code>{exec_result.get('broker_order_no')}</code>\n\n"
                                f"<i>(ระบบเริ่มติดตามสถานะและคุม Stop Loss ใน bot_active_positions แล้ว)</i>",
                        parse_mode="HTML"
                    )
                else:
                    await query.edit_message_caption(
                        caption=f"❌ <b>ส่งคำสั่งซื้อล้มเหลว</b>\nสาเหตุ: {exec_result.get('error')}",
                        parse_mode="HTML"
                    )

            # 3. กดยกเลิก (rej:signal_id)
            elif action == "rej":
                signal_id = int(data[1])
                cur.execute("UPDATE public.bot_trade_signals SET status = 'REJECTED' WHERE id = %s;", (signal_id,))
                conn.commit()
                await query.edit_message_caption(
                    caption="❌ <b>ปฏิเสธสัญญาณเรียบร้อยแล้ว</b>",
                    parse_mode="HTML"
                )
    finally:
        conn.close()