#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : run_buy_scanner.py
LOCATION    : jobs/
OCCASION    : รันเป็น Job ประจำวัน รันช่วงเช้าก่อนตลาดเปิด (09:00 น.) หรือรันตอนค่ำหลังอัปเดตข้อมูลเสร็จ
DESCRIPTION : สคริปต์สแกนและยิงแจ้งเตือนพร้อมกราฟ (jobs/run_buy_scanner.py)
สแกนหาหุ้น, ส่งเข้าแชท Telegram ,สร้างกราฟเทคนิคอล, และส่งข้อความขออนุมัติพร้อมปุ่ม +/- หุ้น
==============================================================================
"""

import os
import asyncio
import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.request import HTTPXRequest 
from dotenv import load_dotenv

import sys
from pathlib import Path
# ถอยกลับไป 1 โฟลเดอร์เพื่อชี้ไปที่ root (thai-stock-screener)
sys.path.append(str(Path(__file__).resolve().parent.parent))

from core.signals.aggregator import run_signal_aggregator
from bot.chart_generator import generate_stock_chart

load_dotenv()

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

def build_trade_keyboard(signal_id: int, current_shares: int, price: float):
    cost = current_shares * price * 1.0025
    keyboard = [
        [
            InlineKeyboardButton("➖ 100", callback_data=f"adj:{signal_id}:{current_shares - 100}"),
            InlineKeyboardButton(f"📦 {current_shares:,} หุ้น ({cost:,.0f} บ.)", callback_data="noop"),
            InlineKeyboardButton("➕ 100", callback_data=f"adj:{signal_id}:{current_shares + 100}")
        ],
        [
            InlineKeyboardButton(f"✅ Approve ({cost:,.0f} บ.)", callback_data=f"app:{signal_id}:{current_shares}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"rej:{signal_id}")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

async def scan_and_notify():
    print("🔍 กำลังรัน Aggregator...")
    run_signal_aggregator()

    bot = Bot(token=TOKEN, request=HTTPXRequest(connect_timeout=20.0, read_timeout=60.0, write_timeout=60.0))
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT s.*, a.line_available 
                FROM public.bot_trade_signals s
                CROSS JOIN (
                    SELECT line_available FROM public.account_info_history 
                    WHERE is_disabled = FALSE ORDER BY import_date DESC LIMIT 1
                ) a
                WHERE s.status = 'PENDING' 
                  AND s.trade_date = (SELECT MAX(trade_date) FROM public.bot_trade_signals WHERE status = 'PENDING')
                ORDER BY s.priority DESC, s.id ASC;
            """)
            signals = cur.fetchall()

            if not signals:
                print("ไม่มีสัญญาณใหม่")
                return

            for sig in signals:
                sym = sig["symbol"]
                p = float(sig["trigger_price"])
                shares = int(sig["recommended_shares"])
                line_avail = float(sig["line_available"] or 0)
                cost = shares * p * 1.0025

                caption = (
                    f"🚨 <b>ตรวจพบสัญญาณซื้อ: {sym}</b>\n"
                    f"• ราคาปิด: <code>{p:.2f}</code> THB | SL Plan: <code>{float(sig['stop_loss_plan']):.2f}</code> THB\n"
                    f"• แหล่งสัญญาณ: <code>{sig['signal_source']}</code>\n"
                    f"• ประมาณการใช้เงิน: <code>{cost:,.2f}</code> THB\n"
                    f"• อำนาจซื้อคงเหลือ: <code>{line_avail:,.2f}</code> THB\n"
                    f"• สัญญาณ: {sig['reason']}\n\n"
                    f"<i>กดปุ่ม ➖ / ➕ เพื่อปรับจำนวนหุ้นก่อนกด Approve</i>"
                )

                reply_markup = build_trade_keyboard(sig["id"], shares, p)
                chart_buf = generate_stock_chart(sym)

                try:
                    if chart_buf:
                        await bot.send_photo(chat_id=CHAT_ID, photo=chart_buf, caption=caption, parse_mode="HTML", reply_markup=reply_markup)
                    else:
                        await bot.send_message(chat_id=CHAT_ID, text=caption, parse_mode="HTML", reply_markup=reply_markup)
                    await asyncio.sleep(2)
                except Exception as e:
                    print(f"Error sending {sym}: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    asyncio.run(scan_and_notify())