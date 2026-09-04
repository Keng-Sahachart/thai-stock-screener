#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : telegram_app.py
LOCATION    : bot/
OCCASION    : รันเป็น Service ตลอดเวลา (Background Worker)
DESCRIPTION : และดักฟัง Event จากปุ่มกด ,รับคำสั่งและตอบกลับผู้ใช้ผ่าน Telegram:
              /port        - แสดงสถานะหุ้นในพอร์ต, เงินสด, และอำนาจซื้อ
              /update_port - ซิงค์ยอดเงินสดล่าสุดจาก Settrade หลังเติมเงิน
              /scan            - สั่งสแกนหาจังหวะซื้อและส่งกราฟขออนุมัติใหม่
              /panic_close_all - [Emergency] ขายล้างทุกไม้ที่บอทดูแลทันที
              /close_pos <SYM> - สั่งขายปิดสถานะหุ้นรายตัวทันที
              /status (ดูสถานะบอท) 
==============================================================================
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes
from dotenv import load_dotenv

import sys
from pathlib import Path
# ถอยกลับไป 1 โฟลเดอร์เพื่อชี้ไปที่ root (thai-stock-screener)
sys.path.append(str(Path(__file__).resolve().parent.parent))
from bot.callback_handlers import handle_signal_callback
from execution.order_manager import place_sell_order
import update.update_Port_info as uport_info
from jobs.run_buy_scanner import scan_and_notify

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("posql_host", "localhost"),
        port=os.getenv("posql_port", "5432"),
        dbname=os.getenv("posql_db", "stocks"),
        user=os.getenv("posql_user", "postgres"),
        password=os.getenv("posql_password", "postgres")
    )

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 <b>Stock Trading Bot Controller</b>\n\n"
        "<b>คำสั่งทั่วไป:</b>\n"
        "• /port - ดูพอร์ตโฟลิโอและยอดเงินสดคงเหลือ\n"
        "• /update_port - ซิงค์ยอดเงินสดจาก Settrade ล่าสุด\n"
        "• /scan - สั่งสแกนหาจังหวะซื้อและส่งกราฟใหม่\n\n"
        "<b>คำสั่งควบคุมความปลอดภัย:</b>\n"
        "• /close_pos &lt;SYMBOL&gt; - ปิดสถานะหุ้นตัวที่ระบุทันที\n"
        "• /panic_close_all - 🚨 สั่งขายล้างทุกไม้ที่บอทถือทันที",
        parse_mode="HTML"
    )

async def cmd_update_port(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ กำลังเชื่อมต่อ Settrade API เพื่อดึงยอดเงินสดล่าสุด...")
    try:
        uport_info.main()
        
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT line_available, cash_balance, import_date 
                FROM public.account_info_history 
                WHERE is_disabled = FALSE 
                ORDER BY import_date DESC LIMIT 1;
            """)
            acc = cur.fetchone()
        conn.close()

        msg = (
            f"✅ <b>อัปเดตข้อมูลพอร์ตสำเร็จ</b>\n"
            f"• อำนาจซื้อ (Line Available): <code>{float(acc['line_available']):,.2f}</code> THB\n"
            f"• เงินสดคงเหลือ (Cash Balance): <code>{float(acc['cash_balance']):,.2f}</code> THB\n\n"
            f"👉 พิมพ์ /scan เพื่อคำนวณขนาดไม้และสแกนส่งคำสั่งใหม่"
        )
        await update.message.reply_text(msg, parse_mode="HTML")
    except Exception as e:
        await update.message.reply_text(f"❌ เกิดข้อผิดพลาดในการดึงข้อมูล: {e}")

async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔍 กำลังเริ่มกระบวนการสแกนและสร้างกราฟแจ้งเตือน...")
    try:
        await scan_and_notify()
    except Exception as e:
        await update.message.reply_text(f"❌ เกิดข้อผิดพลาดในการสแกน: {e}")

async def cmd_port(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT line_available, cash_balance 
                FROM public.account_info_history 
                WHERE is_disabled = FALSE 
                ORDER BY import_date DESC LIMIT 1;
            """)
            acc = cur.fetchone()
            line_avail = float(acc["line_available"]) if acc else 0.0
            cash_bal = float(acc["cash_balance"]) if acc else 0.0

            cur.execute("""
                SELECT b.symbol, b.current_volume, b.entry_price, b.initial_stop_loss,
                       COALESCE(p.close, b.entry_price) AS market_price,
                       ROUND(((COALESCE(p.close, b.entry_price) - b.entry_price) / b.entry_price * 100)::numeric, 2) AS pnl_pct,
                       b.is_managed_by_bot
                FROM public.bot_active_positions b
                LEFT JOIN (
                    SELECT symbol, close FROM public.stock_price_history
                    WHERE date = (SELECT MAX(date) FROM public.stock_price_history)
                ) p ON b.symbol = p.symbol
                WHERE b.status = 'OPEN';
            """)
            rows = cur.fetchall()

            msg = (
                f"💼 <b>พอร์ตโฟลิโอส่วนของบอท</b>\n"
                f"• อำนาจซื้อ (Line Available): <code>{line_avail:,.2f}</code> THB\n"
                f"• เงินสดคงเหลือ (Cash): <code>{cash_bal:,.2f}</code> THB\n"
                f"------------------------------------\n"
                f"📊 <b>รายการหุ้นที่ถือครอง:</b> {len(rows)} ตัว\n"
            )
            if not rows:
                msg += "<i>ไม่มีหุ้นที่บอทถือครองในขณะนี้</i>"
            else:
                num = 1
                for r in rows:
                  tag = "🤖 [BOT]" if r["is_managed_by_bot"] else "👤 [MANUAL]"
                  # sl_str = f"{float(r['initial_stop_loss']):.2f}" if r["initial_stop_loss"] else "N/A"
                  msg += (
                      f"\nNo.{num}: {tag} <b>{r['symbol']}</b> ({r['current_volume']:,} หุ้น)\n"
                      # f"• กำไร/ขาดทุน: <code>{float(r['percent_profit']):+.2f}%</code>\n"
                      # f"• Initial SL: <code>{sl_str}</code> THB\n"
                      f"• ทุน: <code>{float(r['entry_price']):.2f}</code> | ตลาด: <code>{float(r['market_price']):.2f}</code>\n"
                      f"• PnL: <code>{float(r['pnl_pct']):+.2f}%</code> | SL: <code>{float(r['initial_stop_loss']):.2f}</code>\n\n"
                  )
                  num += 1
            await update.message.reply_text(msg, parse_mode="HTML")
    finally:
        conn.close()

async def cmd_close_pos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("กรุณาระบุชื่อหุ้น เช่น <code>/close_pos THREL</code>", parse_mode="HTML")
        return

    sym = context.args[0].upper()
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM public.bot_active_positions WHERE symbol = %s AND status = 'OPEN';", (sym,))
            pos = cur.fetchone()

            if not pos:
                await update.message.reply_text(f"⚠️ ไม่พบหุ้น <b>{sym}</b> ในสถานะที่บอทดูแล", parse_mode="HTML")
                return

            vol = int(pos["current_volume"])
            exit_p = float(pos["entry_price"])  # ใช้ราคาอ้างอิงล่าสุด

        res = place_sell_order(symbol=sym, volume=vol, exit_price=exit_p, exit_reason="MANUAL_TELEGRAM_CLOSE")
        if res.get("success"):
            await update.message.reply_text(f"✅ สั่งปิดสถานะ <b>{sym}</b> จำนวน {vol:,} หุ้น เรียบร้อยแล้ว", parse_mode="HTML")
        else:
            await update.message.reply_text(f"❌ สั่งปิดสถานะล้มเหลว: {res.get('error')}")
    finally:
        conn.close()

async def cmd_panic_close_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🚨 <b>กำลังสั่งขายฉุกเฉินทุกไม้ที่บอทดูแล...</b>", parse_mode="HTML")
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM public.bot_active_positions WHERE status = 'OPEN';")
            positions = cur.fetchall()

            if not positions:
                await update.message.reply_text("ไม่มีไม้ที่ต้องปิดสถานะ")
                return

            closed_count = 0
            for pos in positions:
                sym = pos["symbol"]
                vol = int(pos["current_volume"])
                exit_p = float(pos["entry_price"])
                res = place_sell_order(symbol=sym, volume=vol, exit_price=exit_p, exit_reason="PANIC_CIRCUIT_BREAKER")
                if res.get("success"):
                    closed_count += 1

            await update.message.reply_text(f"🛑 <b>Panic Close สำเร็จ:</b> สั่งปิดไปทั้งหมด {closed_count}/{len(positions)} รายการ", parse_mode="HTML")
    finally:
        conn.close()

def main():
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("port", cmd_port))
    app.add_handler(CommandHandler("update_port", cmd_update_port))
    app.add_handler(CommandHandler("scan", cmd_scan))
    app.add_handler(CommandHandler("close_pos", cmd_close_pos))
    app.add_handler(CommandHandler("panic_close_all", cmd_panic_close_all))
    app.add_handler(CallbackQueryHandler(handle_signal_callback))

    print("🚀 Telegram Bot Service is running with Safety Controls...")
    app.run_polling()

if __name__ == "__main__":
    main()