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
              /scan        - สั่งสแกนหาจังหวะซื้อและส่งกราฟขออนุมัติใหม่
              /buy <SYM> [VOL] [PRICE] - คำสั่งซื้อหุ้นแบบกำหนดเองพร้อมปุ่มปรับราคา/โวลุ่ม
              /order       - แสดงรายการคำสั่งซื้อขายวันนี้ทั้งหมด
              /sync_order  - ซิงค์สถานะ Order ล่าสุดจาก Settrade ทันที
              /cancel <ID> - ขอยกเลิกคำสั่งซื้อขายที่ค้างในตลาด
              /close_pos <SYM> - สั่งขายปิดสถานะหุ้นรายตัวทันที
              /panic_close_all - [Emergency] ขายล้างทุกไม้ที่บอทดูแลทันที
              /status      - แสดงสถานะการทำงานของบอทและสวิตช์ควบคุมความปลอดภัย
              /help        - แสดงคู่มือการใช้งานคำสั่งทั้งหมด
==============================================================================
"""

import os
import sys
import time
import asyncio
import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, LinkPreviewOptions
from pathlib import Path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from dotenv import load_dotenv
load_dotenv(dotenv_path=ROOT_DIR / ".env")

from bot.callback_handlers import handle_signal_callback
from execution.order_manager import place_sell_order, cancel_order
from execution.settrade_executor import get_realtime_quote, recalculate_stop_loss
from core.tick_utils import adjust_price_by_ticks
import update.update_Port_info as uport_info
import update.updatePort as update_port
from jobs.run_buy_scanner import scan_and_notify
from jobs.sync_order_status import sync_live_orders

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("posql_host", "localhost"),
        port=os.getenv("posql_port", "5432"),
        dbname=os.getenv("posql_db", "stocks"),
        user=os.getenv("posql_user", "postgres"),
        password=os.getenv("posql_password", "postgres")
    )

def build_manual_buy_keyboard(symbol: str, shares: int, price: float):
    cost = round(shares * price * 1.0025, 2)
    p_up = adjust_price_by_ticks(price, 1)
    p_down = adjust_price_by_ticks(price, -1)

    keyboard = [
        [
            InlineKeyboardButton("🔻 ราคา", callback_data=f"mbuy_p:{symbol}:{shares}:{p_down}"),
            InlineKeyboardButton(f"💵 {price:.2f} บ.", callback_data="noop"),
            InlineKeyboardButton("🔺 ราคา", callback_data=f"mbuy_p:{symbol}:{shares}:{p_up}")
        ],
        [
            InlineKeyboardButton("➖ 100", callback_data=f"mbuy_v:{symbol}:{shares - 100}:{price}"),
            InlineKeyboardButton(f"📦 {shares:,} หุ้น", callback_data="noop"),
            InlineKeyboardButton("➕ 100", callback_data=f"mbuy_v:{symbol}:{shares + 100}:{price}")
        ],
        [
            InlineKeyboardButton(f"✅ ยืนยันซื้อ ({cost:,.0f} บ.)", callback_data=f"mbuy_conf:{symbol}:{shares}:{price}"),
            InlineKeyboardButton("❌ ยกเลิก", callback_data="mbuy_cancel")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

async def cmd_buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "📌 <b>วิธีใช้งานคำสั่ง /buy:</b>\n"
            "• <code>/buy THREL</code> (ดึงราคาตลาดสดให้อัตโนมัติ)\n"
            "• <code>/buy THREL 200</code> (ระบุ 200 หุ้น)\n"
            "• <code>/buy THREL 200 1.45</code> (ระบุ 200 หุ้น ที่ 1.45 บ.)",
            parse_mode="HTML"
        )
        return

    sym = context.args[0].upper()
    quote = get_realtime_quote(None, sym)
    last_p = quote.get("last", 0.0)
    best_offer = quote.get("offer", last_p)

    # 1. กำหนดราคาตั้งต้น
    if len(context.args) >= 3:
        try:
            target_price = float(context.args[2])
        except ValueError:
            target_price = best_offer if best_offer > 0 else 1.0
    else:
        target_price = best_offer if best_offer > 0 else (last_p if last_p > 0 else 1.0)

    # 2. กำหนดจำนวนหุ้นเริ่มต้น
    if len(context.args) >= 2:
        try:
            shares = int(context.args[1])
        except ValueError:
            shares = 100
    else:
        shares = 100

    shares = max(100, (shares // 100) * 100)
    sl = recalculate_stop_loss(target_price)
    cost = round(shares * target_price * 1.0025, 2)

    # 3. ดึงยอด Line Available ล่าสุด และตรวจสอบคำสั่งซื้อค้างรอคิว
    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT line_available FROM public.account_info_history WHERE is_disabled = FALSE ORDER BY import_date DESC LIMIT 1;")
        acc = cur.fetchone()
        line_avail = float(acc["line_available"]) if acc else 0.0

        cur.execute("""
            SELECT order_id, broker_order_no, status, volume, target_price 
            FROM public.bot_orders 
            WHERE symbol = %s AND side = 'BUY' AND status IN ('SENT', 'QUEUING', 'PARTIAL')
            ORDER BY order_id DESC;
        """, (sym,))
        pending_orders = cur.fetchall()
    conn.close()

    pending_warning = ""
    if pending_orders:
        p_lines = "\n".join([f"  • Ref: <code>{o['broker_order_no'] or o['order_id']}</code> ({o['volume']:,} หุ้น @ <code>{float(o['target_price']):.2f}</code> THB [{o['status']}])" for o in pending_orders])
        pending_warning = (
            f"\n\n⚠️ <b>คำเตือน: มีคำสั่งซื้อ {sym} ค้างรออยู่ในตลาดแล้ว {len(pending_orders)} รายการ:</b>\n"
            f"{p_lines}\n"
            f"👉 <i>หากต้องการส่งคำสั่งนี้เป็น <b>ไม้เพิ่ม</b> ให้กดปุ่มยืนยันซื้อด้านล่าง</i>"
        )

    caption = (
        f"🛒 <b>เตรียมส่งคำสั่งซื้อแบบกำหนดเอง: {sym}</b>\n"
        f"• ราคาตลาด: <code>{last_p:.2f}</code> THB (Bid: <code>{quote.get('bid', 0):.2f}</code> / Offer: <code>{best_offer:.2f}</code>)\n"
        f"• ราคาเสนอซื้อ: <code>{target_price:.2f}</code> THB\n"
        f"• Initial SL Plan: <code>{sl:.2f}</code> THB\n"
        f"• ยอดเงินที่ต้องใช้: <code>{cost:,.2f}</code> THB\n"
        f"• อำนาจซื้อคงเหลือ: <code>{line_avail:,.2f}</code> THB"
        f"{pending_warning}\n\n"
        f"<i>ปรับราคาหรือจำนวนหุ้นด้านล่างก่อนกดยืนยัน:</i>"
    )

    reply_markup = build_manual_buy_keyboard(sym, shares, target_price)
    await update.message.reply_text(caption, parse_mode="HTML", reply_markup=reply_markup)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 <b>Stock Trading Bot Controller</b>\n\n"
        "<b>📊 ข้อมูลพอร์ตและคำสั่ง:</b>\n"
        "• /status - แสดงสถานะบอทและสวิตช์ความปลอดภัย\n"
        "• /port - ดูพอร์ตโฟลิโอและยอดเงินสดคงเหลือ\n"
        "• /scan_port [SYM] - 📈 สแกนรายงานสัญญาณกราฟหุ้นในพอร์ต\n"
        "• /update_port - ซิงค์ยอดเงินสดจาก Settrade ล่าสุด\n"
        "• /order - ตรวจสอบสถานะคำสั่งซื้อขายวันนี้ทั้งหมด\n"
        "• /sync_order - ซิงค์สถานะ Order ล่าสุดจาก Settrade ทันที\n\n"
        "<b>🛒 การส่งและจัดการคำสั่ง:</b>\n"
        "• /scan - สั่งสแกนหาจังหวะซื้อและส่งกราฟใหม่\n"
        "• /buy &lt;SYM&gt; [VOL] [PRICE] - สั่งซื้อหุ้นแบบกำหนดเอง\n"
        "• /sell_monitor - 🔍 ตรวจสอบเงื่อนไขขายและ Stop Loss ทันที\n"
        "• /task_update - 🔄 รันอัปเดตราคา, Indicators และพอร์ตสิ้นวัน\n"
        "• /cancel &lt;ID&gt; - ขอยกเลิกคำสั่งที่รอคิวในตลาด\n\n"
        "<b>🛡️ การควบคุมความปลอดภัยและการตั้งค่า:</b>\n"
        "• /close_pos &lt;SYM&gt; - ปิดสถานะหุ้นตัวที่ระบุทันที\n"
        "• /panic_close_all - 🚨 สั่งขายล้างทุกไม้ที่บอทถือทันที\n"
        "• /exclude [add|del|list] &lt;SYM&gt; - 🔒 จัดการหุ้นที่ได้รับการยกเว้น\n"
        "• /notify_job [on|off] - 🔔 เปิด/ปิดแจ้งเตือน Background Jobs (Crontab)\n"
        "• /help - ดูคู่มือการใช้งานคำสั่งทั้งหมดอย่างละเอียด\n",
        parse_mode="HTML"
    )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/help - แสดงคู่มือการใช้งานคำสั่งทั้งหมด"""
    msg = (
        "📖 <b>คู่มือการใช้งาน Stock Trading Bot</b>\n\n"
        "📊 <b>ข้อมูลพอร์ตและการเงิน:</b>\n"
        "• /port - ดูพอร์ตจริง, พอร์ตจำลอง, กำไร/ขาดทุน และเงินสดคงเหลือ\n"
        "• /scan_port [SYM] - รายงานสัญญาณกราฟเทคนิคอลและสถานะหุ้นในพอร์ต (ระบุ SYM ได้)\n"
        "• /update_port - ซิงค์ยอดเงินสดล่าสุดจาก Settrade API\n"
        "• /status - ตรวจสอบโหมดเทรด (DRY_RUN/LIVE) และสวิตช์ความปลอดภัย\n"
        "• /order - ตรวจสอบรายการและสถานะคำสั่งซื้อขายของวันนี้ทั้งหมด\n"
        "• /sync_order - ซิงค์สถานะ Order กับ Settrade ทันที (ตรวจผลจับคู่/ยกเลิก)\n\n"
        "🛒 <b>การส่งและจัดการคำสั่ง:</b>\n"
        "• /scan - สั่งสแกนหาจังหวะซื้อและส่งกราฟขออนุมัติ\n"
        "• /buy &lt;SYM&gt; [VOL] [PRICE] - ส่งคำสั่งซื้อหุ้นแบบกำหนดเอง\n"
        "• /sell_monitor [force] - ตรวจสอบเงื่อนไขขายและ Stop Loss ของพอร์ตทันที\n"
        "• /task_update [force] - รันงานอัปเดตราคา Indicators สิ้นวัน และซิงค์ Trailing Stop\n"
        "• /cancel &lt;ORDER_ID&gt; - ขอยกเลิกคำสั่งซื้อขายที่รอคิวในตลาด\n\n"
        "🛡️ <b>การควบคุมความปลอดภัยและการตั้งค่า:</b>\n"
        "• /close_pos &lt;SYM&gt; - สั่งขายปิดสถานะหุ้นรายตัวทันที\n"
        "• /panic_close_all - 🚨 ขายล้างทุกไม้ที่บอทดูแลทันที\n"
        "• /exclude [add|del|list] &lt;SYM&gt; - เพิ่ม/ลบ/ดูหุ้นที่ยกเว้นใน config\n"
        "• /notify_job [on|off] - เปิด/ปิดการแจ้งเตือนรอบรัน Background Jobs (Crontab)\n"
    )
    await update.message.reply_text(msg, parse_mode="HTML")

async def cmd_order(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/order - แสดงรายการคำสั่งซื้อขายของวันนี้ทั้งหมด"""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT order_id, symbol, side, order_type, volume, target_price,
                       executed_price, status, broker_order_no, created_at, executed_at
                FROM public.bot_orders
                WHERE (created_at::date = CURRENT_DATE OR executed_at::date = CURRENT_DATE)
                ORDER BY order_id DESC;
            """)
            orders = cur.fetchall()

            if not orders:
                await update.message.reply_text("📋 <b>รายการคำสั่งวันนี้:</b>\n<i>ยังไม่มีคำสั่งซื้อขายในระบบวันนี้</i>", parse_mode="HTML")
                return

            status_icons = {
                "FILLED": "🟢 [จับคู่สำเร็จ]",
                "SENT": "🟡 [ส่งคำสั่งแล้ว]",
                "QUEUING": "🟡 [รอคิวในตลาด]",
                "PARTIAL": "🟠 [จับคู่บางส่วน]",
                "REJECTED": "🔴 [ถูกปฏิเสธ]",
                "CANCELLED": "⚪ [ยกเลิกแล้ว]",
                "EXPIRED": "⌛ [หมดอายุ]"
            }

            msg = f"📋 <b>รายการคำสั่งซื้อขายวันนี้ ({len(orders)} รายการ):</b>\n"
            cancellable_list = []

            for o in orders:
                st = o["status"]
                st_badge = status_icons.get(st, f"⚪ [{st}]")
                side_tag = "🟢 BUY" if o["side"] == "BUY" else "🔴 SELL"
                t_str = o["created_at"].strftime("%H:%M:%S") if o["created_at"] else ""
                exec_p_str = f" | Match: <code>{float(o['executed_price']):.2f}</code>" if o.get("executed_price") else ""
                b_no = o.get("broker_order_no") or "-"

                msg += (
                    f"\n• #{o['order_id']} ({t_str}) {st_badge}\n"
                    f"  {side_tag} <b>{o['symbol']}</b> {o['volume']:,} หุ้น @ <code>{float(o['target_price']):.2f}</code>{exec_p_str}\n"
                    f"  Ref: <code>{b_no}</code>\n"
                )

                if st in ("SENT", "QUEUING", "PARTIAL", "OPEN"):
                    cancellable_list.append(str(o["order_id"]))

            if cancellable_list:
                msg += f"\n👉 <i>ยกเลิกคำสั่งที่ค้างอยู่: พิมพ์ <code>/cancel &lt;ID&gt;</code> เช่น <code>/cancel {cancellable_list[0]}</code></i>"

            await update.message.reply_text(msg, parse_mode="HTML")
    finally:
        conn.close()

async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/cancel <ORDER_ID> - ขอยกเลิกคำสั่งซื้อขายที่ค้างในตลาด"""
    if not context.args:
        await update.message.reply_text(
            "📌 <b>วิธีใช้งานคำสั่ง /cancel:</b>\n"
            "• <code>/cancel 15</code> (ระบุเลข Order ID)\n"
            "• <code>/cancel SIM_BUY_...</code> (หรือระบุ Broker Order No)\n\n"
            "👉 พิมพ์ /order เพื่อดูรายการ Order ID ที่เปิดอยู่",
            parse_mode="HTML"
        )
        return

    order_target = context.args[0].strip()
    await update.message.reply_text(f"⏳ กำลังดำเนินการยกเลิกคำสั่ง {order_target}...")

    res = cancel_order(order_target)
    if res.get("success"):
        note = "\n<i>(ตรวจพบว่าคำสั่งนี้ถูกยกเลิกผ่าน Streaming/โบรกเกอร์ แล้ว ระบบได้อัปเดตสถานะเป็น CANCELLED เรียบร้อย)</i>" if res.get("already_cancelled") else ""
        await update.message.reply_text(
            f"✅ <b>ยกเลิกคำสั่งสำเร็จ!</b>\n"
            f"• Order ID: <code>#{res.get('order_id')}</code>\n"
            f"• หุ้น: <b>{res.get('symbol')}</b>\n"
            f"• Ref: <code>{res.get('broker_order_no')}</code>{note}",
            parse_mode="HTML"
        )
    else:
        await update.message.reply_text(
            f"❌ <b>ยกเลิกคำสั่งไม่สำเร็จ:</b>\n{res.get('error')}",
            parse_mode="HTML"
        )

async def cmd_update_port(update: Update, context: ContextTypes.DEFAULT_TYPE):
    '''/update_port - ซิงค์ยอดเงินสดล่าสุดจาก Settrade หลังเติมเงิน'''
    await update.message.reply_text("⏳ กำลังเชื่อมต่อ Settrade API เพื่อดึงยอดเงินสดล่าสุด...")
    try:
        uport_info.main()
        update_port.main()
        
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

async def cmd_sync_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/sync_order หรือ /sync_orders - ซิงค์สถานะ Order ล่าสุดจาก Settrade ทันที"""
    await update.message.reply_text("⏳ กำลังเชื่อมต่อ Settrade API เพื่อซิงค์สถานะ Order และตรวจสอบพอร์ต...")
    try:
        count = await sync_live_orders(bot=context.bot)
        if count == 0:
            await update.message.reply_text("ℹ️ ตรวจสอบเรียบร้อย: ไม่มี Order ค้างและพอร์ตสอดคล้องสมบูรณ์")
        else:
            await update.message.reply_text(f"✅ ซิงค์สถานะและตรวจสอบพอร์ตสำเร็จ ({count} รายการ)")
        await cmd_order(update, context)
    except Exception as e:
        await update.message.reply_text(f"❌ เกิดข้อผิดพลาดในการซิงค์สถานะ Order: {e}")

async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔍 กำลังเริ่มกระบวนการสแกนและสร้างกราฟแจ้งเตือน...")
    try:
        await scan_and_notify()
    except Exception as e:
        await update.message.reply_text(f"❌ เกิดข้อผิดพลาดในการสแกน: {e}")

async def cmd_scan_port(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/scan_port [SYM] - รายงานสัญญาณกราฟเทคนิคอลของหุ้นในพอร์ตเข้า Telegram"""
    sym = context.args[0].upper().strip() if context.args else None
    if sym:
        await update.message.reply_text(f"🔍 กำลังสร้างกราฟและวิเคราะห์สัญญาณเทคนิคอลของ <b>{sym}</b> ในพอร์ต...", parse_mode="HTML")
    else:
        await update.message.reply_text("🔍 กำลังเริ่มกระบวนการสแกนสัญญาณกราฟหุ้นทั้งหมดในพอร์ต...", parse_mode="HTML")

    try:
        from jobs.run_port_scanner import scan_portfolio_and_notify
        await scan_portfolio_and_notify(symbol=sym, bot=context.bot, chat_id=update.effective_chat.id)
    except Exception as e:
        await update.message.reply_text(f"❌ เกิดข้อผิดพลาดในการสแกนหุ้นในพอร์ต: {e}")

async def cmd_port(update: Update, context: ContextTypes.DEFAULT_TYPE):
    '''/port - แสดงสถานะหุ้นในพอร์ต, เงินสด, และอำนาจซื้อ'''
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 1. ดึงยอดเงินสดและอำนาจซื้อล่าสุด
            cur.execute("""
                SELECT line_available, cash_balance 
                FROM public.account_info_history 
                WHERE is_disabled = FALSE 
                ORDER BY import_date DESC LIMIT 1;
            """)
            acc = cur.fetchone()
            line_avail = float(acc["line_available"]) if acc else 0.0
            cash_bal = float(acc["cash_balance"]) if acc else 0.0

            # 2. ดึงพอร์ตจริงทั้งหมด (มี percent_profit, stop loss, และ trailing stop)
            cur.execute("""
                SELECT symbol, current_volume, average_price, market_price, 
                       percent_profit, initial_stop_loss, trailing_stop_loss, is_managed_by_bot
                FROM public.v_portfolio_with_signals
                WHERE current_volume > 0
                ORDER BY percent_profit DESC;
            """)
            real_rows = cur.fetchall()

            # 3. ดึงไม้จำลอง (DRY-RUN) หรือคำสั่งซื้อจริงที่กำลังรอคิว (PENDING)
            cur.execute("""
                SELECT b.symbol, b.current_volume, b.entry_price, b.initial_stop_loss, b.trailing_stop_loss,
                       COALESCE(p.close, b.entry_price) AS market_price,
                       ROUND(((COALESCE(p.close, b.entry_price) - b.entry_price) / b.entry_price * 100)::numeric, 2) AS pnl_pct,
                       o.broker_order_no, o.status AS order_status
                FROM public.bot_active_positions b
                LEFT JOIN (
                    SELECT symbol, close FROM public.stock_price_history
                    WHERE date = (SELECT MAX(date) FROM public.stock_price_history)
                ) p ON b.symbol = p.symbol
                LEFT JOIN LATERAL (
                    SELECT broker_order_no, status FROM public.bot_orders
                    WHERE symbol = b.symbol AND side = 'BUY'
                    ORDER BY order_id DESC LIMIT 1
                ) o ON TRUE
                WHERE b.status = 'OPEN'
                  AND b.symbol NOT IN (
                      SELECT symbol FROM public.portfolio_stock 
                      WHERE imported_at = (SELECT MAX(imported_at) FROM public.portfolio_stock) 
                        AND current_volume > 0
                  );
            """)
            extra_rows = cur.fetchall()

            header = (
                f"💼 <b>ภาพรวมสถานะการเงิน</b>\n"
                f"• อำนาจซื้อ (Line Available): <b>{line_avail:,.2f}</b> THB\n"
                f"• เงินสดคงเหลือ (Cash): <b>{cash_bal:,.2f}</b> THB\n"
                f"------------------------------------\n"
                f"📊 <b>พอร์ตจริงในบัญชี (Real Portfolio): {len(real_rows)}</b>\n"
            )

            messages_to_send = []
            current_msg = header

            if not real_rows:
                current_msg += "<i>ไม่มีหุ้นถือครองในพอร์ตจริง</i>\n"
            else:
                items_in_batch = 0
                for num, r in enumerate(real_rows, 1):
                    tag = "🤖 [BOT]" if r["is_managed_by_bot"] else "👤 [MANUAL]"
                    sl_str = f"{float(r['initial_stop_loss']):.2f}" if r["initial_stop_loss"] is not None else "N/A"
                    ts_str = f"{float(r['trailing_stop_loss']):.2f}" if r["trailing_stop_loss"] is not None else "-"
                    pnl = float(r["percent_profit"]) if r["percent_profit"] is not None else 0.0
                    sym_link = f"<a href='https://www.settrade.com/th/equities/quote/{r['symbol']}/overview'><b>{r['symbol']}</b></a>"
                    item = (
                        f"\nNo.{num}: {tag} {sym_link} ({r['current_volume']:,} หุ้น)\n"
                        f"• ทุน: {float(r['average_price']):.2f} | ตลาด: {float(r['market_price']):.2f}\n"
                        f"• กำไร/ขาดทุน: {pnl:+.2f}%\n"
                        f"• Initial SL: {sl_str} | Trailing SL: {ts_str}\n"
                    )
                    # ป้องกันข้อความยาวเกินลิมิต 4,096 ตัวอักษร หรือเกิน 100 formatting entities ของ Telegram (แบ่งเป็นข้อความละไม่เกิน 15 หุ้น)
                    if items_in_batch >= 15 or (len(current_msg) + len(item) > 3500):
                        messages_to_send.append(current_msg)
                        current_msg = f"📊 <b>พอร์ตจริงในบัญชี (ต่อ):</b>\n" + item
                        items_in_batch = 1
                    else:
                        current_msg += item
                        items_in_batch += 1

            if extra_rows:
                # แยก Pending Buy ในตลาด กับ Dry-Run
                pending_list = [x for x in extra_rows if x.get("order_status") in ("SENT", "QUEUING", "PARTIAL") and not str(x.get("broker_order_no") or "").startswith("SIM_")]
                dry_list = [x for x in extra_rows if x not in pending_list]

                if pending_list:
                    p_intro = "\n------------------------------------\n⏳ <b>คำสั่งซื้อรอจับคู่ในตลาด (Pending Buy Orders):</b>\n"
                    if len(current_msg) + len(p_intro) > 3800:
                        messages_to_send.append(current_msg)
                        current_msg = p_intro
                        items_in_batch = 0
                    else:
                        current_msg += p_intro

                    for d in pending_list:
                        sym_link = f"<a href='https://www.settrade.com/th/equities/quote/{d['symbol']}/overview'><b>{d['symbol']}</b></a>"
                        item = (
                            f"\n⏳ <b>{sym_link}</b> ({d['current_volume']:,} หุ้น) [สถานะ: {d.get('order_status')}]\n"
                            f"• ราคาเสนอซื้อ: {float(d['entry_price']):.2f} | ตลาด: {float(d['market_price']):.2f}\n"
                        )
                        if items_in_batch >= 15 or (len(current_msg) + len(item) > 3500):
                            messages_to_send.append(current_msg)
                            current_msg = f"⏳ <b>คำสั่งซื้อรอจับคู่ (ต่อ):</b>\n" + item
                            items_in_batch = 1
                        else:
                            current_msg += item
                            items_in_batch += 1

                if dry_list:
                    dry_intro = "\n------------------------------------\n🧪 <b>ไม้จำลองที่บอทถืออยู่ (Dry-Run Positions):</b>\n"
                    if len(current_msg) + len(dry_intro) > 3800:
                        messages_to_send.append(current_msg)
                        current_msg = dry_intro
                        items_in_batch = 0
                    else:
                        current_msg += dry_intro

                    for d in dry_list:
                        sl_str = f"{float(d['initial_stop_loss']):.2f}" if d["initial_stop_loss"] is not None else "N/A"
                        ts_str = f"{float(d['trailing_stop_loss']):.2f}" if d["trailing_stop_loss"] is not None else "-"
                        pnl = float(d["pnl_pct"]) if d["pnl_pct"] is not None else 0.0
                        sym_link = f"<a href='https://www.settrade.com/th/equities/quote/{d['symbol']}/overview'><b>{d['symbol']}</b></a>"
                        item = (
                            f"\n🤖 {sym_link} ({d['current_volume']:,} หุ้น)\n"
                            f"• ทุนจำลอง: {float(d['entry_price']):.2f} | ตลาด: {float(d['market_price']):.2f}\n"
                            f"• กำไร/ขาดทุน: {pnl:+.2f}%\n"
                            f"• Initial SL: {sl_str} | Trailing SL: {ts_str}\n"
                        )
                        if items_in_batch >= 15 or (len(current_msg) + len(item) > 3500):
                            messages_to_send.append(current_msg)
                            current_msg = f"🧪 <b>ไม้จำลองที่บอทถืออยู่ (ต่อ):</b>\n" + item
                            items_in_batch = 1
                        else:
                            current_msg += item
                            items_in_batch += 1

            if current_msg.strip():
                messages_to_send.append(current_msg)

            for m in messages_to_send:
                await update.message.reply_text(
                    m,
                    parse_mode="HTML",
                    link_preview_options=LinkPreviewOptions(is_disabled=True)
                )
    finally:
        conn.close()

async def cmd_close_pos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    '''
    /close_pos <SYM> - สั่งขายปิดสถานะหุ้นรายตัวทันที (ใช้ราคาจับคู่ล่าสุดเป็น Exit Price)'''
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


async def cmd_sell_monitor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/sell_monitor [force] - เรียก run_sell_monitor.py ตรวจสอบเงื่อนไขขายและ Stop Loss ของพอร์ต"""
    is_force = False
    if context.args and context.args[0].lower().strip() in ("force", "all", "true", "1", "บังคับ"):
        is_force = True

    await update.message.reply_text("🔍 <b>กำลังตรวจสอบเงื่อนไขขายและ Stop Loss ของพอร์ต (Sell Monitor)...</b>", parse_mode="HTML")
    try:
        from jobs.run_sell_monitor import check_and_execute_sells
        res = await check_and_execute_sells(ignore_market_hours=is_force)
        
        if not isinstance(res, dict):
            await update.message.reply_text("✅ รัน Sell Monitor เรียบร้อยแล้ว")
            return

        status = res.get("status")
        if status == "skipped":
            reason = res.get("reason", "ตลาดปิดทำการ")
            await update.message.reply_text(
                f"⏰ <b>ข้ามการตรวจสอบ:</b> {reason}\n"
                f"👉 <i>(หากต้องการบังคับตรวจสอบให้พิมพ์ <code>/sell_monitor force</code>)</i>",
                parse_mode="HTML"
            )
        elif res.get("triggers_count", 0) == 0:
            await update.message.reply_text(
                "🛡️ <b>ผลการตรวจ Sell Monitor:</b> พอร์ตปลอดภัย\n"
                "ไม่มีหุ้นใดที่หลุดเกณฑ์ความปลอดภัย หรือเข้าเงื่อนไข Stop Loss ในขณะนี้",
                parse_mode="HTML"
            )
        else:
            triggers_cnt = res.get("triggers_count", 0)
            sold_cnt = res.get("sold_count", 0)
            await update.message.reply_text(
                f"🚨 <b>ผลการตรวจ Sell Monitor:</b>\n"
                f"• พบหุ้นเข้าข่ายต้องขาย: <b>{triggers_cnt}</b> รายการ\n"
                f"• ส่งคำสั่งขายสำเร็จ: <b>{sold_cnt}</b> รายการ\n"
                f"<i>(ตรวจสอบรายละเอียดการสั่งขายในข้อความแจ้งเตือนด้านบน)</i>",
                parse_mode="HTML"
            )
    except Exception as e:
        await update.message.reply_text(f"❌ เกิดข้อผิดพลาดในการรัน Sell Monitor: {e}")


# Concurrency guard for taskUpdate.py
_is_task_update_running = False

async def cmd_task_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/task_update [force] - เรียก taskUpdate.py อัปเดตราคา, Indicators, สัญญาณ และซิงค์พอร์ตสิ้นวัน"""
    global _is_task_update_running

    if _is_task_update_running:
        await update.message.reply_text(
            "⚠️ <b>taskUpdate.py กำลังทำงานอยู่ในเบื้องหลัง</b>\n"
            "กรุณารอให้รอบปัจจุบันทำงานเสร็จสิ้นก่อนครับ",
            parse_mode="HTML"
        )
        return

    is_force = False
    if context.args and context.args[0].lower().strip() in ("force", "true", "1", "บังคับ"):
        is_force = True

    chat_id = update.effective_chat.id

    async def _run_task_update_bg(c_id: int, force_run: bool):
        global _is_task_update_running
        _is_task_update_running = True
        try:
            import taskUpdate
            start_time = time.time()
            res = await asyncio.to_thread(taskUpdate.main, force=force_run)
            elapsed = time.time() - start_time
            mins, secs = divmod(int(elapsed), 60)
            time_str = f"{mins} นาที {secs} วินาที" if mins > 0 else f"{secs} วินาที"

            if isinstance(res, dict) and res.get("status") == "skipped":
                reason = res.get("reason", "ข้ามการทำงาน")
                await context.bot.send_message(
                    chat_id=c_id,
                    text=f"ℹ️ <b>taskUpdate.py:</b> {reason}\n"
                         f"👉 <i>หากต้องการบังคับให้อัปเดต ให้พิมพ์ <code>/task_update force</code></i>",
                    parse_mode="HTML"
                )
            else:
                await context.bot.send_message(
                    chat_id=c_id,
                    text=f"✅ <b>taskUpdate.py ประมวลผลเสร็จสิ้นเรียบร้อยแล้ว!</b>\n"
                         f"• เวลาที่ใช้: <code>{time_str}</code>\n"
                         f"• อัปเดตราคา, Indicators v5, สัญญาณ และซิงค์พอร์ต EOD เรียบร้อย\n\n"
                         f"👉 พิมพ์ /scan เพื่อสแกนหาหุ้นรอบใหม่ หรือ /port เพื่อดูพอร์ตล่าสุด",
                    parse_mode="HTML"
                )
        except Exception as err:
            await context.bot.send_message(
                chat_id=c_id,
                text=f"❌ <b>taskUpdate.py เกิดข้อผิดพลาด:</b>\n<code>{err}</code>",
                parse_mode="HTML"
            )
        finally:
            _is_task_update_running = False

    await update.message.reply_text(
        "⏳ <b>เริ่มประมวลผล taskUpdate.py ในเบื้องหลังแล้ว</b>\n"
        "• ดึงและอัปเดตราคาหุ้นล่าสุด\n"
        "• คำนวณ Indicators & Signals (v5)\n"
        "• ซิงค์พอร์ตและคำนวณ Trailing Stop สิ้นวัน\n\n"
        "<i>(ระบบจะแจ้งเตือนเมื่อเสร็จสิ้น และคุณยังสามารถสั่งงานบอทคำสั่งอื่นต่อได้ตามปกติ)</i>",
        parse_mode="HTML"
    )

    asyncio.create_task(_run_task_update_bg(chat_id, is_force))


async def cmd_exclude(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/exclude [add|del|list] [SYM] - จัดการรายชื่อหุ้นที่ได้รับการยกเว้นใน config/bot_config.json"""
    from core.config_manager import (
        get_excluded_symbols,
        add_excluded_symbols,
        remove_excluded_symbols
    )

    args = [a.strip() for a in context.args if a.strip()]

    # กรณีไม่มี argument หรือพิมพ์ /exclude list
    if not args or (len(args) == 1 and args[0].lower() in ("list", "show", "ดู")):
        symbols = get_excluded_symbols()
        sym_str = ", ".join(symbols) if symbols else "(ไม่มีหุ้นที่ได้รับการยกเว้น)"
        msg = (
            f"🔒 <b>รายชื่อหุ้นที่ได้รับการยกเว้น (Excluded Symbols):</b>\n"
            f"• ทั้งหมด: <b>{len(symbols)}</b> ตัว\n"
            f"• รายชื่อ: <code>{sym_str}</code>\n\n"
            f"<i>หุ้นในกลุ่มนี้ ระบบจะไม่สร้างสัญญาณซื้อ และจะไม่สั่งขายอัตโนมัติ</i>\n\n"
            f"<b>วิธีใช้งานคำสั่ง:</b>\n"
            f"• เพิ่มหุ้น: <code>/exclude add &lt;SYM&gt;</code> (เช่น <code>/exclude add PTT</code>)\n"
            f"• ลบหุ้น: <code>/exclude del &lt;SYM&gt;</code> (เช่น <code>/exclude del PTT</code>)"
        )
        await update.message.reply_text(msg, parse_mode="HTML")
        return

    subcmd = args[0].lower()

    # กรณีเพิ่มหุ้น: /exclude add SYM1 SYM2 ... หรือ /exclude + SYM
    if subcmd in ("add", "+", "เพิ่ม"):
        target_syms = args[1:]
        if not target_syms:
            await update.message.reply_text("⚠️ กรุณาระบุชื่อหุ้นที่ต้องการเพิ่ม เช่น <code>/exclude add PTT</code>", parse_mode="HTML")
            return

        added, already_in, full_list = add_excluded_symbols(target_syms)
        reply_lines = []
        if added:
            reply_lines.append(f"✅ <b>เพิ่มหุ้นเข้า Excluded Symbols สำเร็จ:</b> <code>{', '.join(added)}</code>")
        if already_in:
            reply_lines.append(f"ℹ️ หุ้นที่อยู่ในรายชื่ออยู่แล้ว: <code>{', '.join(already_in)}</code>")
        reply_lines.append(f"\n🔒 <b>รายชื่อปัจจุบัน ({len(full_list)} ตัว):</b>\n<code>{', '.join(full_list)}</code>")
        await update.message.reply_text("\n".join(reply_lines), parse_mode="HTML")
        return

    # กรณีลบหุ้น: /exclude del SYM1 SYM2 ... หรือ /exclude remove หรือ /exclude - SYM
    if subcmd in ("del", "delete", "remove", "rm", "-", "ลบ"):
        target_syms = args[1:]
        if not target_syms:
            await update.message.reply_text("⚠️ กรุณาระบุชื่อหุ้นที่ต้องการลบ เช่น <code>/exclude del PTT</code>", parse_mode="HTML")
            return

        removed, not_found, full_list = remove_excluded_symbols(target_syms)
        reply_lines = []
        if removed:
            reply_lines.append(f"🗑️ <b>นำหุ้นออกจาก Excluded Symbols สำเร็จ:</b> <code>{', '.join(removed)}</code>")
        if not_found:
            reply_lines.append(f"⚠️ ไม่พบหุ้นในรายชื่อ: <code>{', '.join(not_found)}</code>")
        reply_lines.append(f"\n🔒 <b>รายชื่อคงเหลือ ({len(full_list)} ตัว):</b>\n<code>{', '.join(full_list)}</code>")
        await update.message.reply_text("\n".join(reply_lines), parse_mode="HTML")
        return

    # กรณีผู้ใช้พิมพ์ /exclude SYM โดยตรง
    candidate_sym = args[0].upper()
    current_symbols = get_excluded_symbols()
    current_set = set(s.upper() for s in current_symbols)

    if candidate_sym in current_set:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"🗑️ กดเพื่อนำ {candidate_sym} ออกจาก Excluded", callback_data=f"exclude_del:{candidate_sym}")]
        ])
        msg = (
            f"🔒 หุ้น <b>{candidate_sym}</b> อยู่ในรายชื่อ Excluded Symbols อยู่แล้ว\n\n"
            f"👉 กดปุ่มด้านล่างเพื่อนำออก หรือพิมพ์ <code>/exclude del {candidate_sym}</code>"
        )
        await update.message.reply_text(msg, parse_mode="HTML", reply_markup=keyboard)
    else:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"➕ กดเพื่อเพิ่ม {candidate_sym} เข้า Excluded", callback_data=f"exclude_add:{candidate_sym}")]
        ])
        msg = (
            f"ℹ️ หุ้น <b>{candidate_sym}</b> ยังไม่อยู่ในรายชื่อ Excluded Symbols\n\n"
            f"👉 กดปุ่มด้านล่างเพื่อเพิ่มเข้า หรือพิมพ์ <code>/exclude add {candidate_sym}</code>"
        )
        await update.message.reply_text(msg, parse_mode="HTML", reply_markup=keyboard)


async def cmd_exclude_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/exclude_add <SYM> - คำสั่งลัดเพิ่มหุ้นเข้า Excluded Symbols"""
    context.args = ["add"] + (context.args or [])
    await cmd_exclude(update, context)


async def cmd_exclude_del(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/exclude_del <SYM> - คำสั่งลัดนำหุ้นออกจาก Excluded Symbols"""
    context.args = ["del"] + (context.args or [])
    await cmd_exclude(update, context)


async def cmd_notify_job(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/notify_job [on|off] - เปิดหรือปิดการแจ้งเตือนสถานะเริ่มและสิ้นสุดของ Background Jobs"""
    from core.job_notifier import is_job_notification_enabled, set_job_notification_enabled

    if context.args:
        arg = context.args[0].lower().strip()
        if arg in ("on", "true", "1", "enable", "เปิด"):
            set_job_notification_enabled(True)
            await update.message.reply_text(
                "🔔 <b>เปิดการแจ้งเตือน Background Jobs เรียบร้อยแล้ว</b>\n"
                "(ระบบจะส่งข้อความแจ้งเตือนเมื่อ Job เริ่มทำงานและทำงานเสร็จสิ้น)",
                parse_mode="HTML"
            )
            return
        elif arg in ("off", "false", "0", "disable", "ปิด"):
            set_job_notification_enabled(False)
            await update.message.reply_text(
                "🔕 <b>ปิดการแจ้งเตือน Background Jobs เรียบร้อยแล้ว</b>\n"
                "(Jobs จะทำงานเงียบๆ ในเบื้องหลังโดยไม่ส่งข้อความแจ้งเตือน)",
                parse_mode="HTML"
            )
            return

    current_status = is_job_notification_enabled()
    st_text = "🟢 เปิดใช้งานอยู่ (ON)" if current_status else "🔴 ปิดใช้งานอยู่ (OFF)"
    next_action = "off" if current_status else "on"
    btn_text = "🔕 กดเพื่อปิดการแจ้งเตือน" if current_status else "🔔 กดเพื่อเปิดการแจ้งเตือน"

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(btn_text, callback_data=f"toggle_job_notify:{next_action}")]
    ])

    msg = (
        f"📢 <b>การตั้งค่าแจ้งเตือน Background Jobs (Crontab)</b>\n"
        f"• สถานะปัจจุบัน: <b>{st_text}</b>\n\n"
        f"<i>เมื่อเปิดใช้งาน ระบบจะแจ้งเตือนความคืบหน้าของ Job ทุกตัว:\n"
        f"• run_buy_scanner.py (สแกนหาจังหวะซื้อ)\n"
        f"• run_sell_monitor.py (ตรวจสอบเงื่อนไขขาย)\n"
        f"• sync_order_status.py (ซิงค์สถานะ Order กับ Settrade)\n"
        f"• taskUpdate.py (อัปเดตราคาและ Indicators สิ้นวัน)\n"
        f"• run_eod_sync.py (สรุปพอร์ตสิ้นวัน)</i>\n\n"
        f"👉 กดปุ่มด้านล่าง หรือพิมพ์ <code>/notify_job on</code> / <code>/notify_job off</code>"
    )
    await update.message.reply_text(msg, parse_mode="HTML", reply_markup=keyboard)


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    '''/status - แสดงสถานะการทำงานของบอทและสวิตช์ควบคุมความปลอดภัย'''
    from bot.callback_handlers import load_config
    from core.job_notifier import is_job_notification_enabled
    config = load_config()
    
    t_mode = config.get("trading_mode", {})
    mode_str = "🧪 DRY_RUN (จำลองคำสั่ง)" if t_mode.get("dry_run", True) else "🔴 LIVE (เงินจริง)"
    
    ctrl = config.get("auto_sell_controls", {})
    cut_loss_str = "🟢 เปิด (ON)" if ctrl.get("enable_auto_cut_loss") else "🔴 ปิด (OFF)"
    stop_loss_str = "🟢 เปิด (ON)" if ctrl.get("enable_auto_stop_loss") else "🔴 ปิด (OFF)"
    trail_str = "🟢 เปิด (ON)" if ctrl.get("enable_auto_trailing_stop") else "🔴 ปิด (OFF)"
    tech_str = "🟢 เปิด (ON)" if ctrl.get("enable_auto_technical_sell") else "🔴 ปิด (OFF)"

    job_notify_on = is_job_notification_enabled()
    job_notify_str = "🟢 เปิด (ON)" if job_notify_on else "🔴 ปิด (OFF)"
    next_action = "off" if job_notify_on else "on"
    btn_text = "🔕 ปิดแจ้งเตือน Jobs" if job_notify_on else "🔔 เปิดแจ้งเตือน Jobs"

    excluded_list = config.get("excluded_symbols", [])

    msg = (
        f"⚙️ <b>สถานะการทำงานของระบบบอท</b>\n"
        f"• โหมดเทรดปัจจุบัน: <b>{mode_str}</b>\n"
        f"• แจ้งเตือน Background Jobs: <b>{job_notify_str}</b>\n"
        f"------------------------------------\n"
        f"🛡️ <b>สถานะสวิตช์ขายอัตโนมัติ:</b>\n"
        f"• Hard Cut Loss (-10%): {cut_loss_str}\n"
        f"• Initial Stop Loss: {stop_loss_str}\n"
        f"• Trailing Stop Loss: {trail_str}\n"
        f"• Technical Sell Signal: {tech_str}\n"
        f"------------------------------------\n"
        f"🔒 <b>หุ้นที่ได้รับการยกเว้น ({len(excluded_list)} ตัว):</b>\n"
        f"<code>{', '.join(excluded_list)}</code>\n\n"
        f"👉 ปรับการแจ้งเตือน Jobs พิมพ์: <code>/notify_job [on|off]</code>"
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(btn_text, callback_data=f"toggle_job_notify:{next_action}")]
    ])
    await update.message.reply_text(msg, parse_mode="HTML", reply_markup=keyboard)

async def post_init(application):
    """ส่งข้อความแจ้งเตือนเมื่อบอทเริ่มทำงานใหม่"""
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if chat_id:
        try:
            await application.bot.send_message(
                chat_id=chat_id,
                text="🤖 <b>Stock Trading Bot Service Started</b>\nระบบพร้อมรับคำสั่งแล้ว พิมพ์ /help เพื่อดูคำสั่งทั้งหมด",
                parse_mode="HTML"
            )
        except Exception as e:
            print(f"[STARTUP NOTIFY ERROR] {e}")

def main():
    app = ApplicationBuilder().token(TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("usage", cmd_help))
    app.add_handler(CommandHandler("port", cmd_port))
    app.add_handler(CommandHandler("update_port", cmd_update_port))
    app.add_handler(CommandHandler("scan", cmd_scan))

    app.add_handler(CommandHandler("scan_port", cmd_scan_port))
    app.add_handler(CommandHandler("port_scan", cmd_scan_port))
    app.add_handler(CommandHandler("port_chart", cmd_scan_port))
    app.add_handler(CommandHandler("chart_port", cmd_scan_port))
    app.add_handler(CommandHandler("scanport", cmd_scan_port))

    app.add_handler(CommandHandler("buy", cmd_buy))
    app.add_handler(CommandHandler("order", cmd_order))
    app.add_handler(CommandHandler("orders", cmd_order))

    app.add_handler(CommandHandler("sync_order", cmd_sync_orders))
    app.add_handler(CommandHandler("sync_orders", cmd_sync_orders))
    app.add_handler(CommandHandler("update_order", cmd_sync_orders))
    app.add_handler(CommandHandler("update_orders", cmd_sync_orders))
    
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("close_pos", cmd_close_pos))
    app.add_handler(CommandHandler("panic_close_all", cmd_panic_close_all))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("notify_job", cmd_notify_job))
    app.add_handler(CommandHandler("job_notify", cmd_notify_job))
    app.add_handler(CommandHandler("sell_monitor", cmd_sell_monitor))
    app.add_handler(CommandHandler("check_sell", cmd_sell_monitor))
    app.add_handler(CommandHandler("monitor_sell", cmd_sell_monitor))
    app.add_handler(CommandHandler("task_update", cmd_task_update))
    app.add_handler(CommandHandler("update_data", cmd_task_update))
    app.add_handler(CommandHandler("run_task_update", cmd_task_update))
    app.add_handler(CommandHandler("exclude", cmd_exclude))
    app.add_handler(CommandHandler("exclude_sym", cmd_exclude))
    app.add_handler(CommandHandler("excluded_symbols", cmd_exclude))
    app.add_handler(CommandHandler("exclude_add", cmd_exclude_add))
    app.add_handler(CommandHandler("exclude_del", cmd_exclude_del))
    app.add_handler(CallbackQueryHandler(handle_signal_callback)) # ดักฟังปุ่ม Inline Keyboard ของสัญญาณซื้อ

    print("🚀 Telegram Bot Service is running with Safety Controls...")
    app.run_polling()

if __name__ == "__main__":
    main()