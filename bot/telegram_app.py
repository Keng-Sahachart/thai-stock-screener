LAST_AI_EVALUATION = {}
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
              /auto_sell   - เปิด/ปิดสวิตช์ Auto Sell (Trailing Stop, Stop Loss, Cut Loss)
              /status      - แสดงสถานะการทำงานของบอทและสวิตช์ควบคุมความปลอดภัย
              /help        - แสดงคู่มือการใช้งานคำสั่งทั้งหมด
==============================================================================
"""

import os
import sys
import time
import asyncio
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, LinkPreviewOptions
from telegram.request import HTTPXRequest
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes
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

def get_tradingview_url(symbol: str) -> str:
    """ดึงและสร้าง URL TradingView จาก config/bot_config.json"""
    from bot.callback_handlers import load_config
    cfg = load_config()
    tmpl = cfg.get("tradingview_chart_url_template", "https://th.tradingview.com/chart/sYR6pQNG/?symbol=SET%3A{symbol}")
    return tmpl.replace("{symbol}", symbol).replace("[symbol]", symbol)

def build_port_card_keyboard(symbol: str, total_shares: int, mkt_price: float):
    """สร้างคีย์บอร์ดเริ่มต้นใต้การ์ดกราฟหุ้นในพอร์ต (/scan_port)"""
    tv_url = get_tradingview_url(symbol)
    settrade_url = f"https://www.settrade.com/th/equities/quote/{symbol}/overview"
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"🌐 Settrade: {symbol}", url=settrade_url),
            InlineKeyboardButton("📈 TradingView", url=tv_url)
        ],
        [
            InlineKeyboardButton(f"🔴 สั่งขายหุ้นนี้ ({total_shares:,} หุ้น)", callback_data=f"msell_open:{symbol}:{total_shares}:{mkt_price:.2f}")
        ]
    ])

def build_manual_sell_keyboard(symbol: str, shares: int, max_shares: int, price: float):
    """สร้าง Inline Keyboard สำหรับปรับราคาและจำนวนหุ้นเพื่อส่งคำสั่งขาย"""
    shares = max(100, min(shares, max_shares))
    p_up = adjust_price_by_ticks(price, 1)
    p_down = adjust_price_by_ticks(price, -1)
    shares_down = max(100, shares - 100)
    shares_up = min(max_shares, shares + 100)
    est_proceeds = round(shares * price * (1.0 - 0.0025), 2)

    # 50% shares: บังคับหารด้วย 100 ลงตัวเสมอตาม Board Lot
    half_shares = max(100, (max_shares // 2 // 100) * 100)

    tv_url = get_tradingview_url(symbol)
    settrade_url = f"https://www.settrade.com/th/equities/quote/{symbol}/overview"

    keyboard = [
        [
            InlineKeyboardButton(f"🌐 Settrade", url=settrade_url),
            InlineKeyboardButton("📈 TradingView", url=tv_url)
        ],
        [
            InlineKeyboardButton("🔻 ราคา", callback_data=f"msell_p:{symbol}:{shares}:{max_shares}:{p_down}"),
            InlineKeyboardButton(f"💵 {price:.2f} บ.", callback_data="noop"),
            InlineKeyboardButton("🔺 ราคา", callback_data=f"msell_p:{symbol}:{shares}:{max_shares}:{p_up}")
        ],
        [
            InlineKeyboardButton("➖ 100", callback_data=f"msell_v:{symbol}:{shares_down}:{max_shares}:{price}"),
            InlineKeyboardButton(f"📦 {shares:,} หุ้น", callback_data="noop"),
            InlineKeyboardButton("➕ 100", callback_data=f"msell_v:{symbol}:{shares_up}:{max_shares}:{price}")
        ]
    ]

    # ทางลัด 50% / 100% (50% แสดงเฉพาะเมื่อถือ >= 200 หุ้นและหาร 100 ลงตัว)
    if max_shares >= 200 and half_shares < max_shares:
        keyboard.append([
            InlineKeyboardButton(f"🌗 50% ({half_shares:,})", callback_data=f"msell_pct:{symbol}:{half_shares}:{max_shares}:{price}"),
            InlineKeyboardButton(f"🌕 100% ({max_shares:,})", callback_data=f"msell_pct:{symbol}:{max_shares}:{max_shares}:{price}")
        ])
    else:
        keyboard.append([
            InlineKeyboardButton(f"🌕 100% ({max_shares:,} หุ้น)", callback_data=f"msell_pct:{symbol}:{max_shares}:{max_shares}:{price}")
        ])

    keyboard.append([
        InlineKeyboardButton(f"🚨 ยืนยันขาย (~{est_proceeds:,.0f} บ.)", callback_data=f"msell_conf:{symbol}:{shares}:{max_shares}:{price}"),
        InlineKeyboardButton("❌ ยกเลิก", callback_data=f"msell_cancel:{symbol}:{max_shares}:{price}")
    ])

    return InlineKeyboardMarkup(keyboard)

async def cmd_buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    target_msg = update.effective_message

    if not context.args:
        text_help = (
            "📌 <b>วิธีใช้งานคำสั่ง /buy:</b>\n"
            "• <code>/buy THREL</code> (ดึงราคาตลาดสดให้อัตโนมัติ)\n"
            "• <code>/buy THREL 200</code> (ระบุ 200 หุ้น)\n"
            "• <code>/buy THREL 200 1.45</code> (ระบุ 200 หุ้น ที่ 1.45 บ.)"
        )
        if update.message:
            await update.message.reply_text(text_help, parse_mode="HTML")
        elif target_msg:
            await target_msg.reply_text(text_help, parse_mode="HTML")
        elif chat_id:
            await context.bot.send_message(chat_id=chat_id, text=text_help, parse_mode="HTML")
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
    if update.message:
        await update.message.reply_text(caption, parse_mode="HTML", reply_markup=reply_markup)
    elif target_msg:
        await target_msg.reply_text(caption, parse_mode="HTML", reply_markup=reply_markup)
    elif chat_id:
        await context.bot.send_message(chat_id=chat_id, text=caption, parse_mode="HTML", reply_markup=reply_markup)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 <b>Stock Trading Bot Controller</b>\n\n"
        "<b>📊 ข้อมูลพอร์ตและผลการดำเนินงาน:</b>\n"
        "• /port - ดูพอร์ตโฟลิโอ, หุ้นที่ถือ, กำไร/ขาดทุน และเงินสดคงเหลือ\n"
        "• /port_chart [SYM] - 📈 ดูกราฟสัญญาณเทคนิคอลของหุ้นในพอร์ต (ระบุ SYM ได้)\n"
        "• /history [N|SYM] - 📜 ดูประวัติการขายที่ปิดสถานะแล้ว (Closed Trades)\n"
        "• /summary [day|month|year] - 🏆 สรุปผลงาน Performance รายวัน รายเดือน รายปี\n\n"
        "<b>🔍 การสแกนหาหุ้นและเข้าซื้อ:</b>\n"
        "• /scan - 📈 สแกนหาหุ้นซื้อทางเทคนิคอลดั้งเดิม + ส่งการ์ดกราฟขออนุมัติ (รวดเร็ว)\n"
        "• /ai_scan [1|2|3] [SYM] - 🤖 ส่งให้ AI วิเคราะห์ 3 มิติ (1=สั้นกระชับ, 2=ปานกลาง, 3=ละเอียด)\n"
        "• /ai_credit - 💳 ตรวจสอบสถานะและโควตาคงเหลือของ Antigravity CLI บน Pi 5\n"
        "• /buy &lt;SYM&gt; [VOL] [PRICE] - 🛒 ส่งคำสั่งซื้อหุ้นแบบกำหนดเองพร้อมปุ่มปรับราคา\n\n"
        "<b>📦 จัดการคำสั่งซื้อขาย & พอร์ต:</b>\n"
        "• /order - ตรวจสอบสถานะคำสั่งซื้อขายวันนี้ทั้งหมด\n"
        "• /sync_order - ซิงค์สถานะ Order กับ Settrade ทันที (Match / Queuing / Cancel)\n"
        "• /cancel &lt;ID&gt; - ขอยกเลิกคำสั่งซื้อขายที่รอคิวในตลาด\n"
        "• /close_pos &lt;SYM&gt; - สั่งขายปิดสถานะหุ้นรายตัวทันที\n"
        "• /panic_close_all - 🚨 สั่งขายล้างทุกไม้ที่บอทดูแลทันที (Emergency)\n"
        "• /adopt &lt;SYM&gt; - 🛡️ รับเลี้ยงหุ้น Manual เข้าสู่ระบบ Trailing Stop และการคุ้มครอง\n"
        "• /release &lt;SYM&gt; - 👤 ปลดการดูแลหุ้น กลับไปเป็น Manual ตามเดิม\n\n"
        "<b>⚙️ ควบคุมความปลอดภัยและการบำรุงรักษา:</b>\n"
        "• /status - ตรวจสอบสถานะบอท (DRY_RUN/LIVE) และสวิตช์ความปลอดภัย\n"
        "• /auto_sell [on|off] - ตั้งค่า/เปิด/ปิดสวิตช์ Auto-Sell (Trailing Stop, SL, Cut Loss)\n"
        "• /sell_monitor - 🔍 ตรวจสอบเงื่อนไขขายและ Stop Loss ของพอร์ตทันที\n"
        "• /update_port - ซิงค์ยอดเงินสดล่าสุดจาก Settrade API (หลังเติมเงิน)\n"
        "• /task_update - 🔄 รันอัปเดตราคา, ข้อมูลงบการเงิน และ Indicators สิ้นวัน\n"
        "• /exclude [add|del|list] &lt;SYM&gt; - 🔒 จัดการรายชื่อหุ้นที่ได้รับการยกเว้น\n"
        "• /notify_job [on|off] - 🔔 เปิด/ปิดแจ้งเตือนรอบรัน Background Jobs (Cron)\n"
        "• /help - ดูคู่มือการใช้งานคำสั่งทั้งหมดอย่างละเอียด\n",
        parse_mode="HTML"
    )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/help - แสดงคู่มือการใช้งานคำสั่งทั้งหมด"""
    msg = (
        "📖 <b>คู่มือการใช้งาน Stock Trading Bot</b>\n\n"
        "📊 <b>ข้อมูลพอร์ตและผลการดำเนินงาน:</b>\n"
        "• /port - ดูพอร์ตจริง, พอร์ตจำลอง, กำไร/ขาดทุน และเงินสดคงเหลือ\n"
        "• /port_chart [SYM] - รายงานสัญญาณกราฟเทคนิคอลของหุ้นในพอร์ต (ระบุ SYM ได้)\n"
        "• /history [N|SYM] - 📜 ดูประวัติการขายที่ปิดสถานะแล้ว กำไร/ขาดทุน กี่บาท กี่%\n"
        "• /summary [day|month|year] - 🏆 สรุปผลงาน Performance รายวัน รายเดือน รายปี\n\n"
        "🔍 <b>การสแกนหาหุ้นและเข้าซื้อ:</b>\n"
        "• /scan - 📈 สแกนหาหุ้นซื้อทางเทคนิคอลดั้งเดิม + ส่งการ์ดกราฟขออนุมัติ (รวดเร็ว ไม่รอ AI)\n"
        "• /ai_scan [1|2|3] [SYM] - 🤖 ส่งให้ AI วิเคราะห์ 3 มิติ (1=สั้นมาก, 2=กระชับ, 3=ละเอียด)\n"
        "• /ai_credit - 💳 เช็คโควตาคงเหลือ, สถานะการเชื่อมต่อ และบัญชี Antigravity (Pi 5)\n"
        "• /buy &lt;SYM&gt; [VOL] [PRICE] - ส่งคำสั่งซื้อหุ้นแบบกำหนดเองพร้อมปุ่มปรับราคา\n\n"
        "📦 <b>จัดการคำสั่งซื้อขาย & พอร์ต:</b>\n"
        "• /order - ตรวจสอบรายการและสถานะคำสั่งซื้อขายของวันนี้ทั้งหมด\n"
        "• /sync_order - ซิงค์สถานะ Order กับ Settrade ทันที (ตรวจผลจับคู่/ยกเลิก)\n"
        "• /cancel &lt;ORDER_ID&gt; - ขอยกเลิกคำสั่งซื้อขายที่รอคิวในตลาด\n"
        "• /close_pos &lt;SYM&gt; - สั่งขายปิดสถานะหุ้นรายตัวทันที\n"
        "• /panic_close_all - 🚨 ขายล้างทุกไม้ที่บอทดูแลทันที\n"
        "• /adopt &lt;SYM&gt; - 🛡️ รับเลี้ยงหุ้น Manual เข้าสู่ระบบ Trailing Stop และคุ้มครองความปลอดภัย\n"
        "• /release &lt;SYM&gt; - 👤 ปลดการดูแลหุ้น กลับไปเป็น Manual ตามเดิมทันที\n\n"
        "⚙️ <b>ควบคุมความปลอดภัยและการบำรุงรักษา:</b>\n"
        "• /status - ตรวจสอบโหมดเทรด (DRY_RUN/LIVE) และสวิตช์ความปลอดภัย\n"
        "• /auto_sell [trailing|sl|cutloss|all] [on|off] - ตั้งค่า/เปิด/ปิดสวิตช์ Auto-Sell\n"
        "• /sell_monitor [force] - ตรวจสอบเงื่อนไขขายและ Stop Loss ของพอร์ตทันที\n"
        "• /update_port - ซิงค์ยอดเงินสดล่าสุดจาก Settrade API (หลังเติมเงิน)\n"
        "• /task_update [force] - รันงานอัปเดตราคา Indicators สิ้นวัน และซิงค์ Trailing Stop\n"
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



def build_summary_keyboard(current_mode: str = "overview") -> InlineKeyboardMarkup:
    """สร้าง Inline Keyboard สำหรับสลับมุมมองผลงาน Performance (ภาพรวม, รายวัน, รายเดือน, รายปี)"""
    row1 = [
        InlineKeyboardButton("📊 ภาพรวม" + (" ✅" if current_mode == "overview" else ""), callback_data="summary_nav:overview"),
        InlineKeyboardButton("📅 รายวัน" + (" ✅" if current_mode == "day" else ""), callback_data="summary_nav:day"),
    ]
    row2 = [
        InlineKeyboardButton("🗓️ รายเดือน" + (" ✅" if current_mode == "month" else ""), callback_data="summary_nav:month"),
        InlineKeyboardButton("📆 รายปี" + (" ✅" if current_mode == "year" else ""), callback_data="summary_nav:year"),
    ]
    row3 = [
        InlineKeyboardButton("📜 ดูประวัติการขายรายไม้ (/history)", callback_data="summary_nav:history_hint")
    ]
    return InlineKeyboardMarkup([row1, row2, row3])

def build_summary_overview_text(conn) -> str:
    """สร้างข้อความสรุปผลงานภาพรวม: ตลอดการใช้งาน, วันนี้, เดือนนี้, ปีนี้"""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN closed_price > entry_price THEN 1 END) as win,
                COUNT(CASE WHEN closed_price < entry_price THEN 1 END) as loss,
                COUNT(CASE WHEN closed_price = entry_price THEN 1 END) as be,
                COALESCE(ROUND(SUM((closed_price - entry_price) * current_volume), 2), 0) as pnl_thb,
                COALESCE(ROUND(AVG(((closed_price - entry_price) / entry_price) * 100)::numeric, 2), 0) as avg_pct,
                COALESCE(ROUND(MAX(((closed_price - entry_price) / entry_price) * 100)::numeric, 2), 0) as max_win_pct,
                COALESCE(ROUND(MIN(((closed_price - entry_price) / entry_price) * 100)::numeric, 2), 0) as max_loss_pct
            FROM public.bot_active_positions
            WHERE status = 'CLOSED';
        """)
        all_time = cur.fetchone()

        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN closed_price > entry_price THEN 1 END) as win,
                COUNT(CASE WHEN closed_price < entry_price THEN 1 END) as loss,
                COALESCE(ROUND(SUM((closed_price - entry_price) * current_volume), 2), 0) as pnl_thb,
                COALESCE(ROUND(AVG(((closed_price - entry_price) / entry_price) * 100)::numeric, 2), 0) as avg_pct
            FROM public.bot_active_positions
            WHERE status = 'CLOSED' AND closed_date = CURRENT_DATE;
        """)
        today = cur.fetchone()

        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN closed_price > entry_price THEN 1 END) as win,
                COUNT(CASE WHEN closed_price < entry_price THEN 1 END) as loss,
                COALESCE(ROUND(SUM((closed_price - entry_price) * current_volume), 2), 0) as pnl_thb,
                COALESCE(ROUND(AVG(((closed_price - entry_price) / entry_price) * 100)::numeric, 2), 0) as avg_pct
            FROM public.bot_active_positions
            WHERE status = 'CLOSED' AND TO_CHAR(closed_date, 'YYYY-MM') = TO_CHAR(CURRENT_DATE, 'YYYY-MM');
        """)
        month = cur.fetchone()

        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN closed_price > entry_price THEN 1 END) as win,
                COUNT(CASE WHEN closed_price < entry_price THEN 1 END) as loss,
                COALESCE(ROUND(SUM((closed_price - entry_price) * current_volume), 2), 0) as pnl_thb,
                COALESCE(ROUND(AVG(((closed_price - entry_price) / entry_price) * 100)::numeric, 2), 0) as avg_pct
            FROM public.bot_active_positions
            WHERE status = 'CLOSED' AND TO_CHAR(closed_date, 'YYYY') = TO_CHAR(CURRENT_DATE, 'YYYY');
        """)
        year = cur.fetchone()

        tot = all_time['total']
        if tot == 0:
            return "🏆 <b>สรุปผลงานการเทรดภาพรวม (Performance Summary)</b>\n\n<i>ยังไม่มีประวัติการขายที่ปิดสถานะในระบบ</i>"

        win_rate = (all_time['win'] / tot * 100) if tot > 0 else 0.0
        pnl_sign = "🟢" if all_time['pnl_thb'] > 0 else ("🔴" if all_time['pnl_thb'] < 0 else "⚪")
        m_sign = "🟢" if month['pnl_thb'] > 0 else ("🔴" if month['pnl_thb'] < 0 else "⚪")
        t_sign = "🟢" if today['pnl_thb'] > 0 else ("🔴" if today['pnl_thb'] < 0 else "⚪")
        y_sign = "🟢" if year['pnl_thb'] > 0 else ("🔴" if year['pnl_thb'] < 0 else "⚪")

        msg = (
            f"🏆 <b>สรุปผลงานการเทรดภาพรวม (Performance Summary)</b>\n"
            f"<i>(คำนวณจากสถานะไม้ที่ปิดการขายแล้วทั้งหมด)</i>\n"
            f"------------------------------------\n"
            f"🌟 <b>ผลงานตลอดการใช้งาน (All-Time):</b>\n"
            f"• จำนวนไม้ที่ปิดแล้ว: <b>{tot:,}</b> ไม้\n"
            f"• อัตราชนะ (Win Rate): <b>{win_rate:.1f}%</b> (🟢 {all_time['win']} | 🔴 {all_time['loss']} | ⚪ {all_time['be']})\n"
            f"• กำไรสุทธิรวม: {pnl_sign} <b>{float(all_time['pnl_thb']):+,.2f}</b> THB\n"
            f"• ผลตอบแทนเฉลี่ยต่อไม้: <code>{float(all_time['avg_pct']):+.2f}%</code>\n"
            f"• ไม้กำไรสูงสุด: <code>+{float(all_time['max_win_pct']):.2f}%</code> | ขาดทุนสูงสุด: <code>{float(all_time['max_loss_pct']):.2f}%</code>\n"
            f"------------------------------------\n"
            f"📅 <b>วันนี้ (Today):</b>\n"
            f"• ปิดไป: <b>{today['total']}</b> ไม้ (🟢 {today['win']} | 🔴 {today['loss']})\n"
            f"• กำไรสุทธิ: {t_sign} <b>{float(today['pnl_thb']):+,.2f}</b> THB (เฉลี่ย <code>{float(today['avg_pct']):+.2f}%</code>)\n\n"
            f"🗓️ <b>เดือนนี้ ({datetime.now().strftime('%m/%Y')}):</b>\n"
            f"• ปิดไป: <b>{month['total']}</b> ไม้ (🟢 {month['win']} | 🔴 {month['loss']})\n"
            f"• กำไรสุทธิ: {m_sign} <b>{float(month['pnl_thb']):+,.2f}</b> THB (เฉลี่ย <code>{float(month['avg_pct']):+.2f}%</code>)\n\n"
            f"📆 <b>ปีนี้ ({datetime.now().strftime('%Y')}):</b>\n"
            f"• ปิดไป: <b>{year['total']}</b> ไม้ (🟢 {year['win']} | 🔴 {year['loss']})\n"
            f"• กำไรสุทธิ: {y_sign} <b>{float(year['pnl_thb']):+,.2f}</b> THB (เฉลี่ย <code>{float(year['avg_pct']):+.2f}%</code>)\n"
            f"------------------------------------\n"
            f"👉 <i>แตะปุ่มด้านล่างเพื่อเลือกดูเจาะลึก รายวัน / รายเดือน / รายปี</i>"
        )
        return msg

def build_summary_day_text(conn) -> str:
    """สร้างข้อความสรุปผลงานรายวัน (10 วันทำการล่าสุด)"""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT 
                closed_date,
                COUNT(*) as total,
                COUNT(CASE WHEN closed_price > entry_price THEN 1 END) as win,
                COUNT(CASE WHEN closed_price < entry_price THEN 1 END) as loss,
                COALESCE(ROUND(SUM((closed_price - entry_price) * current_volume), 2), 0) as pnl_thb,
                COALESCE(ROUND(AVG(((closed_price - entry_price) / entry_price) * 100)::numeric, 2), 0) as avg_pct,
                STRING_AGG(symbol || ' (' || CASE WHEN closed_price >= entry_price THEN '+' ELSE '' END || ROUND(((closed_price - entry_price) / entry_price * 100)::numeric, 1) || '%)', ', ') as items
            FROM public.bot_active_positions
            WHERE status = 'CLOSED'
            GROUP BY closed_date
            ORDER BY closed_date DESC
            LIMIT 10;
        """)
        days = cur.fetchall()

        if not days:
            return "📅 <b>สรุปผลงานรายวัน (Daily Summary)</b>\n\n<i>ยังไม่มีประวัติการขายที่ปิดสถานะ</i>"

        msg = (
            f"📅 <b>สรุปผลงานการเทรดรายวัน (Daily Summary)</b>\n"
            f"<i>(ย้อนหลัง 10 วันทำการล่าสุดที่มีการปิดขาย)</i>\n"
            f"------------------------------------\n"
        )
        total_pnl = 0.0
        total_trades = 0
        total_win = 0
        total_loss = 0

        for d in days:
            tot = d["total"]
            w = d["win"]
            l = d["loss"]
            pnl_val = float(d["pnl_thb"])
            avg_p = float(d["avg_pct"])
            total_pnl += pnl_val
            total_trades += tot
            total_win += w
            total_loss += l

            icon = "🟢" if pnl_val > 0 else ("🔴" if pnl_val < 0 else "⚪")
            d_str = d["closed_date"].strftime("%Y-%m-%d")
            items_str = f"\n  ↳ <i>{d['items']}</i>" if d.get("items") else ""

            msg += (
                f"• <b>{d_str}</b>: {tot} ไม้ (🟢 {w} | 🔴 {l})\n"
                f"  กำไรสุทธิ: {icon} <b>{pnl_val:+,.2f}</b> THB (เฉลี่ย <code>{avg_p:+.2f}%</code>){items_str}\n\n"
            )

        period_wr = (total_win / total_trades * 100) if total_trades > 0 else 0.0
        tot_icon = "🟢" if total_pnl > 0 else ("🔴" if total_pnl < 0 else "⚪")
        msg += (
            f"------------------------------------\n"
            f"📊 <b>รวมช่วงที่แสดง ({len(days)} วันทำการ):</b>\n"
            f"• ปิดไปทั้งหมด: <b>{total_trades}</b> ไม้ (Win Rate: <b>{period_wr:.1f}%</b>)\n"
            f"• กำไรสุทธิรวม: {tot_icon} <b>{total_pnl:+,.2f}</b> THB\n"
        )
        return msg

def build_summary_month_text(conn) -> str:
    """สร้างข้อความสรุปผลงานรายเดือน"""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT 
                TO_CHAR(closed_date, 'YYYY-MM') as month,
                COUNT(*) as total,
                COUNT(CASE WHEN closed_price > entry_price THEN 1 END) as win,
                COUNT(CASE WHEN closed_price < entry_price THEN 1 END) as loss,
                COUNT(CASE WHEN closed_price = entry_price THEN 1 END) as be,
                COALESCE(ROUND(SUM((closed_price - entry_price) * current_volume), 2), 0) as pnl_thb,
                COALESCE(ROUND(AVG(((closed_price - entry_price) / entry_price) * 100)::numeric, 2), 0) as avg_pct
            FROM public.bot_active_positions
            WHERE status = 'CLOSED'
            GROUP BY TO_CHAR(closed_date, 'YYYY-MM')
            ORDER BY month DESC;
        """)
        months = cur.fetchall()

        if not months:
            return "🗓️ <b>สรุปผลงานรายเดือน (Monthly Summary)</b>\n\n<i>ยังไม่มีประวัติการขายที่ปิดสถานะ</i>"

        msg = (
            f"🗓️ <b>สรุปผลงานการเทรดรายเดือน (Monthly Summary)</b>\n"
            f"------------------------------------\n"
        )
        total_pnl = 0.0
        total_trades = 0
        total_win = 0
        total_loss = 0

        for m in months:
            tot = m["total"]
            w = m["win"]
            l = m["loss"]
            be = m["be"]
            pnl_val = float(m["pnl_thb"])
            avg_p = float(m["avg_pct"])
            wr = (w / tot * 100) if tot > 0 else 0.0

            total_pnl += pnl_val
            total_trades += tot
            total_win += w
            total_loss += l

            icon = "🟢" if pnl_val > 0 else ("🔴" if pnl_val < 0 else "⚪")

            msg += (
                f"• <b>เดือน {m['month']}</b> ({tot} ไม้):\n"
                f"  • สถิติ: 🟢 {w} | 🔴 {l} | ⚪ {be} (Win Rate: <b>{wr:.1f}%</b>)\n"
                f"  • กำไรสุทธิ: {icon} <b>{pnl_val:+,.2f}</b> THB\n"
                f"  • ผลตอบแทนเฉลี่ย: <code>{avg_p:+.2f}%</code> ต่อไม้\n\n"
            )

        all_wr = (total_win / total_trades * 100) if total_trades > 0 else 0.0
        tot_icon = "🟢" if total_pnl > 0 else ("🔴" if total_pnl < 0 else "⚪")
        msg += (
            f"------------------------------------\n"
            f"📊 <b>รวมทุกเดือน (All-Time Total):</b>\n"
            f"• ปิดไปทั้งหมด: <b>{total_trades}</b> ไม้ (Win Rate: <b>{all_wr:.1f}%</b>)\n"
            f"• กำไรสุทธิรวม: {tot_icon} <b>{total_pnl:+,.2f}</b> THB\n"
        )
        return msg

def build_summary_year_text(conn) -> str:
    """สร้างข้อความสรุปผลงานรายปี"""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT 
                TO_CHAR(closed_date, 'YYYY') as year,
                COUNT(*) as total,
                COUNT(CASE WHEN closed_price > entry_price THEN 1 END) as win,
                COUNT(CASE WHEN closed_price < entry_price THEN 1 END) as loss,
                COUNT(CASE WHEN closed_price = entry_price THEN 1 END) as be,
                COALESCE(ROUND(SUM((closed_price - entry_price) * current_volume), 2), 0) as pnl_thb,
                COALESCE(ROUND(AVG(((closed_price - entry_price) / entry_price) * 100)::numeric, 2), 0) as avg_pct,
                COALESCE(ROUND(MAX(((closed_price - entry_price) / entry_price) * 100)::numeric, 2), 0) as max_win_pct,
                COALESCE(ROUND(MIN(((closed_price - entry_price) / entry_price) * 100)::numeric, 2), 0) as max_loss_pct
            FROM public.bot_active_positions
            WHERE status = 'CLOSED'
            GROUP BY TO_CHAR(closed_date, 'YYYY')
            ORDER BY year DESC;
        """)
        years = cur.fetchall()

        if not years:
            return "📆 <b>สรุปผลงานรายปี (Yearly Summary)</b>\n\n<i>ยังไม่มีประวัติการขายที่ปิดสถานะ</i>"

        msg = (
            f"📆 <b>สรุปผลงานการเทรดรายปี (Yearly Summary)</b>\n"
            f"------------------------------------\n"
        )

        for y in years:
            tot = y["total"]
            w = y["win"]
            l = y["loss"]
            be = y["be"]
            pnl_val = float(y["pnl_thb"])
            avg_p = float(y["avg_pct"])
            wr = (w / tot * 100) if tot > 0 else 0.0
            icon = "🟢" if pnl_val > 0 else ("🔴" if pnl_val < 0 else "⚪")

            msg += (
                f"• <b>ปี {y['year']}</b> (รวม {tot:,} ไม้):\n"
                f"  • สถิติ: 🟢 ชนะ {w} | 🔴 แพ้ {l} | ⚪ เสมอ {be}\n"
                f"  • Win Rate: <b>{wr:.1f}%</b>\n"
                f"  • กำไรสุทธิ: {icon} <b>{pnl_val:+,.2f}</b> THB\n"
                f"  • ผลตอบแทนเฉลี่ย: <code>{avg_p:+.2f}%</code> ต่อไม้\n"
                f"  • ไม้กำไรสูงสุด: <code>+{float(y['max_win_pct']):.2f}%</code>\n"
                f"  • ไม้ขาดทุนสูงสุด: <code>{float(y['max_loss_pct']):.2f}%</code>\n\n"
            )

        return msg

async def cmd_summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /summary [day|month|year|overview] - แสดง Performance Summary ผลงานการเทรด
    """
    sub = "overview"
    if context.args:
        arg = context.args[0].lower().strip()
        if arg in ("day", "daily", "d", "วัน", "รายวัน"):
            sub = "day"
        elif arg in ("month", "monthly", "m", "เดือน", "รายเดือน"):
            sub = "month"
        elif arg in ("year", "yearly", "y", "ปี", "รายปี"):
            sub = "year"

    conn = get_db_connection()
    try:
        if sub == "day":
            msg = build_summary_day_text(conn)
        elif sub == "month":
            msg = build_summary_month_text(conn)
        elif sub == "year":
            msg = build_summary_year_text(conn)
        else:
            msg = build_summary_overview_text(conn)

        kbd = build_summary_keyboard(current_mode=sub)
        await update.message.reply_text(msg, parse_mode="HTML", reply_markup=kbd)
    finally:
        conn.close()

async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /history [N หรือ SYM] - ดูประวัติการขายที่ปิดสถานะแล้ว กำไรขาดทุนกี่บาท กี่ %
    เช่น:
      /history        -> แสดง 10 ไม้ล่าสุด
      /history 20     -> แสดง 20 ไม้ล่าสุด
      /history TDEX   -> แสดงประวัติเฉพาะหุ้น TDEX
    """
    limit = 10
    sym_filter = None

    if context.args:
        arg = context.args[0].strip()
        if arg.isdigit():
            limit = min(max(int(arg), 1), 50)
        else:
            sym_filter = arg.upper()
            if len(context.args) > 1 and context.args[1].isdigit():
                limit = min(max(int(context.args[1]), 1), 50)

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            base_sql = """
                SELECT 
                    b.id AS position_id,
                    b.symbol,
                    b.current_volume AS volume,
                    b.entry_price,
                    b.closed_price,
                    b.entry_date,
                    b.closed_date,
                    b.exit_reason,
                    b.is_adopted,
                    ROUND((b.closed_price - b.entry_price) * b.current_volume, 2) AS pnl_thb,
                    ROUND(((b.closed_price - b.entry_price) / b.entry_price * 100)::numeric, 2) AS pnl_pct,
                    o.order_id,
                    o.broker_order_no,
                    o.executed_at
                FROM public.bot_active_positions b
                LEFT JOIN LATERAL (
                    SELECT order_id, broker_order_no, executed_at, executed_price
                    FROM public.bot_orders
                    WHERE symbol = b.symbol 
                      AND side = 'SELL' 
                      AND status = 'FILLED'
                      AND (executed_at::date = b.closed_date OR created_at::date = b.closed_date)
                    ORDER BY order_id DESC LIMIT 1
                ) o ON TRUE
                WHERE b.status = 'CLOSED'
            """
            params = []
            if sym_filter:
                base_sql += " AND b.symbol = %s"
                params.append(sym_filter)
            base_sql += " ORDER BY b.closed_date DESC, b.id DESC LIMIT %s;"
            params.append(limit)

            cur.execute(base_sql, tuple(params))
            rows = cur.fetchall()

            if not rows:
                if sym_filter:
                    await update.message.reply_text(f"📜 ไม่พบประวัติการขายสำหรับหุ้น <b>{sym_filter}</b>", parse_mode="HTML")
                else:
                    await update.message.reply_text("📜 ยังไม่มีประวัติการขายหุ้นที่ปิดสถานะในระบบ", parse_mode="HTML")
                return

            filter_note = f" (เฉพาะหุ้น {sym_filter})" if sym_filter else ""
            header = (
                f"📜 <b>ประวัติการขายหุ้นที่ปิดสถานะแล้ว{filter_note}</b>\n"
                f"<i>(แสดง {len(rows)} รายการล่าสุด)</i>\n"
                f"------------------------------------\n"
            )

            messages_to_send = []
            current_msg = header
            tot_pnl_thb = 0.0
            win_cnt = 0
            loss_cnt = 0

            for r in rows:
                sym = r["symbol"]
                vol = int(r["volume"])
                entry_p = float(r["entry_price"])
                closed_p = float(r["closed_price"])
                pnl_thb = float(r["pnl_thb"])
                pnl_pct = float(r["pnl_pct"])
                tot_pnl_thb += pnl_thb

                if pnl_thb > 0:
                    win_cnt += 1
                elif pnl_thb < 0:
                    loss_cnt += 1

                c_date = r["closed_date"].strftime("%Y-%m-%d") if r["closed_date"] else "-"
                days = (r["closed_date"] - r["entry_date"]).days if (r["closed_date"] and r["entry_date"]) else 0
                tag = "🛡️ [ADOPT]" if r.get("is_adopted") else "🤖 [BOT]"
                pnl_icon = "🟢" if pnl_thb > 0 else ("🔴" if pnl_thb < 0 else "⚪")
                ref = f"#{r['broker_order_no']}" if r.get("broker_order_no") else (f"Order #{r['order_id']}" if r.get("order_id") else "-")
                reason = r.get("exit_reason") or "SELL"

                sym_link = f"<a href='https://www.settrade.com/th/equities/quote/{sym}/overview'><b>{sym}</b></a>"

                item = (
                    f"{pnl_icon} {sym_link} (<code>{vol:,}</code> หุ้น) {tag}\n"
                    f"• วันที่ปิด: <code>{c_date}</code> (ถือ {days} วัน) | Ref: <code>{ref}</code>\n"
                    f"• ทุน: <code>{entry_p:.2f}</code> ➔ ขาย: <code>{closed_p:.2f}</code> THB\n"
                    f"• กำไร/ขาดทุน: <b>{pnl_thb:+,.2f} THB</b> (<code>{pnl_pct:+.2f}%</code>)\n"
                    f"• เหตุผล: <code>{reason}</code>\n\n"
                )

                if len(current_msg) + len(item) > 3500:
                    messages_to_send.append(current_msg)
                    current_msg = item
                else:
                    current_msg += item

            batch_wr = (win_cnt / len(rows) * 100) if rows else 0.0
            tot_icon = "🟢" if tot_pnl_thb > 0 else ("🔴" if tot_pnl_thb < 0 else "⚪")
            avg_pnl_pct = sum(float(x["pnl_pct"]) for x in rows) / len(rows) if rows else 0.0

            footer = (
                f"------------------------------------\n"
                f"📊 <b>สรุปชุดนี้ ({len(rows)} ไม้):</b>\n"
                f"• กำไรสุทธิ: {tot_icon} <b>{tot_pnl_thb:+,.2f}</b> THB (เฉลี่ย <code>{avg_pnl_pct:+.2f}%</code> ต่อไม้)\n"
                f"• Win Rate: <b>{batch_wr:.1f}%</b> (🟢 {win_cnt} | 🔴 {loss_cnt})\n"
                f"👉 <i>ดูสรุปผลงานภาพรวม: พิมพ์ /summary</i>"
            )

            if len(current_msg) + len(footer) > 3800:
                messages_to_send.append(current_msg)
                messages_to_send.append(footer)
            else:
                current_msg += footer
                messages_to_send.append(current_msg)

            for m in messages_to_send:
                await update.message.reply_text(
                    m,
                    parse_mode="HTML",
                    link_preview_options=LinkPreviewOptions(is_disabled=True)
                )
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
    """/scan - สแกนหาจังหวะซื้อทางเทคนิคอลดั้งเดิม และส่งการ์ดกราฟขออนุมัติ (รวดเร็ว ไม่รอ AI)"""
    await update.message.reply_text("🔍 กำลังสแกนหาจังหวะซื้อทางเทคนิคอลและสร้างกราฟ...")
    try:
        from jobs.run_buy_scanner import scan_and_notify
        await scan_and_notify(run_ai=False)
    except Exception as e:
        await update.message.reply_text(f"❌ เกิดข้อผิดพลาดในการสแกน: {e}")

async def cmd_ai_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/ai_scan [1|2|3] [SYM] - สแกนหุ้นและส่งให้ AI วิเคราะห์ 3 มิติ (ขึ้นจริง/หลอก, VI พื้นฐาน, Swing R:R)"""
    level = 1
    sym = None

    if context.args:
        for arg in context.args:
            arg_str = arg.strip()
            if arg_str in ("1", "2", "3"):
                level = int(arg_str)
            elif arg_str.isalpha():
                sym = arg_str.upper()

    level_names = {
        1: "1 (MINIMAL - Flash Scorecard 1 บรรทัด/ตัว)",
        2: "2 (COMPACT - สรุป 3 เสาหลัก ชี้ขาดขึ้นจริง/หลอก)",
        3: "3 (DETAILED - บทวิเคราะห์เจาะลึกจัดเต็ม)"
    }
    level_label = level_names.get(level, level_names[1])

    target_desc = f"หุ้น <b>{sym}</b>" if sym else "หุ้นผู้สมัครทั้งหมด"
    progress_msg = await update.message.reply_text(
        f"🤖 <b>AI Stock Evaluator</b>\n"
        f"• กำลังประเมิน: {target_desc}\n"
        f"• ระดับความละเอียด: <code>{level_label}</code>\n"
        f"⏳ <i>กรุณารอสักครู่ กำลังประมวลผลผ่าน AI บน Pi 5...</i>",
        parse_mode="HTML"
    )

    try:
        from core.signals.aggregator import run_signal_aggregator
        from core.ai.router import evaluate_candidates
        from core.ai.formatter import format_ai_evaluation_html

        if not sym:
            run_signal_aggregator()

        symbols = [sym] if sym else None
        ai_res = evaluate_candidates(symbols=symbols)

        if ai_res and ai_res.get("error"):
            err_text = str(ai_res.get("error"))
            if "timed out" in err_text.lower():
                err_msg = (
                    "⚠️ <b>AI Evaluation Timeout:</b>\n"
                    "ระบบ AI ใช้เวลาวิเคราะห์นานเกินกำหนด (Timeout)\n\n"
                    "💡 <i>คำแนะนำ:</i>\n"
                    "• ลองพิมพ์ <code>/ai_scan SMPC</code> เพื่อให้ AI วิเคราะห์หุ้นตัวที่สนใจรายตัว (ตอบกลับไว 10-15 วิ)\n"
                    "• หรือพิมพ์ <code>/ai_scan 1</code> เพื่อลองใหม่อีกครั้ง"
                )
            else:
                if len(err_text) > 1500:
                    err_text = err_text[:1500] + "... (truncated)"
                err_msg = f"⚠️ <b>AI Evaluation Error:</b>\n<code>{err_text}</code>"
            await progress_msg.edit_text(err_msg, parse_mode="HTML")
            return

        if not ai_res or not ai_res.get("evaluations"):
            no_data_msg = (
                "🛡️ <b>ผลการสแกนด้วย AI</b>\n\n"
                "ไม่พบสัญญาณซื้อใหม่ที่ผ่านเกณฑ์ทางเทคนิคอลในขณะนี้\n\n"
                "💡 <i>คำแนะนำ:</i>\n"
                "• พิมพ์ <code>/ai_scan &lt;SYM&gt;</code> เพื่อให้ AI วิเคราะห์หุ้นรายตัวที่ต้องการ (เช่น <code>/ai_scan SMPC</code>)\n"
                "• หรือพิมพ์ <code>/scan</code> เพื่อตรวจสอบสัญญาณและดูกราฟเทคนิคอลทั่วไป"
            )
            await progress_msg.edit_text(no_data_msg, parse_mode="HTML")
            return

        html_output = format_ai_evaluation_html(ai_res, detail_level=level)

        LAST_AI_EVALUATION["result"] = ai_res
        from core.ai.state import save_last_evaluation
        save_last_evaluation(ai_res)
        evaluations = ai_res.get("evaluations", [])
        buy_syms = [ev["symbol"] for ev in evaluations if ev.get("decision") == "BUY"]
        all_syms = [ev["symbol"] for ev in evaluations]

        top_pick = ai_res.get("top_pick", "NONE")
        buttons = []
        if top_pick != "NONE":
            tv_url = get_tradingview_url(top_pick)
            buttons.append([
                InlineKeyboardButton(f"🛒 ซื้อ {top_pick}", callback_data=f"ai_buy:{top_pick}"),
                InlineKeyboardButton(f"📈 TradingView {top_pick}", url=tv_url)
            ])

        if buy_syms:
            buttons.append([
                InlineKeyboardButton(f"📊 ดูกราฟเฉพาะตัวที่แนะนำ BUY ({len(buy_syms)} ตัว)", callback_data="ai_charts:buy")
            ])

        buttons.append([
            InlineKeyboardButton(f"📈 ดูกราฟทั้งหมดของรอบนี้ ({len(all_syms)} ตัว)", callback_data="ai_charts:all")
        ])
        reply_markup = InlineKeyboardMarkup(buttons)

        from core.ai.formatter import split_telegram_html
        chunks = split_telegram_html(html_output, max_length=3800)

        if len(chunks) == 1:
            await progress_msg.edit_text(chunks[0], parse_mode="HTML", reply_markup=reply_markup)
        else:
            await progress_msg.edit_text(chunks[0], parse_mode="HTML")
            for chunk in chunks[1:-1]:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=chunk,
                    parse_mode="HTML"
                )
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=chunks[-1],
                parse_mode="HTML",
                reply_markup=reply_markup
            )

    except Exception as e:
        err_str = str(e)
        if len(err_str) > 1500:
            err_str = err_str[:1500] + "... (truncated)"
        try:
            await progress_msg.edit_text(f"❌ <b>AI Evaluation Error:</b>\n<code>{err_str}</code>", parse_mode="HTML")
        except Exception:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"❌ AI Evaluation Error:\n{err_str[:1000]}"
            )



async def cmd_ai_credit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/ai_credit หรือ /ai_quota - เช็คโควตาคงเหลือและสถานะของ Antigravity CLI บน Pi 5"""
    from core.ai.router import get_ai_quota_status, get_active_provider_name

    active_provider = get_active_provider_name()
    if active_provider != "agy_pi5":
        msg = (
            "⚠️ <b>คำสั่งนี้ใช้งานได้เฉพาะเมื่อเปิดใช้งาน Provider agy_pi5 เท่านั้น</b>\n\n"
            f"💡 <b>สถานะปัจจุบัน:</b>\n"
            f"• Provider ที่ใช้งานอยู่: <code>{active_provider}</code>\n"
            f"• คำสั่ง /ai_credit ออกแบบมาสำหรับตรวจสอบสถานะและโควตาของ Antigravity CLI บน Raspberry Pi 5\n\n"
            "<i>หากต้องการสลับกลับมาใช้ agy_pi5 ให้ตั้งค่าใน <code>config/bot_config.json</code></i>"
        )
        if update.effective_message:
            await update.effective_message.reply_text(msg, parse_mode="HTML")
        return

    progress_msg = None
    if update.effective_message:
        progress_msg = await update.effective_message.reply_text(
            "⏳ <b>กำลังตรวจสอบสถานะและโควตาของ Antigravity CLI บน Pi 5...</b>",
            parse_mode="HTML"
        )

    try:
        status_info = get_ai_quota_status()

        if not status_info.get("supported", False):
            if progress_msg:
                await progress_msg.edit_text(
                    f"⚠️ {status_info.get('message', 'ไม่รองรับการตรวจสอบโควตา')}",
                    parse_mode="HTML"
                )
            return

        is_online = (status_info.get("status") == "ONLINE")
        conn_emoji = "🟢" if is_online else "🔴"
        conn_text = "เชื่อมต่อสำเร็จ (Online)" if is_online else "การเชื่อมต่อขัดข้อง (Offline)"
        latency = status_info.get("latency_ms", 0)

        weekly_reqs = status_info.get("weekly_requests", 0)
        is_exhausted = status_info.get("quota_exhausted", False)
        reset_in = status_info.get("quota_reset_in")

        if is_exhausted:
            quota_badge = "🔴 <b>เต็มชั่วคราว (Quota Reached)</b>"
            reset_note = f"\n⏳ <b>กำหนดรีเซ็ตโควตา:</b> ในอีก <code>{reset_in or 'รอบสัปดาห์ถัดไป'}</code>"
        else:
            quota_badge = "🟢 <b>พร้อมใช้งาน (Active)</b>"
            reset_note = ""

        msg = (
            "🤖 <b>สถานะ Antigravity AI (Raspberry Pi 5)</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🔌 <b>สถานะการเชื่อมต่อ:</b> {conn_emoji} {conn_text} ({latency} ms)\n"
            f"👤 <b>บัญชีที่เข้าสู่ระบบ:</b> <code>{status_info.get('account', 'kengnex@gmail.com')}</code>\n"
            f"💳 <b>แพ็กเกจบัญชี:</b> <b>{status_info.get('tier', 'Gemini Code Assist')}</b>\n"
            f"🎁 <b>โควตารายสัปดาห์ (Rolling 7D):</b> {quota_badge}{reset_note}\n"
            f"🧠 <b>โมเดล AI หลัก:</b> <code>{status_info.get('model', 'Gemini 3.8 Flash (High)')}</code>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "📊 <b>สถิติการใช้งานคำขอ (Requests):</b>\n"
            f"• คำขอวันนี้: <b>{status_info.get('today_requests', 0):,} ครั้ง</b>\n"
            f"• คำขอใน 7 วันล่าสุด: <b>{weekly_reqs:,} ครั้ง</b> (~50 ครั้ง/สัปดาห์)\n"
            f"• คำขอสะสมทั้งหมด: <b>{status_info.get('total_requests', 0):,} ครั้ง</b>\n"
        )

        last_usage = status_info.get("last_usage")
        if last_usage and last_usage.get("usage"):
            u = last_usage.get("usage", {})
            dur = last_usage.get("duration_seconds", 0)
            msg += (
                f"\n⚡ <b>รอบวิเคราะห์ล่าสุด:</b>\n"
                f"• ความเร็วประมวลผล: {dur:.1f} วินาที\n"
                f"• โทเค็นที่ใช้: <code>{u.get('total_tokens', 0):,} tokens</code> "
                f"(In: {u.get('input_tokens', 0):,} | Out: {u.get('output_tokens', 0):,})\n"
            )

        msg += (
            f"\n🕒 <b>เรียกใช้ล่าสุด:</b> {status_info.get('last_request_time', '-')}\n"
            f"🔑 <b>OAuth Token:</b> 🟢 Valid (หมดอายุ: {status_info.get('token_expiry_formatted', '-')})\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "💡 <i>Tip: ใช้คำสั่ง /ai_scan เพื่อวิเคราะห์หุ้น 3 มิติได้ตลอดเวลา</i>"
        )

        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🔄 รีเฟรชสถานะ", callback_data="ai_credit_refresh"),
                InlineKeyboardButton("⚡ สแกนหุ้นด่วน (/ai_scan)", callback_data="run_scan_ai")
            ]
        ])

        if progress_msg:
            await progress_msg.edit_text(msg, parse_mode="HTML", reply_markup=keyboard)
        elif update.effective_message:
            await update.effective_message.reply_text(msg, parse_mode="HTML", reply_markup=keyboard)

    except Exception as e:
        if progress_msg:
            await progress_msg.edit_text(f"❌ <b>เกิดข้อผิดพลาดในการตรวจสอบโควตา:</b>\n<code>{e}</code>", parse_mode="HTML")
        elif update.effective_message:
            await update.effective_message.reply_text(f"❌ <b>เกิดข้อผิดพลาดในการตรวจสอบโควตา:</b>\n<code>{e}</code>", parse_mode="HTML")


async def show_ai_candidate_charts(update: Update, context: ContextTypes.DEFAULT_TYPE, mode: str = "all"):
    """ส่งต่อให้ show_ai_candidate_charts ใน callback_handlers จัดการ"""
    from bot.callback_handlers import show_ai_candidate_charts as _show
    await _show(update, context, mode=mode)

async def cmd_scan_port(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/scan_port [SYM] - รายงานสัญญาณกราฟเทคนิคอลของหุ้นในพอร์ตเข้า Telegram"""
    sym = context.args[0].upper().strip() if context.args else None
    target_msg = update.effective_message
    if target_msg:
        if sym:
            await target_msg.reply_text(f"🔍 กำลังสร้างกราฟและวิเคราะห์สัญญาณเทคนิคอลของ <b>{sym}</b> ในพอร์ต...", parse_mode="HTML")
        else:
            await target_msg.reply_text("🔍 กำลังเริ่มกระบวนการสแกนสัญญาณกราฟหุ้นทั้งหมดในพอร์ต...", parse_mode="HTML")

    try:
        from jobs.run_port_scanner import scan_portfolio_and_notify
        await scan_portfolio_and_notify(symbol=sym, bot=context.bot, chat_id=update.effective_chat.id)
    except Exception as e:
        if target_msg:
            await target_msg.reply_text(f"❌ เกิดข้อผิดพลาดในการสแกนหุ้นในพอร์ต: {e}")

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
                       percent_profit, initial_stop_loss, trailing_stop_loss, is_managed_by_bot,
                       is_adopted, bot_entry_price
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
                from core.config_manager import get_excluded_symbols
                excluded_set = set(s.upper() for s in get_excluded_symbols())
                items_in_batch = 0
                for num, r in enumerate(real_rows, 1):
                    is_adopt = bool(r.get("is_adopted"))
                    if is_adopt:
                        tag = "🛡️ [ADOPT]"
                    elif r["is_managed_by_bot"]:
                        tag = "🤖 [BOT]"
                    else:
                        tag = "👤 [MANUAL]"

                    if r["symbol"].upper() in excluded_set:
                        tag += " 🔒 [EXCLUDED]"

                    sl_str = f"{float(r['initial_stop_loss']):.2f}" if r["initial_stop_loss"] is not None else "N/A"
                    ts_str = f"{float(r['trailing_stop_loss']):.2f}" if r["trailing_stop_loss"] is not None else "-"
                    pnl = float(r["percent_profit"]) if r["percent_profit"] is not None else 0.0
                    sym_link = f"<a href='https://www.settrade.com/th/equities/quote/{r['symbol']}/overview'><b>{r['symbol']}</b></a>"

                    if is_adopt and r.get("bot_entry_price"):
                        adopt_base = float(r["bot_entry_price"])
                        mkt_p = float(r["market_price"])
                        adopt_pnl = round(((mkt_p - adopt_base) / adopt_base) * 100, 2) if adopt_base > 0 else 0.0
                        item = (
                            f"\nNo.{num}: {tag} {sym_link} ({r['current_volume']:,} หุ้น)\n"
                            f"• ทุนจริง: {float(r['average_price']):.2f} | ตลาด: {mkt_p:.2f} (พอร์ต: {pnl:+.2f}%)\n"
                            f"• ฐานรับเลี้ยง: {adopt_base:.2f} (รอบใหม่: {adopt_pnl:+.2f}%)\n"
                            f"• Initial SL: {sl_str} | Trailing SL: {ts_str}\n"
                        )
                    else:
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

async def cmd_adopt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/adopt <SYM> - แปลงหุ้น Manual ให้บอทเข้ามาช่วยดูแลความปลอดภัยและ Trailing Stop รอบใหม่"""
    if not context.args:
        await update.message.reply_text(
            "📌 <b>วิธีใช้งานคำสั่ง /adopt:</b>\n"
            "• <code>/adopt 3BBIF</code> (ระบุชื่อหุ้นที่ถืออยู่ในพอร์ต)\n\n"
            "ℹ️ <i>บอทจะเริ่มนับฐานราคาความปลอดภัยจากราคาตลาดปัจจุบัน และคำนวณ Initial SL พร้อมตั้ง Trailing Stop ยกตามตูดให้ทันที โดยไม่นำผลขาดทุนสะสมเดิมมาตัดขายทิ้ง</i>\n"
            "👉 พิมพ์ /port เพื่อดูรายชื่อหุ้น [MANUAL] ในพอร์ต",
            parse_mode="HTML"
        )
        return

    sym = context.args[0].upper().strip()
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 1. ตรวจสอบว่ามีหุ้นในพอร์ตจริงหรือไม่
            cur.execute("""
                SELECT p.symbol, p.current_volume, p.average_price, p.market_price, p.percent_profit,
                       c.quote_type, i.atr14,
                       b.id AS position_id, b.status AS pos_status, b.is_adopted, b.is_managed_by_bot
                FROM public.portfolio_stock p
                LEFT JOIN public.master_stock_classification c ON p.symbol = c.symbol
                LEFT JOIN public.bot_active_positions b ON p.symbol = b.symbol AND b.status = 'OPEN'
                LEFT JOIN public.mv_stock_indicators i ON p.symbol = i.symbol
                    AND i.trade_date = (SELECT MAX(trade_date) FROM public.mv_stock_indicators)
                WHERE p.imported_at = (SELECT MAX(imported_at) FROM public.portfolio_stock)
                  AND p.symbol = %s
                  AND p.current_volume > 0;
            """, (sym,))
            row = cur.fetchone()

            if not row:
                await update.message.reply_text(
                    f"❌ ไม่พบหุ้น <b>{sym}</b> ที่ถือครองในพอร์ตจริงขณะนี้\n"
                    f"👉 พิมพ์ <code>/port</code> เพื่อดูรายชื่อหุ้นทั้งหมดในพอร์ต",
                    parse_mode="HTML"
                )
                return

            # 2. ตรวจสอบว่าบอทดูแลอยู่แล้วหรือไม่
            if row.get("position_id") and row.get("pos_status") == "OPEN":
                if row.get("is_adopted"):
                    await update.message.reply_text(
                        f"ℹ️ หุ้น <b>{sym}</b> อยู่ในสถานะ 🛡️ <b>[ADOPT]</b> ที่บอทดูแลอยู่แล้วครับ\n"
                        f"👉 หากต้องการยกเลิกให้พิมพ์ <code>/release {sym}</code>",
                        parse_mode="HTML"
                    )
                    return
                elif row.get("is_managed_by_bot"):
                    await update.message.reply_text(
                        f"ℹ️ หุ้น <b>{sym}</b> เป็นหุ้นที่บอทซื้อและดูแลอยู่แล้ว (🤖 <b>[BOT]</b>)",
                        parse_mode="HTML"
                    )
                    return

            # 3. ดึงราคาตลาดปัจจุบัน และ ATR
            mkt_price = float(row["market_price"] or 0)
            avg_price = float(row["average_price"] or 0)
            pnl_pct = float(row["percent_profit"] or 0)
            curr_vol = int(row["current_volume"])
            atr = float(row["atr14"]) if row["atr14"] else None
            q_type = row["quote_type"] or "EQUITY"

            if mkt_price <= 0:
                try:
                    quote = get_realtime_quote(None, sym)
                    mkt_price = float(quote.get("last", 0.0))
                except Exception:
                    pass
                if mkt_price <= 0:
                    mkt_price = avg_price

            # คำนวณ Initial Stop Loss สำหรับรอบรับเลี้ยงใหม่
            from core.position_tracker import calculate_initial_stop_loss, load_config
            config = load_config()
            init_sl = calculate_initial_stop_loss(mkt_price, atr, q_type, config)
            sl_pct = round(((mkt_price - init_sl) / mkt_price) * 100, 2) if mkt_price > 0 else 0.0

            # 4. หากอยู่ใน excluded_symbols ให้ปลดออกเพื่อเปิดให้ระบบคุ้มครอง
            from core.config_manager import remove_excluded_symbols, get_excluded_symbols
            if sym in get_excluded_symbols():
                remove_excluded_symbols([sym])

            # 5. Upsert เข้า bot_active_positions
            cur.execute("""
                INSERT INTO public.bot_active_positions (
                    symbol, entry_date, entry_price, entry_atr14,
                    initial_stop_loss, max_price_reached, current_volume,
                    is_managed_by_bot, is_adopted, status
                ) VALUES (
                    %s, CURRENT_DATE, %s, %s,
                    %s, %s, %s,
                    TRUE, TRUE, 'OPEN'
                )
                ON CONFLICT (symbol) WHERE (status = 'OPEN') DO UPDATE
                SET entry_date = CURRENT_DATE,
                    entry_price = EXCLUDED.entry_price,
                    entry_atr14 = EXCLUDED.entry_atr14,
                    initial_stop_loss = EXCLUDED.initial_stop_loss,
                    max_price_reached = EXCLUDED.max_price_reached,
                    current_volume = EXCLUDED.current_volume,
                    is_managed_by_bot = TRUE,
                    is_adopted = TRUE,
                    status = 'OPEN',
                    updated_at = timezone('Asia/Bangkok', now());
            """, (sym, mkt_price, atr, init_sl, mkt_price, curr_vol))

            conn.commit()

            msg = (
                f"🛡️ <b>รับเลี้ยงหุ้น [ADOPT] สำเร็จเรียบร้อย!</b>\n\n"
                f"• หุ้น: <b>{sym}</b> (<code>{curr_vol:,}</code> หุ้น)\n"
                f"• ทุนเดิมในพอร์ต: <code>{avg_price:.2f}</code> THB (P/L รวม: <code>{pnl_pct:+.2f}%</code>)\n"
                f"• <b>ฐานราคาเริ่มดูแล (Adopt Base):</b> <code>{mkt_price:.2f}</code> THB\n"
                f"• Initial Stop Loss: <code>{init_sl:.2f}</code> THB (<code>-{sl_pct:.2f}%</code>)\n"
                f"• Trailing Stop: <i>จะเริ่มยกตามเมื่อราคาปิดสูงกว่า <code>{mkt_price:.2f}</code> THB</i>\n"
                f"------------------------------------\n"
                f"ℹ️ <i>บอทคุ้มครองความปลอดภัยรอบใหม่ให้ทันที และไม่นำผลขาดทุนเดิมมาตัดขาย\n"
                f"👉 หากต้องการยกเลิกให้กลับเป็น Manual พิมพ์ <code>/release {sym}</code></i>"
            )
        conn.close()
        await update.message.reply_text(msg, parse_mode="HTML")
    except Exception as e:
        if conn and not conn.closed:
            conn.close()
        # ถ้าข้อความสำเร็จถูกส่งไปแล้ว หรือเกิด network timeout ให้ log แทน
        print(f"[ADOPT ERROR] {sym}: {e}")
        await update.message.reply_text(f"❌ เกิดข้อผิดพลาดในการ Adopt หุ้น {sym}: {e}")

async def cmd_release(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/release <SYM> - ปลดหุ้นจากการดูแลของบอท กลับไปเป็น [MANUAL]"""
    if not context.args:
        await update.message.reply_text(
            "📌 <b>วิธีใช้งานคำสั่ง /release:</b>\n"
            "• <code>/release 3BBIF</code> (ระบุชื่อหุ้นที่ต้องการปลดออกจากการดูแล)\n\n"
            "ℹ️ <i>เมื่อปลดออก หุ้นจะกลับไปเป็นสถานะ 👤 [MANUAL] บอทจะไม่ตั้ง Stop Loss ไม่ขยับ Trailing Stop และไม่สั่งขายอัตโนมัติ</i>",
            parse_mode="HTML"
        )
        return

    sym = context.args[0].upper().strip()
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, symbol, is_adopted, is_managed_by_bot
                FROM public.bot_active_positions
                WHERE symbol = %s AND status = 'OPEN';
            """, (sym,))
            row = cur.fetchone()

            if not row:
                await update.message.reply_text(
                    f"ℹ️ หุ้น <b>{sym}</b> ไม่ได้อยู่ในการดูแลของบอทอยู่แล้ว (สถานะเป็น 👤 <b>[MANUAL]</b>)",
                    parse_mode="HTML"
                )
                return

            # ปิดสถานะใน bot_active_positions
            cur.execute("""
                UPDATE public.bot_active_positions
                SET status = 'CLOSED',
                    closed_date = CURRENT_DATE,
                    exit_reason = 'USER_RELEASED',
                    updated_at = timezone('Asia/Bangkok', now())
                WHERE id = %s;
            """, (row["id"],))
            conn.commit()

            msg = (
                f"👤 <b>ปลดการดูแลหุ้น {sym} สำเร็จ!</b>\n\n"
                f"• หุ้น: <b>{sym}</b>\n"
                f"• สถานะปัจจุบัน: กลับเป็น 👤 <b>[MANUAL]</b> ตามเดิมเรียบร้อย\n"
                f"------------------------------------\n"
                f"<i>บอทจะไม่ส่งคำสั่งขาย ไม่คิด Trailing Stop และไม่คำนวณ Stop Loss สำหรับหุ้นตัวนี้อีกต่อไป</i>\n"
                f"👉 หากต้องการให้บอทกลับมาดูแลใหม่ พิมพ์ <code>/adopt {sym}</code> ได้ตลอดเวลา"
            )
        conn.close()
        await update.message.reply_text(msg, parse_mode="HTML")
    except Exception as e:
        if conn and not conn.closed:
            conn.close()
        print(f"[RELEASE ERROR] {sym}: {e}")
        await update.message.reply_text(f"❌ เกิดข้อผิดพลาดในการปลดหุ้น {sym}: {e}")

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



async def cmd_holidays(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/holidays [year] - แสดงปฏิทินวันหยุดตลาดหลักทรัพย์ SET และสถานะตลาดวันนี้"""
    from core.market_schedule import is_trading_day_calendar, load_holidays, add_holiday, remove_holiday
    from datetime import date

    today = date.today()
    args = [a.strip() for a in context.args if a.strip()]

    # จัดการ Subcommand: add หรือ del
    if args and args[0].lower() in ("add", "+"):
        if len(args) < 3:
            await update.message.reply_text("⚠️ รูปแบบการเพิ่ม: <code>/holidays add YYYY-MM-DD คำอธิบาย</code>", parse_mode="HTML")
            return
        d_str = args[1]
        desc = " ".join(args[2:])
        ok, res = add_holiday(d_str, desc)
        await update.message.reply_text(f"📅 {res}", parse_mode="HTML")
        return

    if args and args[0].lower() in ("del", "remove", "-"):
        if len(args) < 2:
            await update.message.reply_text("⚠️ รูปแบบการลบ: <code>/holidays del YYYY-MM-DD</code>", parse_mode="HTML")
            return
        d_str = args[1]
        ok, res = remove_holiday(d_str)
        await update.message.reply_text(f"📅 {res}", parse_mode="HTML")
        return

    # ตรวจสอบปีที่ต้องการดู
    target_year = str(today.year)
    if args and args[0].isdigit() and len(args[0]) == 4:
        target_year = args[0]

    all_holidays = load_holidays()
    year_holidays = all_holidays.get(target_year, {})

    is_trading, cal_reason = is_trading_day_calendar(today)
    cal_status = "🟢 วันทำการปกติ" if is_trading else f"🔴 {cal_reason}"

    msg_lines = [
        f"📅 <b>ปฏิทินวันหยุดตลาดหลักทรัพย์แห่งประเทศไทย (SET {target_year})</b>",
        f"• สถานะวันนี้ ({today.strftime('%Y-%m-%d')}): <b>{cal_status}</b>",
        f"------------------------------------",
        f"<b>รายการวันหยุดตลอดทั้งปี ({len(year_holidays)} วัน):</b>"
    ]

    if not year_holidays:
        msg_lines.append("<i>(ยังไม่มีข้อมูลวันหยุดของปีนี้)</i>")
    else:
        for h_date, h_desc in sorted(year_holidays.items()):
            prefix = "📌" if h_date == today.strftime("%Y-%m-%d") else ("▫️" if h_date < today.strftime("%Y-%m-%d") else "🔹")
            msg_lines.append(f"{prefix} <code>{h_date}</code> : {h_desc}")

    msg_lines.append("------------------------------------")
    msg_lines.append("💡 <i>ระบบมีด่านตรวจ Failover อัตโนมัติเวลา 10:02 น. ทุกวันทำการ</i>\n"
                     "👉 ดูปีอื่น: <code>/holidays 2025</code>\n"
                     "👉 เพิ่มวันหยุด: <code>/holidays add YYYY-MM-DD คำอธิบาย</code>\n"
                     "👉 ลบวันหยุด: <code>/holidays del YYYY-MM-DD</code>")

    await update.message.reply_text("\n".join(msg_lines), parse_mode="HTML")

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
    """/status - แสดงสถานะการทำงานของบอทและสวิตช์ควบคุมความปลอดภัย"""
    from bot.callback_handlers import load_config, build_status_text, build_status_keyboard
    from core.job_notifier import is_job_notification_enabled
    config = load_config()
    ctrl = config.get("auto_sell_controls", {})
    job_notify_on = is_job_notification_enabled()

    msg = build_status_text(config, job_notify_on)
    keyboard = build_status_keyboard(ctrl, job_notify_on)
    await update.message.reply_text(msg, parse_mode="HTML", reply_markup=keyboard)


async def cmd_auto_sell(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/auto_sell [trailing|sl|cutloss|tech|all] [on|off] - จัดการสวิตช์ส่งคำสั่งขายอัตโนมัติ"""
    from core.config_manager import (
        get_auto_sell_controls,
        set_auto_sell_control,
        set_all_auto_sell_controls,
        toggle_auto_sell_control,
        resolve_auto_sell_field
    )
    from bot.callback_handlers import build_auto_sell_text, build_auto_sell_keyboard

    args = [a.strip().lower() for a in context.args if a.strip()]

    # กรณีไม่มี argument หรือพิมพ์ /auto_sell status / list / ดู
    if not args or args[0] in ("status", "list", "show", "ดู"):
        ctrl = get_auto_sell_controls()
        msg = build_auto_sell_text(ctrl)
        keyboard = build_auto_sell_keyboard(ctrl, view_mode="auto_sell")
        await update.message.reply_text(msg, parse_mode="HTML", reply_markup=keyboard)
        return

    # กรณีเปิด/ปิดทั้งหมด: /auto_sell on, /auto_sell off, /auto_sell all on, /auto_sell all off
    if args[0] in ("on", "เปิด", "enable") or (len(args) >= 2 and args[0] in ("all", "ทุกตัว", "ทั้งหมด") and args[1] in ("on", "เปิด", "enable")):
        set_all_auto_sell_controls(True)
        ctrl = get_auto_sell_controls()
        msg = (
            "⚡ <b>เปิดระบบตัดขายอัตโนมัติ (Auto-Sell) ทุกเงื่อนไขแล้ว</b>\n\n"
            + build_auto_sell_text(ctrl)
        )
        keyboard = build_auto_sell_keyboard(ctrl, view_mode="auto_sell")
        await update.message.reply_text(msg, parse_mode="HTML", reply_markup=keyboard)
        return

    if args[0] in ("off", "ปิด", "disable") or (len(args) >= 2 and args[0] in ("all", "ทุกตัว", "ทั้งหมด") and args[1] in ("off", "ปิด", "disable")):
        set_all_auto_sell_controls(False)
        ctrl = get_auto_sell_controls()
        msg = (
            "⛔ <b>ปิดระบบตัดขายอัตโนมัติ (Auto-Sell) ทุกเงื่อนไขแล้ว</b>\n"
            "<i>(เมื่อหุ้นหลุดเกณฑ์ ระบบจะส่งเพียง Alert Only แต่ไม่ส่งคำสั่งขาย)</i>\n\n"
            + build_auto_sell_text(ctrl)
        )
        keyboard = build_auto_sell_keyboard(ctrl, view_mode="auto_sell")
        await update.message.reply_text(msg, parse_mode="HTML", reply_markup=keyboard)
        return

    # กรณีระบุประเภท: /auto_sell <setting> [on|off]
    target_key = args[0]
    field, info = resolve_auto_sell_field(target_key)
    if not field:
        await update.message.reply_text(
            "⚠️ <b>ไม่พบเงื่อนไขที่ระบุ</b>\n\n"
            "📌 <b>เงื่อนไขที่รองรับ:</b>\n"
            "• <code>trailing</code> (หรือ <code>ts</code>) : Trailing Stop Loss\n"
            "• <code>stoploss</code> (หรือ <code>sl</code>) : Initial Stop Loss\n"
            "• <code>cutloss</code> (หรือ <code>cut</code>) : Hard Cut Loss (-10%)\n"
            "• <code>tech</code> (หรือ <code>signal</code>) : Technical Sell Signal\n"
            "• <code>all</code> : ทุกเงื่อนไข\n\n"
            "<b>ตัวอย่างคำสั่ง:</b>\n"
            "• <code>/auto_sell trailing on</code> (เปิด Auto Trailing Stop)\n"
            "• <code>/auto_sell trailing off</code> (ปิด Auto Trailing Stop)\n"
            "• <code>/auto_sell sl on</code>\n"
            "• <code>/auto_sell cutloss on</code>\n"
            "• <code>/auto_sell all off</code> (ปิดทั้งหมด)\n"
            "• <code>/auto_sell</code> (เปิดแผงปุ่มกด)",
            parse_mode="HTML"
        )
        return

    display_name = info["name"]

    if len(args) >= 2:
        action = args[1]
        if action in ("on", "open", "enable", "เปิด", "1", "true"):
            target_state = True
        elif action in ("off", "close", "disable", "ปิด", "0", "false"):
            target_state = False
        else:
            await update.message.reply_text(
                f"⚠️ กรุณาระบุ <code>on</code> หรือ <code>off</code> เช่น <code>/auto_sell {target_key} on</code>",
                parse_mode="HTML"
            )
            return
        set_auto_sell_control(field, target_state)
        new_val = target_state
    else:
        # หากไม่ระบุ on/off ให้ Toggle
        _, new_val, _, _ = toggle_auto_sell_control(field)

    state_icon = "🟢 <b>เปิดใช้งาน (ON)</b>" if new_val else "🔴 <b>ปิดใช้งาน (OFF)</b>"
    desc_extra = "บอทจะส่งคำสั่งขายทันทีที่เข้าเงื่อนไข" if new_val else "บอทจะแจ้งเตือน ⚠️ Alert Only เท่านั้น ไม่ส่งคำสั่งขาย"

    ctrl = get_auto_sell_controls()
    keyboard = build_auto_sell_keyboard(ctrl, view_mode="auto_sell")

    reply_msg = (
        f"⚙️ <b>อัปเดตสวิตช์ Auto-Sell สำเร็จ!</b>\n\n"
        f"• เงื่อนไข: <b>{display_name}</b>\n"
        f"• สถานะใหม่: {state_icon}\n"
        f"• ผลการทำงาน: <i>{desc_extra}</i>\n\n"
        f"👉 กดปุ่มด้านล่างเพื่อปรับเปลี่ยนเพิ่มเติม หรือพิมพ์ <code>/auto_sell</code>"
    )
    await update.message.reply_text(reply_msg, parse_mode="HTML", reply_markup=keyboard)

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
    req = HTTPXRequest(connect_timeout=20.0, read_timeout=60.0, write_timeout=20.0)
    app = ApplicationBuilder().token(TOKEN).request(req).post_init(post_init).build()
        # --- 1. ข้อมูลระบบและช่วยเหลือ (System & Help) ---
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("status", cmd_status))

    # --- 2. พอร์ตและผลการดำเนินงาน (Portfolio & Performance) ---
    app.add_handler(CommandHandler("port", cmd_port))
    app.add_handler(CommandHandler("port_chart", cmd_scan_port))
    app.add_handler(CommandHandler("scan_port", cmd_scan_port))
    app.add_handler(CommandHandler("history", cmd_history))
    app.add_handler(CommandHandler("trades", cmd_history))
    app.add_handler(CommandHandler("summary", cmd_summary))
    app.add_handler(CommandHandler("pnl", cmd_summary))
    app.add_handler(CommandHandler("performance", cmd_summary))

    # --- 3. สแกนหาหุ้นและเข้าซื้อ (Scanning & Buying) ---
    app.add_handler(CommandHandler("scan", cmd_scan))
    app.add_handler(CommandHandler("ai_scan", cmd_ai_scan))
    app.add_handler(CommandHandler("ai_credit", cmd_ai_credit))
    app.add_handler(CommandHandler("ai_quota", cmd_ai_credit))
    app.add_handler(CommandHandler("buy", cmd_buy))

    # --- 4. จัดการคำสั่งซื้อขาย & พอร์ต (Orders & Position Control) ---
    app.add_handler(CommandHandler("order", cmd_order))
    app.add_handler(CommandHandler("orders", cmd_order))
    app.add_handler(CommandHandler("sync_order", cmd_sync_orders))
    app.add_handler(CommandHandler("sync_orders", cmd_sync_orders))
    app.add_handler(CommandHandler("update_order", cmd_sync_orders))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("close_pos", cmd_close_pos))
    app.add_handler(CommandHandler("panic_close_all", cmd_panic_close_all))
    app.add_handler(CommandHandler("adopt", cmd_adopt))
    app.add_handler(CommandHandler("release", cmd_release))
    app.add_handler(CommandHandler("unadopt", cmd_release))

    # --- 5. ควบคุมความปลอดภัยและการบำรุงรักษา (Safety & Maintenance) ---
    app.add_handler(CommandHandler("auto_sell", cmd_auto_sell))
    app.add_handler(CommandHandler("autosell", cmd_auto_sell))
    app.add_handler(CommandHandler("sell_monitor", cmd_sell_monitor))
    app.add_handler(CommandHandler("check_sell", cmd_sell_monitor))
    app.add_handler(CommandHandler("update_port", cmd_update_port))
    app.add_handler(CommandHandler("task_update", cmd_task_update))
    app.add_handler(CommandHandler("update_data", cmd_task_update))
    app.add_handler(CommandHandler("exclude", cmd_exclude))
    app.add_handler(CommandHandler("exclude_add", cmd_exclude_add))
    app.add_handler(CommandHandler("exclude_del", cmd_exclude_del))
    app.add_handler(CommandHandler("notify_job", cmd_notify_job))
    app.add_handler(CommandHandler("holidays", cmd_holidays))
    app.add_handler(CommandHandler("holiday", cmd_holidays))
    app.add_handler(CallbackQueryHandler(handle_signal_callback)) # ดักฟังปุ่ม Inline Keyboard ของสัญญาณซื้อ

    print("🚀 Telegram Bot Service is running with Safety Controls...")
    app.run_polling()

if __name__ == "__main__":
    main()