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
              /cancel <ID> - ขอยกเลิกคำสั่งซื้อขายที่ค้างในตลาด
              /close_pos <SYM> - สั่งขายปิดสถานะหุ้นรายตัวทันที
              /panic_close_all - [Emergency] ขายล้างทุกไม้ที่บอทดูแลทันที
              /status      - แสดงสถานะการทำงานของบอทและสวิตช์ควบคุมความปลอดภัย
              /help        - แสดงคู่มือการใช้งานคำสั่งทั้งหมด
==============================================================================
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes
from dotenv import load_dotenv

import sys
from pathlib import Path
# ถอยกลับไป 1 โฟลเดอร์เพื่อชี้ไปที่ root (thai-stock-screener)
sys.path.append(str(Path(__file__).resolve().parent.parent))
from bot.callback_handlers import handle_signal_callback
from execution.order_manager import place_sell_order, cancel_order
from execution.settrade_executor import get_realtime_quote, recalculate_stop_loss
from core.tick_utils import adjust_price_by_ticks
import update.update_Port_info as uport_info
import update.updatePort as update_port
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

    # 3. ดึงยอด Line Available ล่าสุด
    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT line_available FROM public.account_info_history WHERE is_disabled = FALSE ORDER BY import_date DESC LIMIT 1;")
        acc = cur.fetchone()
        line_avail = float(acc["line_available"]) if acc else 0.0
    conn.close()

    caption = (
        f"🛒 <b>เตรียมส่งคำสั่งซื้อแบบกำหนดเอง: {sym}</b>\n"
        f"• ราคาตลาด: <code>{last_p:.2f}</code> THB (Bid: <code>{quote.get('bid', 0):.2f}</code> / Offer: <code>{best_offer:.2f}</code>)\n"
        f"• ราคาเสนอซื้อ: <code>{target_price:.2f}</code> THB\n"
        f"• Initial SL Plan: <code>{sl:.2f}</code> THB\n"
        f"• ยอดเงินที่ต้องใช้: <code>{cost:,.2f}</code> THB\n"
        f"• อำนาจซื้อคงเหลือ: <code>{line_avail:,.2f}</code> THB\n\n"
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
        "• /update_port - ซิงค์ยอดเงินสดจาก Settrade ล่าสุด\n"
        "• /order - ตรวจสอบสถานะคำสั่งซื้อขายวันนี้ทั้งหมด\n\n"
        "<b>🛒 การส่งและจัดการคำสั่ง:</b>\n"
        "• /scan - สั่งสแกนหาจังหวะซื้อและส่งกราฟใหม่\n"
        "• /buy &lt;SYM&gt; [VOL] [PRICE] - สั่งซื้อหุ้นแบบกำหนดเอง\n"
        "• /cancel &lt;ID&gt; - ขอยกเลิกคำสั่งที่รอคิวในตลาด\n\n"
        "<b>🛡️ การควบคุมความปลอดภัย:</b>\n"
        "• /close_pos &lt;SYM&gt; - ปิดสถานะหุ้นตัวที่ระบุทันที\n"
        "• /panic_close_all - 🚨 สั่งขายล้างทุกไม้ที่บอทถือทันที\n"
        "• /help - ดูคู่มือการใช้งานคำสั่งทั้งหมดอย่างละเอียด\n",
        parse_mode="HTML"
    )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/help - แสดงคู่มือการใช้งานคำสั่งทั้งหมด"""
    msg = (
        "📖 <b>คู่มือการใช้งาน Stock Trading Bot</b>\n\n"
        "📊 <b>ข้อมูลพอร์ตและการเงิน:</b>\n"
        "• /port - ดูพอร์ตจริง, พอร์ตจำลอง, กำไร/ขาดทุน และเงินสดคงเหลือ\n"
        "• /update_port - ซิงค์ยอดเงินสดล่าสุดจาก Settrade API\n"
        "• /status - ตรวจสอบโหมดเทรด (DRY_RUN/LIVE) และสวิตช์ความปลอดภัย\n"
        "• /order - ตรวจสอบรายการและสถานะคำสั่งซื้อขายของวันนี้ทั้งหมด\n\n"
        "🛒 <b>การส่งและจัดการคำสั่ง:</b>\n"
        "• /scan - สั่งสแกนหาจังหวะซื้อและส่งกราฟขออนุมัติ\n"
        "• /buy &lt;SYM&gt; [VOL] [PRICE] - ส่งคำสั่งซื้อหุ้นแบบกำหนดเอง\n"
        "• /cancel &lt;ORDER_ID&gt; - ขอยกเลิกคำสั่งซื้อขายที่รอคิวในตลาด\n\n"
        "🛡️ <b>การควบคุมความปลอดภัย:</b>\n"
        "• /close_pos &lt;SYM&gt; - สั่งขายปิดสถานะหุ้นรายตัวทันที\n"
        "• /panic_close_all - 🚨 ขายล้างทุกไม้ที่บอทดูแลทันที\n"
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

async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔍 กำลังเริ่มกระบวนการสแกนและสร้างกราฟแจ้งเตือน...")
    try:
        await scan_and_notify()
    except Exception as e:
        await update.message.reply_text(f"❌ เกิดข้อผิดพลาดในการสแกน: {e}")

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

            # 2. ดึงพอร์ตจริงทั้งหมด (มี percent_profit และ stop loss)
            cur.execute("""
                SELECT symbol, current_volume, average_price, market_price, 
                       percent_profit, initial_stop_loss, is_managed_by_bot
                FROM public.v_portfolio_with_signals
                WHERE current_volume > 0
                ORDER BY percent_profit DESC;
            """)
            real_rows = cur.fetchall()

            # 3. ดึงไม้จำลอง (DRY-RUN) ที่บอทถืออยู่แต่ยังไม่มีในพอร์ตจริง
            cur.execute("""
                SELECT b.symbol, b.current_volume, b.entry_price, b.initial_stop_loss,
                       COALESCE(p.close, b.entry_price) AS market_price,
                       ROUND(((COALESCE(p.close, b.entry_price) - b.entry_price) / b.entry_price * 100)::numeric, 2) AS pnl_pct
                       --,b.is_managed_by_bot
                FROM public.bot_active_positions b
                LEFT JOIN (
                    SELECT symbol, close FROM public.stock_price_history
                    WHERE date = (SELECT MAX(date) FROM public.stock_price_history)
                ) p ON b.symbol = p.symbol
                WHERE b.status = 'OPEN'
                  AND b.symbol NOT IN (
                      SELECT symbol FROM public.portfolio_stock 
                      WHERE imported_at = (SELECT MAX(imported_at) FROM public.portfolio_stock) 
                        AND current_volume > 0
                  );
            """)
            dry_rows = cur.fetchall()

            msg = (
                f"💼 <b>ภาพรวมสถานะการเงิน</b>\n"
                f"• อำนาจซื้อ (Line Available): <code>{line_avail:,.2f}</code> THB\n"
                f"• เงินสดคงเหลือ (Cash): <code>{cash_bal:,.2f}</code> THB\n"
                f"------------------------------------\n"
                f"📊 <b>พอร์ตจริงในบัญชี (Real Portfolio): {len(real_rows)}</b>\n"
            )

            if not real_rows:
                msg += "<i>ไม่มีหุ้นถือครองในพอร์ตจริง</i>\n"
            else:
                num = 1
                for r in real_rows:
                  tag = "🤖 [BOT]" if r["is_managed_by_bot"] else "👤 [MANUAL]"
                  sl_str = f"{float(r['initial_stop_loss']):.2f}" if r["initial_stop_loss"] else "N/A"
                  pnl = float(r["percent_profit"]) if r["percent_profit"] is not None else 0.0
                  msg += (
                      f"\nNo.{num}: {tag} <b>{r['symbol']}</b> ({r['current_volume']:,} หุ้น)\n"
                      f"• ทุน: <code>{float(r['average_price']):.2f}</code> | ตลาด: <code>{float(r['market_price']):.2f}</code>\n"
                      f"• กำไร/ขาดทุน: <code>{pnl:+.2f}%</code>\n"
                      # f"• กำไร/ขาดทุน: <code>{float(r['percent_profit']):+.2f}%</code>\n"
                      f"• Initial SL: <code>{sl_str}</code> THB\n"
                      # f"• ทุน: <code>{float(r['entry_price']):.2f}</code> | ตลาด: <code>{float(r['market_price']):.2f}</code>\n"
                      # f"• PnL: <code>{float(r['pnl_pct']):+.2f}%</code> | SL: <code>{float(r['initial_stop_loss']):.2f}</code>\n\n"
                  )
                  num += 1
            if dry_rows:
                msg += "\n------------------------------------\n"
                msg += "🧪 <b>ไม้จำลองที่บอทถืออยู่ (Dry-Run Positions):</b>\n"
                for d in dry_rows:
                    msg += (
                        f"\n🤖 <b>{d['symbol']}</b> ({d['current_volume']:,} หุ้น)\n"
                        f"• ทุนจำลอง: <code>{float(d['entry_price']):.2f}</code> | ตลาด: <code>{float(d['market_price']):.2f}</code>\n"
                        f"• กำไร/ขาดทุน: <code>{float(d['pnl_pct']):+.2f}%</code>\n"
                        f"• Initial SL: <code>{float(d['initial_stop_loss']):.2f}</code> THB\n"
                    )

            await update.message.reply_text(msg, parse_mode="HTML")
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


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    '''/status - แสดงสถานะการทำงานของบอทและสวิตช์ควบคุมความปลอดภัย'''
    from bot.callback_handlers import load_config
    config = load_config()
    
    t_mode = config.get("trading_mode", {})
    mode_str = "🧪 DRY_RUN (จำลองคำสั่ง)" if t_mode.get("dry_run", True) else "🔴 LIVE (เงินจริง)"
    
    ctrl = config.get("auto_sell_controls", {})
    cut_loss_str = "🟢 เปิด (ON)" if ctrl.get("enable_auto_cut_loss") else "🔴 ปิด (OFF)"
    stop_loss_str = "🟢 เปิด (ON)" if ctrl.get("enable_auto_stop_loss") else "🔴 ปิด (OFF)"
    trail_str = "🟢 เปิด (ON)" if ctrl.get("enable_auto_trailing_stop") else "🔴 ปิด (OFF)"
    tech_str = "🟢 เปิด (ON)" if ctrl.get("enable_auto_technical_sell") else "🔴 ปิด (OFF)"

    excluded_list = config.get("excluded_symbols", [])

    msg = (
        f"⚙️ <b>สถานะการทำงานของระบบบอท</b>\n"
        f"• โหมดเทรดปัจจุบัน: <b>{mode_str}</b>\n"
        f"------------------------------------\n"
        f"🛡️ <b>สถานะสวิตช์ขายอัตโนมัติ:</b>\n"
        f"• Hard Cut Loss (-10%): {cut_loss_str}\n"
        f"• Initial Stop Loss: {stop_loss_str}\n"
        f"• Trailing Stop Loss: {trail_str}\n"
        f"• Technical Sell Signal: {tech_str}\n"
        f"------------------------------------\n"
        f"🔒 <b>หุ้นที่ได้รับการยกเว้น ({len(excluded_list)} ตัว):</b>\n"
        f"<code>{', '.join(excluded_list)}</code>"
    )
    await update.message.reply_text(msg, parse_mode="HTML")

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
    app.add_handler(CommandHandler("buy", cmd_buy))
    app.add_handler(CommandHandler("order", cmd_order))
    app.add_handler(CommandHandler("orders", cmd_order))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("close_pos", cmd_close_pos))
    app.add_handler(CommandHandler("panic_close_all", cmd_panic_close_all))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CallbackQueryHandler(handle_signal_callback)) # ดักฟังปุ่ม Inline Keyboard ของสัญญาณซื้อ

    print("🚀 Telegram Bot Service is running with Safety Controls...")
    app.run_polling()

if __name__ == "__main__":
    main()