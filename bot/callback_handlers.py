#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : callback_handlers.py
LOCATION    : bot/
OCCASION    : ทำงานเบื้องหลังเมื่อมีผู้ใช้กดปุ่ม Inline Keyboard บน Telegram
              เมื่อมีการกดปุ่มผ่าน Telegram โมดูลนี้จะอัปเดตสถานะของสัญญาณในตาราง bot_trade_signals และเตรียมคำสั่งส่งเข้า bot_orders:
DESCRIPTION :  ตัวจัดการปุ่มกดโต้ตอบทั้งหมด:
              1. ระบบ Auto Scanner:
                 - ปรับหุ้น (adj): ตรวจสอบเพดานหุ้นและเงินสดคงเหลือสดๆ
                 - อนุมัติ (app): ดึง ATR14 คำนวณความเสี่ยง และส่งคำสั่งผ่าน order_manager
                 - ปฏิเสธ (rej): อัปเดตสถานะเป็น REJECTED
              2. ระบบ Manual Buy (/buy):
                 - ปรับราคาตาม Tick (mbuy_p)
                 - ปรับจำนวนหุ้น (mbuy_v)
                 - ยืนยันซื้อ (mbuy_conf)
                 - ยกเลิก (mbuy_cancel)
==============================================================================
"""

import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from dotenv import load_dotenv

from execution.order_manager import place_buy_order
from execution.settrade_executor import recalculate_stop_loss
from core.audit_logger import log_event

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
    ''' สร้าง Inline Keyboard สำหรับปรับจำนวนหุ้นและอนุมัติ/ปฏิเสธสัญญาณซื้อ
    ใช้ฟังก์ชันเดียวกับ jobs/run_buy_scanner.py เพื่อให้ปรับจำนวนหุ้นและราคาตรงกันระหว่างการสแกนและการอนุมัติ '''
    from jobs.run_buy_scanner import build_trade_keyboard as b_kbd
    return b_kbd(signal_id, current_shares, price)

async def handle_signal_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data.split(":")
    action = data[0]

    if action == "noop":
        await query.answer()
        return

    if action == "has_pending":
        await query.answer("⚠️ หุ้นตัวนี้มีคำสั่งซื้อของวันนี้หรือรอคิวในตลาดแล้ว (ปิดปุ่ม Approve)", show_alert=True)
        return

    if action == "toggle_job_notify":
        from core.job_notifier import set_job_notification_enabled
        target_state = (data[1] == "on")
        set_job_notification_enabled(target_state)

        st_text = "🟢 เปิดใช้งานอยู่ (ON)" if target_state else "🔴 ปิดใช้งานอยู่ (OFF)"
        next_action = "off" if target_state else "on"
        btn_text = "🔕 กดเพื่อปิดการแจ้งเตือน" if target_state else "🔔 กดเพื่อเปิดการแจ้งเตือน"

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(btn_text, callback_data=f"toggle_job_notify:{next_action}")]
        ])

        alert_msg = "🔔 เปิดการแจ้งเตือน Background Jobs แล้ว" if target_state else "🔕 ปิดการแจ้งเตือน Background Jobs แล้ว"
        await query.answer(alert_msg)

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
        try:
            await query.edit_message_text(msg, parse_mode="HTML", reply_markup=keyboard)
        except Exception:
            pass
        return

    if action == "exclude_del":
        from core.config_manager import remove_excluded_symbols
        sym = data[1].upper()
        removed, not_found, full_list = remove_excluded_symbols([sym])
        await query.answer(f"นำ {sym} ออกจาก Excluded Symbols แล้ว", show_alert=True)
        sym_list_str = ", ".join(full_list) if full_list else "(ไม่มี)"
        new_msg = (
            f"🗑️ <b>นำหุ้น {sym} ออกจาก Excluded Symbols เรียบร้อยแล้ว</b>\n\n"
            f"🔒 <b>รายชื่อหุ้นที่ได้รับการยกเว้นปัจจุบัน ({len(full_list)} ตัว):</b>\n"
            f"<code>{sym_list_str}</code>\n\n"
            f"👉 เพิ่มหุ้น: <code>/exclude add &lt;SYM&gt;</code>\n"
            f"👉 ลบหุ้น: <code>/exclude del &lt;SYM&gt;</code>"
        )
        try:
            await query.edit_message_text(new_msg, parse_mode="HTML")
        except Exception:
            pass
        return

    if action == "exclude_add":
        from core.config_manager import add_excluded_symbols
        sym = data[1].upper()
        added, already_in, full_list = add_excluded_symbols([sym])
        await query.answer(f"เพิ่ม {sym} เข้า Excluded Symbols แล้ว", show_alert=True)
        sym_list_str = ", ".join(full_list) if full_list else "(ไม่มี)"
        new_msg = (
            f"✅ <b>เพิ่มหุ้น {sym} เข้าสู่ Excluded Symbols เรียบร้อยแล้ว</b>\n\n"
            f"🔒 <b>รายชื่อหุ้นที่ได้รับการยกเว้นปัจจุบัน ({len(full_list)} ตัว):</b>\n"
            f"<code>{sym_list_str}</code>\n\n"
            f"👉 เพิ่มหุ้น: <code>/exclude add &lt;SYM&gt;</code>\n"
            f"👉 ลบหุ้น: <code>/exclude del &lt;SYM&gt;</code>"
        )
        try:
            await query.edit_message_text(new_msg, parse_mode="HTML")
        except Exception:
            pass
        return

    # =========================================================================
    # ส่วนที่ 1: ระบบสั่งซื้อแบบกำหนดเองผ่านคำสั่ง /buy (Manual Buy)
    # =========================================================================
    if action.startswith("mbuy_"):
        from bot.telegram_app import build_manual_buy_keyboard

        if action == "mbuy_cancel":
            await query.answer()
            await query.edit_message_text("❌ <b>ยกเลิกคำสั่งซื้อเรียบร้อยแล้ว</b>", parse_mode="HTML")
            return

        sym = data[1]
        shares = int(data[2])
        price = float(data[3])

        if action in ("mbuy_p", "mbuy_v"):
            if shares < 100:
                await query.answer("⚠️ จำนวนหุ้นขั้นต่ำคือ 100 หุ้น", show_alert=True)
                return
            if price <= 0:
                await query.answer("⚠️ ราคาไม่ถูกต้อง", show_alert=True)
                return

            sl = recalculate_stop_loss(price)
            cost = round(shares * price * 1.0025, 2)

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

            new_caption = (
                f"🛒 <b>เตรียมส่งคำสั่งซื้อแบบกำหนดเอง: {sym}</b>\n"
                f"• ราคาเสนอซื้อ: <code>{price:.2f}</code> THB\n"
                f"• Initial SL Plan: <code>{sl:.2f}</code> THB\n"
                f"• ยอดเงินที่ต้องใช้: <code>{cost:,.2f}</code> THB\n"
                f"• อำนาจซื้อคงเหลือ: <code>{line_avail:,.2f}</code> THB"
                f"{pending_warning}\n\n"
                f"<i>ปรับราคาหรือจำนวนหุ้นด้านล่างก่อนกดยืนยัน:</i>"
            )
            new_kbd = build_manual_buy_keyboard(sym, shares, price)
            await query.answer()
            await query.edit_message_text(new_caption, parse_mode="HTML", reply_markup=new_kbd)
            return

        elif action == "mbuy_conf":
            sl = recalculate_stop_loss(price)
            await query.answer("⏳ กำลังตรวจสอบวงเงินและส่งคำสั่งซื้อ...")

            exec_res = place_buy_order(
                signal_id=None,
                symbol=sym,
                volume=shares,
                target_price=price,
                stop_loss_plan=sl,
                atr14=None,
                is_fixed_price=True
            )

            if exec_res.get("success"):
                mode = "DRY_RUN" if "SIM_" in exec_res.get("broker_order_no", "") else "LIVE"
                await query.edit_message_text(
                    f"✅ <b>ส่งคำสั่งซื้อสำเร็จ [{mode}]</b>\n"
                    f"• หุ้น: <b>{sym}</b>\n• จำนวน: <code>{shares:,}</code> หุ้น\n"
                    f"• ราคาเสนอซื้อ: <code>{exec_res.get('buy_price', price):.2f}</code> THB\n"
                    f"• Initial SL: <code>{exec_res.get('stop_loss', sl):.2f}</code> THB\n"
                    f"• Order No: <code>{exec_res.get('broker_order_no')}</code>\n\n"
                    f"<i>(คำสั่งถูกส่งเข้าตลาดและบันทึกลงระบบเรียบร้อย)</i>",
                    parse_mode="HTML"
                )
            else:
                await query.edit_message_text(f"❌ <b>ส่งคำสั่งซื้อไม่สำเร็จ:</b>\n\n{exec_res.get('error')}", parse_mode="HTML")
            return

    # =========================================================================
    # ส่วนที่ 1.5: ระบบสั่งขายผ่านการ์ดสแกนพอร์ต (/scan_port) (Manual Sell Panel)
    # =========================================================================
    if action.startswith("msell_"):
        from bot.telegram_app import build_manual_sell_keyboard, build_port_card_keyboard
        from execution.order_manager import place_sell_order

        if action == "msell_cancel":
            sym = data[1]
            max_shares = int(data[2])
            price = float(data[3])
            orig_kbd = build_port_card_keyboard(sym, max_shares, price)
            await query.answer("ยกเลิกหน้าต่างขาย")
            await query.edit_message_reply_markup(reply_markup=orig_kbd)
            return

        if action == "msell_open":
            sym = data[1]
            max_shares = int(data[2])
            price = float(data[3])
            sell_kbd = build_manual_sell_keyboard(sym, max_shares, max_shares, price)
            await query.answer("เปิดหน้าต่างตั้งราคาขาย")
            await query.edit_message_reply_markup(reply_markup=sell_kbd)
            return

        if action in ("msell_p", "msell_v", "msell_pct"):
            sym = data[1]
            shares = int(data[2])
            max_shares = int(data[3])
            price = float(data[4])

            if shares < 100:
                await query.answer("⚠️ จำนวนหุ้นขั้นต่ำคือ 100 หุ้น", show_alert=True)
                return
            if shares > max_shares:
                await query.answer(f"⚠️ เกินจำนวนที่ถือครอง ({max_shares:,} หุ้น)", show_alert=True)
                return
            if price <= 0:
                await query.answer("⚠️ ราคาไม่ถูกต้อง", show_alert=True)
                return

            sell_kbd = build_manual_sell_keyboard(sym, shares, max_shares, price)
            await query.answer()
            await query.edit_message_reply_markup(reply_markup=sell_kbd)
            return

        if action == "msell_conf":
            sym = data[1]
            shares = int(data[2])
            max_shares = int(data[3])
            price = float(data[4])

            await query.answer("⏳ กำลังส่งคำสั่งขาย...")
            exec_res = place_sell_order(
                symbol=sym,
                volume=shares,
                exit_price=price,
                exit_reason="MANUAL_CARD_SELL"
            )

            if exec_res.get("success"):
                mode = "DRY_RUN" if "SIM_" in exec_res.get("broker_order_no", "") else "LIVE"
                b_no = exec_res.get("broker_order_no", "-")
                est_val = shares * price

                caption_note = (
                    f"\n\n🚨 <b>ส่งคำสั่งขายสำเร็จ [{mode}]</b>\n"
                    f"• หุ้น: <b>{sym}</b>\n"
                    f"• จำนวนขาย: <code>{shares:,}</code> หุ้น @ <code>{price:.2f}</code> THB (~{est_val:,.0f} บ.)\n"
                    f"• Order No: <code>{b_no}</code>\n"
                    f"<i>(คำสั่งถูกส่งเข้าตลาดและบันทึกสถานะเรียบร้อย)</i>"
                )

                rem_shares = max_shares - shares
                if rem_shares >= 100:
                    new_reply_kbd = build_port_card_keyboard(sym, rem_shares, price)
                else:
                    new_reply_kbd = None

                try:
                    if query.message.caption:
                        new_caption = query.message.caption + caption_note
                        await query.edit_message_caption(caption=new_caption, parse_mode="HTML", reply_markup=new_reply_kbd)
                    else:
                        new_text = (query.message.text or "") + caption_note
                        await query.edit_message_text(text=new_text, parse_mode="HTML", reply_markup=new_reply_kbd)
                except Exception as edit_err:
                    print(f"⚠️ Edit message on msell_conf warning: {edit_err}")
                    await query.message.reply_text(caption_note, parse_mode="HTML")
            else:
                await query.answer(f"❌ ส่งคำสั่งขายไม่สำเร็จ: {exec_res.get('error')}", show_alert=True)
            return

    # =========================================================================
    # ส่วนที่ 2: ระบบตรวจจับและอนุมัติของ Auto Scanner (สัญญาณประจำวัน)
    # =========================================================================
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 1. การปรับจำนวนหุ้น (adj:signal_id:new_shares)
            if action == "adj":
                signal_id = int(data[1])
                target_shares = int(data[2])

                # กฎความปลอดภัย 1: ห้ามต่ำกว่า Board Lot 100 หุ้น
                if target_shares < 100:
                    await query.answer("⚠️ จำนวนหุ้นขั้นต่ำคือ 100 หุ้น", show_alert=True)
                    return

                # กฎความปลอดภัย 2: ตรวจสอบเพดานสูงสุดต่อไม้ตาม config
                config = load_config()
                max_shares = config.get("budget_limits", {}).get("max_shares_per_trade", 200)
                if target_shares > max_shares:
                    await query.answer(f"⚠️ เกินเพดานสูงสุด ({max_shares:,} หุ้น)", show_alert=True)
                    return

                cur.execute("SELECT trigger_price, symbol FROM public.bot_trade_signals WHERE id = %s;", (signal_id,))
                sig = cur.fetchone()
                p = float(sig["trigger_price"])
                est_cost = target_shares * p * 1.0025

                # กฎความปลอดภัย 3: เช็คกับ Line Available ล่าสุดสดๆ ป้องกันเงินไม่พอ
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

                # ดึงสัญญาณพร้อม Join หาค่า ATR14 และตรวจเช็คสถานะการหมดอายุ (Auto-Expire)
                cur.execute("""
                    SELECT s.*, i.atr14,
                           (s.expired_at IS NOT NULL AND s.expired_at < timezone('Asia/Bangkok', now())) AS is_expired
                    FROM public.bot_trade_signals s
                    LEFT JOIN public.mv_stock_indicators i 
                        ON s.symbol = i.symbol AND s.trade_date = i.trade_date
                    WHERE s.id = %s;
                """, (signal_id,))
                sig = cur.fetchone()

                if not sig or sig["status"] != "PENDING":
                    await query.answer("⚠️ สัญญาณนี้ไม่อยู่ในสถานะรอดำเนินการ", show_alert=True)
                    return

                sym = sig["symbol"]

                # กฎความปลอดภัย 1: ตรวจเช็คการหมดอายุของสัญญาณ (Signal Expiry Check)
                if sig.get("is_expired"):
                    cur.execute("UPDATE public.bot_trade_signals SET status = 'EXPIRED' WHERE id = %s;", (signal_id,))
                    conn.commit()
                    log_event(
                        event_type="EXPIRED_SIGNAL",
                        message=f"ปฏิเสธการอนุมัติ: สัญญาณ {sym} หมดอายุแล้ว",
                        symbol=sym,
                        level="WARNING"
                    )
                    await query.answer("⌛ สัญญาณนี้หมดอายุแล้ว ไม่สามารถอนุมัติได้", show_alert=True)
                    await query.edit_message_caption(
                        caption=f"⌛ <b>สัญญาณหมดอายุ (Expired): {sym}</b>\n<i>(สัญญาณนี้เลยเวลาที่กำหนดแล้ว ระบบยกเลิกอัตโนมัติ)</i>",
                        parse_mode="HTML"
                    )
                    return

                p = float(sig["trigger_price"])
                sl = float(sig["stop_loss_plan"])
                atr = float(sig["atr14"]) if sig.get("atr14") else None
                est_cost = round(final_shares * p * 1.0025, 2)

                # กฎความปลอดภัย 2: ตรวจเช็ค Line Available สดๆ ณ วินาทีที่กด Approve (Race Condition Protection)
                cur.execute("""
                    SELECT line_available 
                    FROM public.account_info_history 
                    WHERE is_disabled = FALSE 
                    ORDER BY import_date DESC LIMIT 1;
                """)
                acc = cur.fetchone()
                line_avail = float(acc["line_available"]) if acc else 0.0

                if est_cost > line_avail:
                    diff = est_cost - line_avail
                    err_msg = f"❌ เงินสดไม่พอ! ต้องการ {est_cost:,.2f} บ. (มี {line_avail:,.2f} บ. ขาดอีก {diff:,.2f} บ.)"
                    log_event(
                        event_type="REJECTED_INSUFFICIENT_CASH",
                        message=f"ปฏิเสธการอนุมัติ {sym}: เงินสดไม่พอ (ต้องการ {est_cost:,.2f} / มี {line_avail:,.2f})",
                        symbol=sym,
                        level="WARNING"
                    )
                    await query.answer(err_msg, show_alert=True)
                    return

                # กฎความปลอดภัย 3: ตรวจสอบคำสั่งซื้อค้างรอคิวอยู่ในตลาด (Duplicate Pending Buy Protection)
                cur.execute("""
                    SELECT order_id, broker_order_no, status 
                    FROM public.bot_orders 
                    WHERE symbol = %s AND side = 'BUY' AND status IN ('SENT', 'QUEUING', 'PARTIAL');
                """, (sym,))
                pending_buy = cur.fetchone()
                if pending_buy:
                    await query.answer(f"⚠️ มีคำสั่งซื้อ {sym} ค้างรอในตลาดแล้ว (#{pending_buy['broker_order_no']})", show_alert=True)
                    return

                # ส่งคำสั่งผ่าน Order Manager (Dry-run หรือ Real Trade ตาม Config)
                exec_result = place_buy_order(
                    signal_id=signal_id,
                    symbol=sym,
                    volume=final_shares,
                    target_price=p,
                    stop_loss_plan=sl,
                    atr14=atr,
                    is_fixed_price=False
                )

                if exec_result.get("success"):
                    # อัปเดตสัญญาณเป็น APPROVED เฉพาะเมื่อส่งคำสั่งสำเร็จจริง
                    cur.execute("UPDATE public.bot_trade_signals SET status = 'APPROVED', recommended_shares = %s WHERE id = %s;", (final_shares, signal_id))
                    conn.commit()
                    
                    mode_label = "DRY_RUN (จำลอง)" if "SIM_" in exec_result.get("broker_order_no", "") else "LIVE (ส่งจริง)"
                    await query.edit_message_caption(
                        caption=f"✅ <b>อนุมัติและเปิดสถานะสำเร็จ [{mode_label}]</b>\n"
                                f"• หุ้น: <b>{sym}</b>\n"
                                f"• จำนวน: <code>{final_shares:,}</code> หุ้น\n"
                                f"• ราคาตั้งซื้อ: <code>{exec_result.get('buy_price', p):.2f}</code> THB\n"
                                f"• Initial SL: <code>{exec_result.get('stop_loss', sl):.2f}</code> THB\n"
                                f"• Order No: <code>{exec_result.get('broker_order_no')}</code>\n\n"
                                f"<i>(ระบบเริ่มติดตามสถานะและคุม Stop Loss ใน bot_active_positions แล้ว)</i>",
                        parse_mode="HTML"
                    )
                else:
                    await query.answer("❌ ส่งคำสั่งซื้อไม่สำเร็จ!", show_alert=True)
                    await query.edit_message_caption(
                        caption=f"❌ <b>ส่งคำสั่งซื้อล้มเหลว ({sym})</b>\nสาเหตุ: {exec_result.get('error')}",
                        parse_mode="HTML"
                    )

            # 3. กดยกเลิก/ปฏิเสธสัญญาณ (rej:signal_id)
            elif action == "rej":
                signal_id = int(data[1])
                cur.execute("SELECT symbol FROM public.bot_trade_signals WHERE id = %s;", (signal_id,))
                row = cur.fetchone()
                sym = row["symbol"] if row else "UNKNOWN"

                cur.execute("UPDATE public.bot_trade_signals SET status = 'REJECTED' WHERE id = %s;", (signal_id,))
                conn.commit()

                log_event(
                    event_type="REJECTED_SIGNAL",
                    message=f"ผู้ใช้กดปฏิเสธสัญญาณ {sym}",
                    symbol=sym,
                    level="INFO"
                )

                await query.edit_message_caption(
                    caption=f"❌ <b>ปฏิเสธสัญญาณ {sym} เรียบร้อยแล้ว</b>",
                    parse_mode="HTML"
                )
    finally:
        conn.close()