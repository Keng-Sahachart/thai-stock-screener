#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : run_buy_scanner.py
LOCATION    : jobs/
OCCASION    : รันเป็น Job ประจำวัน รันช่วงเช้าก่อนตลาดเปิด (09:00 น.) หรือรันตอนค่ำหลังอัปเดตข้อมูลเสร็จ
DESCRIPTION : สคริปต์สแกนและยิงแจ้งเตือนพร้อมกราฟ (jobs/run_buy_scanner.py)
              1. สแกนหาหุ้นผ่าน Aggregator
              2. ส่งต่อให้ AI Router วิเคราะห์ 3 มิติ (ขึ้นจริง/หลอก, งบการเงิน, Risk/Reward)
              3. ส่ง Scorecard สรุปผลเข้า Telegram ตามระดับความละเอียด (1=MINIMAL, 2=COMPACT, 3=DETAILED)
              4. รองรับ Auto-buy อัตโนมัติหาก auto_buy_on_top_pick เปิดใช้งาน
==============================================================================
"""

import os
import html
import asyncio
import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.request import HTTPXRequest 
from dotenv import load_dotenv

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import time
from core.signals.aggregator import run_signal_aggregator
from bot.chart_generator import generate_stock_chart
from core.job_notifier import notify_job_start, notify_job_finish
from core.ai.router import evaluate_candidates, load_config
from core.ai.formatter import format_ai_evaluation_html, parse_detail_level
from execution.order_manager import place_buy_order

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

def build_trade_keyboard(signal_id: int, current_shares: int, price: float, pending_order: dict = None):
    if pending_order:
        order_ref = pending_order.get('broker_order_no') or f"#{pending_order.get('order_id')}"
        keyboard = [
            [
                InlineKeyboardButton(f"🔒 มี Order #{order_ref} แล้ว ({pending_order['status']})", callback_data=f"has_pending:{signal_id}")
            ],
            [
                InlineKeyboardButton("❌ ปิดการ์ดนี้", callback_data=f"rej:{signal_id}")
            ]
        ]
        return InlineKeyboardMarkup(keyboard)

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

async def scan_and_notify(
    run_ai: bool = False,
    detail_level: int = None,
    target_symbols: list = None,
    chat_id: str = None,
    bot_instance = None
):
    start_t = time.time()
    job_desc = "สแกนหาจังหวะซื้อและประเมินด้วย AI" if run_ai else "สแกนหาจังหวะซื้อทางเทคนิคอลและสร้างการ์ดขออนุมัติ"
    await notify_job_start("Run Buy Scanner", job_desc)
    
    if not target_symbols:
        print("🔍 กำลังรัน Aggregator...")
        run_signal_aggregator()

    bot = bot_instance or Bot(token=TOKEN, request=HTTPXRequest(connect_timeout=20.0, read_timeout=60.0, write_timeout=60.0))
    target_chat = chat_id or CHAT_ID
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if target_symbols:
                cur.execute("""
                    SELECT s.*, a.line_available 
                    FROM public.bot_trade_signals s
                    CROSS JOIN (
                        SELECT line_available FROM public.account_info_history 
                        WHERE is_disabled = FALSE ORDER BY import_date DESC LIMIT 1
                    ) a
                    WHERE s.status = 'PENDING' 
                      AND s.trade_date = (SELECT MAX(trade_date) FROM public.bot_trade_signals WHERE status = 'PENDING')
                      AND s.symbol = ANY(%s)
                    ORDER BY s.priority DESC, s.id ASC;
                """, (target_symbols,))
            else:
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
                await bot.send_message(
                    chat_id=target_chat,
                    text="🛡️ <b>ผลการสแกนประจำวัน</b>\nไม่พบสัญญาณซื้อใหม่ที่ผ่านเกณฑ์ในขณะนี้",
                    parse_mode="HTML"
                )
                await notify_job_finish("Run Buy Scanner", elapsed_seconds=time.time() - start_t, summary="ไม่พบสัญญาณซื้อใหม่ที่ผ่านเกณฑ์")
                return

            config = load_config()
            ai_cfg = config.get("ai_evaluation", {})
            ai_lookup = {}

            # =================================================================
            # 🤖 AI ROUTER EVALUATION LAYER (เฉพาะเมื่อ run_ai=True)
            # =================================================================
            if run_ai and ai_cfg.get("enabled", True):
                chosen_level = parse_detail_level(detail_level or ai_cfg.get("default_detail_level", 1))
                cand_symbols = [s["symbol"] for s in signals]
                target_date = str(signals[0]["trade_date"])

                print(f"🤖 กำลังส่งหุ้น {len(cand_symbols)} ตัวให้ AI Router ประเมิน (ระดับ {chosen_level})...")
                ai_result = evaluate_candidates(symbols=cand_symbols, target_date=target_date, conn=conn)

                if ai_result and "evaluations" in ai_result:
                    for ev in ai_result.get("evaluations", []):
                        ai_lookup[ev.get("symbol")] = ev

                    # ส่งการ์ดสรุปผล AI Scorecard เข้า Telegram
                    scorecard_html = format_ai_evaluation_html(ai_result, detail_level=chosen_level)
                    await bot.send_message(chat_id=target_chat, text=scorecard_html, parse_mode="HTML")

                    # ตรวจสอบสวิตช์ Auto-buy เมื่อมี Top Pick ผ่านเกณฑ์
                    if ai_cfg.get("auto_buy_on_top_pick", False):
                        top_pick = ai_result.get("top_pick", "NONE")
                        top_ev = ai_lookup.get(top_pick, {})
                        min_score = ai_cfg.get("min_confidence_score", 80)

                        if top_pick != "NONE" and top_ev.get("decision") == "BUY" and top_ev.get("confidence_score", 0) >= min_score:
                            top_sig = next((s for s in signals if s["symbol"] == top_pick), None)
                            if top_sig:
                                print(f"🚀 [AUTO BUY TRIGGERED] ยิงคำสั่งซื้อ {top_pick} อัตโนมัติ (คะแนน: {top_ev.get('confidence_score')})...")
                                order_res = place_buy_order(
                                    signal_id=top_sig["id"],
                                    symbol=top_pick,
                                    volume=top_sig["recommended_shares"],
                                    target_price=float(top_sig["trigger_price"]),
                                    stop_loss_plan=float(top_sig["stop_loss_plan"])
                                )
                                if order_res.get("status") == "SUCCESS":
                                    auto_msg = (
                                        f"🤖 <b>[AI AUTO BUY EXECUTED]</b>\n"
                                        f"• สั่งซื้อ: <b>{top_pick}</b> สำเร็จอัตโนมัติ!\n"
                                        f"• คะแนนความมั่นใจ: <code>{top_ev.get('confidence_score')}/100</code>\n"
                                        f"• จำนวน: <code>{top_sig['recommended_shares']:,}</code> หุ้น @ <code>{float(top_sig['trigger_price']):.2f}</code> THB\n"
                                        f"• เหตุผล: <i>{top_ev.get('verdict', '')}</i>"
                                    )
                                    await bot.send_message(chat_id=target_chat, text=auto_msg, parse_mode="HTML")

            # =================================================================
            # 📊 INDIVIDUAL CANDIDATE CARDS (พร้อมกราฟ)
            # =================================================================
            for sig in signals:
                sym = sig["symbol"]
                p = float(sig["trigger_price"])
                shares = int(sig["recommended_shares"])
                line_avail = float(sig["line_available"] or 0)
                cost = shares * p * 1.0025

                # ตรวจสอบว่ามีคำสั่งซื้อของวันนี้ หรือรอคิวอยู่ในตลาดแล้วหรือไม่
                cur.execute("""
                    SELECT order_id, broker_order_no, status, volume, target_price 
                    FROM public.bot_orders 
                    WHERE symbol = %s AND side = 'BUY' 
                      AND (status IN ('SENT', 'QUEUING', 'PARTIAL') OR created_at::date = CURRENT_DATE)
                    ORDER BY order_id DESC LIMIT 1;
                """, (sym,))
                pending_order = cur.fetchone()

                pending_note = ""
                if pending_order:
                    p_ref = pending_order['broker_order_no'] or f"#{pending_order['order_id']}"
                    p_st = pending_order['status']
                    p_vol = pending_order['volume']
                    p_prc = float(pending_order['target_price'])
                    pending_note = (
                        f"\n\n⚠️ <b>[แจ้งเตือน: หุ้นตัวนี้มีคำสั่งซื้อของวันนี้แล้ว]</b>\n"
                        f"• Order No: <code>{p_ref}</code> ({p_st})\n"
                        f"• จำนวน: <code>{p_vol:,}</code> หุ้น @ <code>{p_prc:.2f}</code> THB\n"
                        f"🔒 <i>(ปิดปุ่ม Approve เพื่อป้องกันการส่งคำสั่งซื้อซ้ำ)</i>"
                    )

                ai_ev = ai_lookup.get(sym)
                ai_snippet = ""
                if ai_ev:
                    ai_dec = ai_ev.get("decision", "PASS")
                    ai_score = ai_ev.get("confidence_score", 0)
                    ai_verdict = ai_ev.get("verdict", "")
                    ai_badge = "✅ BUY" if ai_dec == "BUY" else "❌ PASS"
                    ai_snippet = (
                        f"\n🤖 <b>ความเห็น AI:</b> {ai_badge} (<code>{ai_score}/100</code>)\n"
                        f"• <i>{ai_verdict}</i>\n"
                    )

                caption = (
                    f"🚨 <b>ตรวจพบสัญญาณซื้อ: {sym}</b>\n"
                    f"• ราคาปิด: <code>{p:.2f}</code> THB | SL Plan: <code>{float(sig['stop_loss_plan']):.2f}</code> THB\n"
                    f"• แหล่งสัญญาณ: <code>{html.escape(str(sig['signal_source']))}</code>\n"
                    f"• ประมาณการใช้เงิน: <code>{cost:,.2f}</code> THB\n"
                    f"• อำนาจซื้อคงเหลือ: <code>{line_avail:,.2f}</code> THB\n"
                    f"• สัญญาณเทคนิค: {html.escape(str(sig['reason'] or ''))}"
                    f"{ai_snippet}"
                    f"{pending_note}\n\n"
                    + ("<i>กดปุ่ม ➖ / ➕ เพื่อปรับจำนวนหุ้นก่อนกด Approve</i>" if not pending_order else "<i>หุ้นนี้มีคำสั่งซื้อในระบบแล้ว</i>")
                )

                reply_markup = build_trade_keyboard(sig["id"], shares, p, pending_order=pending_order)
                chart_buf = generate_stock_chart(sym)

                try:
                    if chart_buf:
                        await bot.send_photo(chat_id=target_chat, photo=chart_buf, caption=caption, parse_mode="HTML", reply_markup=reply_markup)
                    else:
                        await bot.send_message(chat_id=target_chat, text=caption, parse_mode="HTML", reply_markup=reply_markup)
                    await asyncio.sleep(2)
                except Exception as e:
                    print(f"Error sending {sym}: {e}")

            # ส่งข้อความสรุปปิดท้ายรอบการสแกน
            sym_tag = f" (เฉพาะหุ้น {len(signals)} ตัวที่เลือก)" if target_symbols else ""
            summary_msg = (
                f"🏁 <b>การสแกนเสร็จสิ้น{sym_tag}</b>\n"
                f"• ตรวจพบสัญญาณซื้อทั้งหมด: <code>{len(signals)}</code> รายการ\n"
                f"<i>(สามารถกด Approve หรือปรับจำนวนหุ้นผ่านการ์ดด้านบน หรือพิมพ์ /port เพื่อดูยอดเงิน)</i>"
            )
            await bot.send_message(chat_id=target_chat, text=summary_msg, parse_mode="HTML")
            await notify_job_finish("Run Buy Scanner", elapsed_seconds=time.time() - start_t, summary=f"ตรวจพบสัญญาณซื้อ {len(signals)} รายการ")
    except Exception as e:
        await notify_job_finish("Run Buy Scanner", elapsed_seconds=time.time() - start_t, success=False, error=str(e))
        raise e
    finally:
        conn.close()

if __name__ == "__main__":
    level = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else None
    cfg = load_config()
    should_ai = cfg.get("ai_evaluation", {}).get("enabled", False)
    asyncio.run(scan_and_notify(run_ai=should_ai, detail_level=level))
