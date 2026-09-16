#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : run_port_scanner.py
LOCATION    : jobs/
OCCASION    : รันรายงานสัญญาณเทคนิคอลและกราฟของหุ้นที่ถือครองในพอร์ต (ผ่าน Telegram หรือ Crontab)
DESCRIPTION : ดึงหุ้นที่ถืออยู่ในพอร์ตจริง (v_portfolio_with_signals) ร่วมกับ
              Indicators ล่าสุด (mv_stock_indicators) และเงื่อนไขขาย (v_bot_sell_triggers)
              วาดกราฟเทคนิคอล (Candlestick, EMA, MACD, RSI) ส่งเข้าห้องแชท Telegram
==============================================================================
"""

import os
import sys
import time
import asyncio
import html
import psycopg2
from pathlib import Path
from psycopg2.extras import RealDictCursor
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.request import HTTPXRequest
from dotenv import load_dotenv

# เพิ่ม root directory ลงใน sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from bot.chart_generator import generate_stock_chart
from core.job_notifier import notify_job_start, notify_job_finish

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

async def scan_portfolio_and_notify(symbol: str = None, bot: Bot = None, chat_id: str = None):
    """
    สแกนหุ้นในพอร์ตและส่งการ์ดสัญญาณกราฟเทคนิคอลเข้า Telegram
    :param symbol: ชื่อหุ้นที่ต้องการตรวจเฉพาะตัว (None = ตรวจทุกตัวในพอร์ต)
    :param bot: instance ของ Telegram Bot (ถ้า None จะสร้างใหม่จาก TOKEN)
    :param chat_id: ID ห้องแชทที่ต้องการส่ง (ถ้า None จะใช้ CHAT_ID จาก .env)
    """
    start_t = time.time()
    sym_upper = symbol.upper().strip() if symbol else None
    job_name = f"Scan Portfolio ({sym_upper})" if sym_upper else "Scan Portfolio"
    
    await notify_job_start(job_name, f"รายงานสัญญาณกราฟเทคนิคอลของหุ้นในพอร์ต {'[' + sym_upper + ']' if sym_upper else '(ทุกตัว)'}")

    if bot is None:
        bot = Bot(token=TOKEN, request=HTTPXRequest(connect_timeout=20.0, read_timeout=60.0, write_timeout=60.0))
    target_chat = chat_id or CHAT_ID

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT 
                    p.symbol, p.current_volume, p.average_price, p.market_price, p.profit, p.percent_profit,
                    p.signal_type, p.signal_reason, p.trend_status, p.is_managed_by_bot,
                    p.initial_stop_loss, p.trailing_stop_loss,
                    m.rsi14, m.macd_12_26_9, m.macd_12_26_9_signal, m.macd_12_26_9_hist,
                    m.ema12, m.ema26,
                    st.exit_trigger_type
                FROM public.v_portfolio_with_signals p
                LEFT JOIN (
                    SELECT DISTINCT ON (symbol) symbol, rsi14, macd_12_26_9, macd_12_26_9_signal, macd_12_26_9_hist, ema12, ema26
                    FROM public.mv_stock_indicators
                    ORDER BY symbol, trade_date DESC
                ) m ON p.symbol = m.symbol
                LEFT JOIN public.v_bot_sell_triggers st ON p.symbol = st.symbol
                WHERE p.current_volume > 0
                  AND (%s IS NULL OR p.symbol = %s)
                ORDER BY p.percent_profit DESC;
            """
            cur.execute(query, (sym_upper, sym_upper))
            rows = cur.fetchall()

            if not rows:
                not_found_msg = (
                    f"⚠️ ไม่พบหุ้น <b>{sym_upper}</b> ที่ถือครองอยู่ในพอร์ตปัจจุบัน"
                    if sym_upper else
                    "ℹ️ ไม่พบหุ้นที่ถือครองอยู่ในพอร์ตปัจจุบัน"
                )
                await bot.send_message(chat_id=target_chat, text=not_found_msg, parse_mode="HTML")
                await notify_job_finish(job_name, elapsed_seconds=time.time() - start_t, summary="ไม่พบหุ้นที่ถือครองในพอร์ต")
                return

            buy_count = 0
            sell_count = 0
            hold_count = 0

            for idx, r in enumerate(rows, 1):
                sym = r["symbol"]
                vol = int(r["current_volume"])
                avg_p = float(r["average_price"])
                mkt_p = float(r["market_price"])
                pnl_pct = float(r["percent_profit"] or 0.0)
                profit = float(r["profit"] or 0.0)
                tag = "🤖 [BOT]" if r["is_managed_by_bot"] else "👤 [MANUAL]"

                # สัญญาณ
                sig_type = (r["signal_type"] or "SIDEWAY").upper()
                if sig_type == "BUY":
                    sig_badge = "🟢 BUY (สัญญาณซื้อ)"
                    buy_count += 1
                elif sig_type == "SELL":
                    sig_badge = "🔴 SELL (สัญญาณขาย)"
                    sell_count += 1
                elif sig_type == "HOLD":
                    sig_badge = "🔵 HOLD (ถือครอง)"
                    hold_count += 1
                else:
                    sig_badge = f"🟡 {sig_type}"
                    hold_count += 1

                # แนวโน้ม
                trend_raw = (r["trend_status"] or "").lower()
                if "up" in trend_raw or "bull" in trend_raw:
                    trend_str = "🟢 ขาขึ้น (Uptrend)"
                elif "down" in trend_raw or "bear" in trend_raw:
                    trend_str = "🔴 ขาลง (Downtrend)"
                else:
                    trend_str = "🟡 ไซด์เวย์ (Sideway)"

                # RSI
                rsi_val = float(r["rsi14"]) if r["rsi14"] is not None else None
                if rsi_val is not None:
                    if rsi_val >= 70:
                        rsi_str = f"<code>{rsi_val:.1f}</code> (Overbought ⚠️)"
                    elif rsi_val <= 30:
                        rsi_str = f"<code>{rsi_val:.1f}</code> (Oversold 💎)"
                    else:
                        rsi_str = f"<code>{rsi_val:.1f}</code> (Neutral)"
                else:
                    rsi_str = "N/A"

                # MACD
                macd_v = float(r["macd_12_26_9"]) if r["macd_12_26_9"] is not None else None
                sig_v = float(r["macd_12_26_9_signal"]) if r["macd_12_26_9_signal"] is not None else None
                hist_v = float(r["macd_12_26_9_hist"]) if r["macd_12_26_9_hist"] is not None else None
                macd_line = (
                    f"<code>{macd_v:.3f}</code> (Signal: <code>{sig_v:.3f}</code>, Hist: <code>{hist_v:+.3f}</code>)"
                    if macd_v is not None and sig_v is not None and hist_v is not None else "N/A"
                )

                # Stop loss & Trailing Stop
                sl_str = f"{float(r['initial_stop_loss']):.2f}" if r["initial_stop_loss"] is not None else "N/A"
                ts_str = f"{float(r['trailing_stop_loss']):.2f}" if r["trailing_stop_loss"] is not None else "-"

                # สัญญาณเตือนขายจากระบบ Sell Monitor
                exit_trigger = r.get("exit_trigger_type")
                sell_warning = ""
                if exit_trigger:
                    sell_count += 1
                    sell_warning = f"\n\n🚨 <b>[เตือนขาย: {exit_trigger}]</b> หุ้นนี้เข้าเกณฑ์ขายตามระบบแล้ว!"

                sym_url = f"https://www.settrade.com/th/equities/quote/{sym}/overview"
                caption = (
                    f"📊 <b>สัญญาณกราฟหุ้นในพอร์ต ({idx}/{len(rows)}): {sym}</b>\n"
                    f"• ประเภท: {tag} | ถือครอง: <code>{vol:,}</code> หุ้น\n"
                    f"• ทุน: <code>{avg_p:.2f}</code> | ตลาด: <code>{mkt_p:.2f}</code> THB\n"
                    f"• กำไร/ขาดทุน: <code>{pnl_pct:+.2f}%</code> (<code>{profit:+,.2f}</code> THB)\n"
                    f"------------------------------------\n"
                    f"• สัญญาณเทคนิคอล: <b>{sig_badge}</b>\n"
                    f"• แนวโน้มราคา: <b>{trend_str}</b>\n"
                    f"• RSI (14): {rsi_str}\n"
                    f"• MACD (12,26,9): {macd_line}\n"
                    f"• รายละเอียด: <i>{html.escape(str(r['signal_reason'] or '-'))}</i>\n"
                    f"• Initial SL: <code>{sl_str}</code> | Trailing SL: <code>{ts_str}</code>"
                    f"{sell_warning}"
                )

                from bot.telegram_app import build_port_card_keyboard
                reply_markup = build_port_card_keyboard(sym, vol, mkt_p)

                # สร้างภาพกราฟ
                chart_buf = generate_stock_chart(sym)
                try:
                    if chart_buf:
                        await bot.send_photo(
                            chat_id=target_chat,
                            photo=chart_buf,
                            caption=caption,
                            parse_mode="HTML",
                            reply_markup=reply_markup
                        )
                    else:
                        await bot.send_message(
                            chat_id=target_chat,
                            text=caption,
                            parse_mode="HTML",
                            reply_markup=reply_markup
                        )
                except Exception as ex:
                    print(f"[WARN] Failed to send {sym}: {ex}")
                    # ส่งข้อความสำรองแบบ text หากส่งรูปไม่ผ่าน
                    try:
                        await bot.send_message(chat_id=target_chat, text=caption, parse_mode="HTML", reply_markup=reply_markup)
                    except Exception as ex2:
                        print(f"[WARN] Failed to send fallback text for {sym}: {ex2}")

                # หน่วงเวลาเล็กน้อยเพื่อป้องกัน Telegram Flood Limit (หากมีหลายตัว)
                if len(rows) > 1:
                    await asyncio.sleep(1.5)

            # ข้อความสรุปภาพรวมตอนท้าย
            summary_msg = (
                f"🏁 <b>การรายงานสัญญาณกราฟหุ้นในพอร์ตเสร็จสิ้น</b>\n"
                f"• รายงานทั้งหมด: <code>{len(rows)}</code> ตัว\n"
                f"• สัญญาณซื้อ (BUY): <code>{buy_count}</code> ตัว\n"
                f"• สัญญาณขาย/เตือนขาย (SELL/Trigger): <code>{sell_count}</code> ตัว\n"
                f"• ถือครอง/ไซด์เวย์ (HOLD/SIDEWAY): <code>{hold_count}</code> ตัว\n\n"
                f"👉 <i>พิมพ์ /port เพื่อดูภาพรวมพอร์ต หรือ /scan เพื่อสแกนหาจังหวะซื้อใหม่</i>"
            )
            await bot.send_message(chat_id=target_chat, text=summary_msg, parse_mode="HTML")
            await notify_job_finish(job_name, elapsed_seconds=time.time() - start_t, summary=f"รายงานสัญญาณกราฟ {len(rows)} ตัวเสร็จสิ้น")
    except Exception as e:
        await notify_job_finish(job_name, elapsed_seconds=time.time() - start_t, success=False, error=str(e))
        raise e
    finally:
        conn.close()

if __name__ == "__main__":
    target_sym = sys.argv[1] if len(sys.argv) > 1 else None
    asyncio.run(scan_portfolio_and_notify(symbol=target_sym))
