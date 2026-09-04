#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : run_eod_sync.py
LOCATION    : jobs/
OCCASION    : รันช่วงเย็นหลังตลาดปิด (17:30 - 19:00 น.) หลังรัน taskUpdate.py
DESCRIPTION : ซิงค์ข้อมูลพอร์ตสิ้นวัน:
              - อัปเดต max_price_reached และขยับ trailing_stop_loss
              - สร้างรายงานสรุปสถานะพอร์ต PnL ส่งเข้า Telegram
==============================================================================
"""

import os
import asyncio
import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import Bot
from telegram.request import HTTPXRequest
from dotenv import load_dotenv

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

async def sync_eod_portfolio():
    conn = get_db_connection()
    bot = Bot(token=TOKEN, request=HTTPXRequest(connect_timeout=20.0, read_timeout=60.0))

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 1. ดึง Position ที่เปิดอยู่ทั้งหมดมาเทียบกับ High และ Close ของวันนี้
            cur.execute("""
                SELECT 
                    b.id, b.symbol, b.entry_price, b.initial_stop_loss,
                    b.max_price_reached, b.trailing_stop_loss, b.current_volume,
                    p.high AS today_high, p.close AS today_close,
                    i.atr14
                FROM public.bot_active_positions b
                JOIN (
                    SELECT symbol, high, close, date
                    FROM public.stock_price_history
                    WHERE date = (SELECT MAX(date) FROM public.stock_price_history)
                ) p ON b.symbol = p.symbol
                LEFT JOIN public.mv_stock_indicators i 
                    ON b.symbol = i.symbol AND p.date = i.trade_date
                WHERE b.status = 'OPEN';
            """)
            positions = cur.fetchall()

            for pos in positions:
                pos_id = pos["id"]
                high = float(pos["today_high"] or 0)
                curr_max = float(pos["max_price_reached"])
                init_sl = float(pos["initial_stop_loss"])
                prev_trailing = float(pos["trailing_stop_loss"]) if pos["trailing_stop_loss"] else None
                atr = float(pos["atr14"]) if pos["atr14"] else None

                # ขยับ Max Price Reached หากทำ New High
                new_max = max(curr_max, high)
                new_trailing = prev_trailing

                # ถ้ามีกำไรและมี ATR ให้คำนวณ Trailing Stop ใหม่
                if atr and new_max > float(pos["entry_price"]):
                    candidate_trailing = round(new_max - (atr * 2.0), 4)
                    if candidate_trailing > init_sl:
                        new_trailing = max(prev_trailing or 0.0, candidate_trailing)

                cur.execute("""
                    UPDATE public.bot_active_positions
                    SET max_price_reached = %s,
                        trailing_stop_loss = %s,
                        updated_at = timezone('Asia/Bangkok', now())
                    WHERE id = %s;
                """, (new_max, new_trailing, pos_id))

            conn.commit()

            # 2. สร้างข้อความสรุป EOD ประจำวัน
            cur.execute("""
                SELECT line_available, cash_balance 
                FROM public.account_info_history 
                WHERE is_disabled = FALSE ORDER BY import_date DESC LIMIT 1;
            """)
            acc = cur.fetchone()
            line_avail = float(acc["line_available"]) if acc else 0.0

            cur.execute("""
                SELECT 
                    b.symbol, b.entry_price, b.current_volume,
                    COALESCE(p.close, b.entry_price) AS last_price,
                    b.initial_stop_loss, b.trailing_stop_loss,
                    ROUND(((COALESCE(p.close, b.entry_price) - b.entry_price) / b.entry_price * 100)::numeric, 2) AS pnl_pct
                FROM public.bot_active_positions b
                LEFT JOIN (
                    SELECT symbol, close FROM public.stock_price_history
                    WHERE date = (SELECT MAX(date) FROM public.stock_price_history)
                ) p ON b.symbol = p.symbol
                WHERE b.status = 'OPEN'
                ORDER BY pnl_pct DESC;
            """)
            open_items = cur.fetchall()

            msg = (
                f"🌙 <b>สรุปรายงานสถานะพอร์ตสิ้นวัน (EOD Summary)</b>\n"
                f"• อำนาจซื้อคงเหลือ: <code>{line_avail:,.2f}</code> THB\n"
                f"• จำนวนหุ้นที่บอทถือครอง: <b>{len(open_items)}</b> ตัว\n"
                f"------------------------------------\n"
            )

            if not open_items:
                msg += "<i>ไม่มีสถานะหุ้นที่เปิดอยู่</i>"
            else:
                for item in open_items:
                    trail_str = f"{float(item['trailing_stop_loss']):.2f}" if item["trailing_stop_loss"] else "ยังไม่เปิดใช้งาน"
                    msg += (
                        f"📌 <b>{item['symbol']}</b> ({item['current_volume']:,} หุ้น)\n"
                        f"• ทุน: <code>{float(item['entry_price']):.2f}</code> | ปิด: <code>{float(item['last_price']):.2f}</code>\n"
                        f"• กำไร/ขาดทุน: <code>{float(item['pnl_pct']):+.2f}%</code>\n"
                        f"• Initial SL: <code>{float(item['initial_stop_loss']):.2f}</code> THB\n"
                        f"• Trailing SL: <code>{trail_str}</code>\n\n"
                    )

            await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="HTML")
            print("✅ ซิงค์ EOD และส่งรายงานพอร์ตสำเร็จ")

    finally:
        conn.close()

if __name__ == "__main__":
    asyncio.run(sync_eod_portfolio())