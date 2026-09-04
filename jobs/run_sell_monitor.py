#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : run_sell_monitor.py
LOCATION    : jobs/
OCCASION    : รันอัตโนมัติทุกๆ 5 - 15 นาที ระหว่างตลาดเปิดทำการ
              (รอบเช้า 10:00 - 12:30 น. และรอบบ่าย 14:30 - 16:30 น.)
DESCRIPTION : ตรวจสอบหุ้นใน bot_active_positions หากหลุดจุด Stop Loss,
              Trailing Stop, Hard Cut Loss (-10%) หรือมีสัญญาณ Sell จะส่งคำสั่ง
              ขายตัดขาดทุน/ล็อกกำไรทันที พร้อมยิงแจ้งเตือนเข้า Telegram
==============================================================================
"""

import os
import asyncio
import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import Bot
from telegram.request import HTTPXRequest
from dotenv import load_dotenv

import sys
from pathlib import Path
# ถอยกลับไป 1 โฟลเดอร์เพื่อชี้ไปที่ root (thai-stock-screener)
sys.path.append(str(Path(__file__).resolve().parent.parent))
from execution.order_manager import place_sell_order

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

async def check_and_execute_sells():
    conn = get_db_connection()
    bot = Bot(token=TOKEN, request=HTTPXRequest(connect_timeout=20.0, read_timeout=60.0))

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # ดึงรายการหุ้นที่เข้าเงื่อนไขต้องขายด่วน
            cur.execute("""
                SELECT * FROM public.v_bot_sell_triggers 
                ORDER BY exit_urgency_priority DESC;
            """)
            triggers = cur.fetchall()

            if not triggers:
                print("🛡️ พอร์ตปลอดภัย: ไม่มีหุ้นที่เข้าเงื่อนไขตัดขายในขณะนี้")
                return

            print(f"⚠️ พบ {len(triggers)} หุ้นที่หลุดเกณฑ์ความปลอดภัย กำลังดำเนินการตัดขาย...")
            
            for item in triggers:
                sym = item["symbol"]
                vol = int(item["current_volume"])
                mkt_p = float(item["market_price"])
                pnl_pct = float(item["percent_profit"])
                trigger_type = item["exit_trigger_type"]

                # 1. สั่งขายทันทีผ่าน Order Manager
                res = place_sell_order(
                    symbol=sym,
                    volume=vol,
                    exit_price=mkt_p,
                    exit_reason=trigger_type
                )

                if res.get("success"):
                    mode = "DRY_RUN" if "SIM_" in res.get("broker_order_no", "") else "LIVE"
                    msg = (
                        f"🚨 <b>แจ้งเตือนการสั่งขายอัตโนมัติ [{mode}]</b>\n"
                        f"• หุ้น: <b>{sym}</b> ({vol:,} หุ้น)\n"
                        f"• ราคาขาย: <code>{mkt_p:.2f}</code> THB\n"
                        f"• ผลตอบแทน: <code>{pnl_pct:+.2f}%</code>\n"
                        f"• สาเหตุ: <code>{trigger_type}</code>\n"
                        f"• Order No: <code>{res.get('broker_order_no', 'N/A')}</code>\n\n"
                        f"<i>(ระบบปิดสถานะใน bot_active_positions เรียบร้อยแล้ว)</i>"
                    )
                    await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="HTML")
                    print(f"✅ ขาย {sym} สำเร็จเนื่องจาก {trigger_type}")
                else:
                    err_msg = f"❌ เกิดข้อผิดพลาดในการขาย {sym}: {res.get('error')}"
                    await bot.send_message(chat_id=CHAT_ID, text=err_msg)
                    print(err_msg)

    finally:
        conn.close()

if __name__ == "__main__":
    asyncio.run(check_and_execute_sells())