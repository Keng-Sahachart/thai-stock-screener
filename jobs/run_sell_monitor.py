#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : run_sell_monitor.py
LOCATION    : jobs/
OCCASION    : รันอัตโนมัติทุกๆ 5 - 15 นาที ระหว่างตลาดเปิดทำการ
              - รอบเช้า: 10:00 - 12:30 น.
              - รอบบ่าย: 14:30 - 16:30 น.
DESCRIPTION : ตรวจสอบหุ้นในพอร์ตและไม้จำลองเทียบกับเงื่อนไขขาย:
              - กรองหุ้นใน excluded_symbols ออกเพื่อไม่ให้สั่งขาย
              - ตรวจสอบสวิตช์ enable_auto_cut_loss และ enable_auto_stop_loss
              - หากเปิดใช้งาน: สั่งขายทันทีผ่าน order_manager
              - หากปิดใช้งาน: ส่งข้อความแจ้งเตือนความเสี่ยงเข้า Telegram (Alert Only)
==============================================================================
"""

import os
import json
import asyncio
import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import Bot
from telegram.request import HTTPXRequest
from dotenv import load_dotenv

import sys
import time
from pathlib import Path
# ถอยกลับไป 1 โฟลเดอร์เพื่อชี้ไปที่ root (thai-stock-screener)
sys.path.append(str(Path(__file__).resolve().parent.parent))
from execution.order_manager import place_sell_order
from risk_manager import is_market_trading_time
from core.job_notifier import notify_job_start, notify_job_finish

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
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

def is_sell_enabled(trigger_type: str, controls: dict) -> bool:
    """ตรวจสอบว่าประเภทสัญญาณนี้เปิดให้สั่งขายอัตโนมัติหรือไม่"""
    if trigger_type == "HARD_CUT_LOSS":
        return controls.get("enable_auto_cut_loss", False)
    elif trigger_type in ("INITIAL_SL_HIT", "TRAILING_STOP_HIT"):
        if trigger_type == "TRAILING_STOP_HIT":
            return controls.get("enable_auto_trailing_stop", controls.get("enable_auto_stop_loss", False))
        return controls.get("enable_auto_stop_loss", False)
    elif trigger_type == "TECHNICAL_SELL_SIGNAL":
        return controls.get("enable_auto_technical_sell", False)
    return False

async def check_and_execute_sells(ignore_market_hours: bool = False):
    start_t = time.time()
    await notify_job_start("Run Sell Monitor", "ตรวจสอบเงื่อนไขขายและ Stop Loss ของพอร์ต")

    config = load_config()
    controls = config.get("auto_sell_controls", {})
    excluded_symbols = set(config.get("excluded_symbols", []))
    is_dry_run = config.get("trading_mode", {}).get("dry_run", True)
    check_market = config.get("risk_management", {}).get("check_market_hours", True)

    # ตรวจสอบเวลาเปิดทำการของตลาด (สำหรับ LIVE Mode)
    if not is_dry_run and check_market and not ignore_market_hours:
        is_open, mkt_reason = is_market_trading_time()
        if not is_open:
            print(f"⏰ [SELL MONITOR] {mkt_reason} ข้ามการตรวจจับและส่งคำสั่งขายจริง")
            await notify_job_finish("Run Sell Monitor", elapsed_seconds=time.time() - start_t, summary=f"ข้ามการทำงาน ({mkt_reason})")
            return {"status": "skipped", "reason": mkt_reason, "triggers_count": 0, "sold_count": 0}

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
                await notify_job_finish("Run Sell Monitor", elapsed_seconds=time.time() - start_t, summary="พอร์ตปลอดภัย ไม่มีหุ้นที่เข้าเงื่อนไขตัดขาย")
                return {"status": "ok", "triggers_count": 0, "sold_count": 0, "triggers": []}

            print(f"⚠️ พบ {len(triggers)} หุ้นที่หลุดเกณฑ์ความปลอดภัย กำลังดำเนินการตัดขาย...")
            
            sold_count = 0
            for item in triggers:
                sym = item["symbol"]
                vol = int(item["current_volume"])
                mkt_p = float(item["market_price"])
                pnl_pct = float(item["percent_profit"])
                trigger_type = item["exit_trigger_type"]
                holding_src = item["holding_source"]

                # 1. ข้ามหุ้นที่ระบุไว้ใน excluded_symbols
                if sym in excluded_symbols:
                    print(f"⏭️ ข้ามหุ้น {sym}: อยู่ใน excluded_symbols (ยกเว้นการขายอัตโนมัติ)")
                    continue

                # 1.1 ตรวจสอบว่ามีคำสั่งขายค้างรออยู่ในตลาดแล้วหรือไม่ ป้องกันการส่งซ้ำ (คำสั่งเป็น Day Order ตรวจเฉพาะของวันนี้)
                cur.execute("""
                    SELECT order_id, broker_order_no, status, volume 
                    FROM public.bot_orders 
                    WHERE symbol = %s AND side = 'SELL' 
                      AND status IN ('SENT', 'QUEUING', 'PARTIAL')
                      AND created_at::date = CURRENT_DATE;
                """, (sym,))
                pending_sell = cur.fetchone()

                if pending_sell:
                    p_ref = pending_sell.get('broker_order_no') or f"#{pending_sell.get('order_id')}"
                    print(f"⏭️ ข้ามการขาย {sym}: มีคำสั่งขายรออยู่ในตลาดแล้ว (Ref: {p_ref}, สถานะ: {pending_sell['status']})")
                    continue

                # 2. ตรวจสอบสวิตช์ auto_sell_controls
                if not is_sell_enabled(trigger_type, controls):
                    alert_msg = (
                        f"⚠️ <b>[ALERT ONLY] หุ้นเข้าเกณฑ์ต้องขาย: {sym}</b>\n"
                        f"• จำนวน: <code>{vol:,}</code> หุ้น [{holding_src}]\n"
                        f"• ราคาตลาด: <code>{mkt_p:.2f}</code> THB\n"
                        f"• กำไร/ขาดทุน: <code>{pnl_pct:+.2f}%</code>\n"
                        f"• เงื่อนไขที่เข้าข่าย: <code>{trigger_type}</code>\n"
                        f"<i>(ระบบไม่ได้ส่งคำสั่งขาย เนื่องจากปิดสวิตช์ Auto-Sell สำหรับเงื่อนไขนี้)</i>"
                    )
                    await bot.send_message(chat_id=CHAT_ID, text=alert_msg, parse_mode="HTML")
                    print(f"🔔 ส่งแจ้งเตือน Alert-Only สำหรับ {sym} ({trigger_type})")
                    continue

                # 3. กรณีเปิดสวิตช์: ดำเนินการส่งคำสั่งขายจริงหรือจำลอง

                # สั่งขายทันทีผ่าน Order Manager
                res = place_sell_order(
                    symbol=sym,
                    volume=vol,
                    exit_price=mkt_p,
                    exit_reason=trigger_type
                )

                if res.get("success"):
                    sold_count += 1
                    mode = "DRY_RUN" if "SIM_" in res.get("broker_order_no", "") else "LIVE"
                    msg = (
                        f"🚨 <b>แจ้งเตือนการสั่งขายอัตโนมัติ [{mode}]</b>\n"
                        f"• หุ้น: <b>{sym}</b> ({vol:,} หุ้น) [{holding_src}]\n"
                        f"• ราคาขาย: <code>{mkt_p:.2f}</code> THB\n"
                        f"• กำไร/ขาดทุน: <code>{pnl_pct:+.2f}%</code>\n"
                        f"• เงื่อนไข: <code>{trigger_type}</code>\n"
                        f"• Order No: <code>{res.get('broker_order_no', 'N/A')}</code>\n\n"
                        f"<i>(ส่งคำสั่งขายและบันทึกสถานะเรียบร้อยแล้ว)</i>"
                    )
                    await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="HTML")
                    print(f"✅ สั่งขาย {sym} สำเร็จเนื่องจาก {trigger_type}")
                else:
                    err_msg = f"❌ เกิดข้อผิดพลาดในการขาย {sym}: {res.get('error')}"
                    await bot.send_message(chat_id=CHAT_ID, text=err_msg)
                    print(err_msg)

            await notify_job_finish("Run Sell Monitor", elapsed_seconds=time.time() - start_t, summary=f"ตรวจสอบเสร็จสิ้น (พบเข้าข่าย {len(triggers)} ตัว, ส่งคำสั่งขาย {sold_count} ตัว)")
            return {"status": "ok", "triggers_count": len(triggers), "sold_count": sold_count, "triggers": triggers}
    except Exception as e:
        await notify_job_finish("Run Sell Monitor", elapsed_seconds=time.time() - start_t, success=False, error=str(e))
        raise e
    finally:
        conn.close()

if __name__ == "__main__":
    asyncio.run(check_and_execute_sells())