#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : job_notifier.py
LOCATION    : core/
DESCRIPTION : โมดูลศูนย์กลางสำหรับส่งการแจ้งเตือนเริ่มและสิ้นสุดการทำงานของ Background Jobs
              - อ่าน/เขียนการตั้งค่าเปิด-ปิดจาก config/bot_config.json (job_notifications.enabled)
              - รองรับทั้งการเรียกแบบ Async (await) และ Sync
              - จัดการข้อผิดพลาดในตัว ไม่ทำให้ Job หลักหยุดทำงานหากเน็ตเวิร์กมีปัญหา
==============================================================================
"""

import os
import json
import time
import asyncio
import html
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from telegram import Bot
from telegram.request import HTTPXRequest

# โหลด .env
ROOT_DIR = Path(__file__).resolve().parent.parent
env_path = ROOT_DIR / ".env"
load_dotenv(dotenv_path=env_path)

CONFIG_PATH = ROOT_DIR / "config" / "bot_config.json"
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def is_job_notification_enabled() -> bool:
    """ตรวจสอบว่าเปิดการแจ้งเตือน Job อยู่หรือไม่"""
    try:
        if not CONFIG_PATH.exists():
            return True
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        return cfg.get("job_notifications", {}).get("enabled", True)
    except Exception as e:
        print(f"[JOB NOTIFIER] Error reading config: {e}")
        return True

def set_job_notification_enabled(enabled: bool) -> bool:
    """เปิดหรือปิดการแจ้งเตือน Job และบันทึกลง bot_config.json"""
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)

        if "job_notifications" not in cfg:
            cfg["job_notifications"] = {
                "enabled": enabled,
                "description": "true = ส่งข้อความแจ้งเตือนเข้า Telegram เมื่อ Background Jobs (Cron) เริ่มและสิ้นสุดการทำงาน, false = ปิดการแจ้งเตือน"
            }
        else:
            cfg["job_notifications"]["enabled"] = enabled

        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=4, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"[JOB NOTIFIER] Error writing config: {e}")
        return False

def _get_bot():
    if not TOKEN:
        return None
    return Bot(token=TOKEN, request=HTTPXRequest(connect_timeout=20.0, read_timeout=60.0))

async def notify_job_start(job_name: str, details: str = None):
    """ส่งข้อความแจ้งเตือนเมื่อ Job เริ่มต้นทำงาน (Async)"""
    if not is_job_notification_enabled() or not TOKEN or not CHAT_ID:
        return

    now_str = datetime.now().strftime("%H:%M:%S")
    detail_line = f"• รายละเอียด: <i>{html.escape(str(details))}</i>\n" if details else ""

    msg = (
        f"⏳ [JOB START] <b>{html.escape(str(job_name))}</b>\n"
        f"• เวลา: <code>{now_str}</code>\n"
        f"{detail_line}"
        f"• สถานะ: กำลังเริ่มกระบวนการทำงาน..."
    )

    try:
        bot = _get_bot()
        if bot:
            await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="HTML")
    except Exception as e:
        print(f"[JOB NOTIFIER ERROR] notify_job_start failed for {job_name}: {e}")

async def notify_job_finish(job_name: str, elapsed_seconds: float = None, summary: str = None, success: bool = True, error: str = None):
    """ส่งข้อความแจ้งเตือนเมื่อ Job ทำงานสิ้นสุด (Async)"""
    if not is_job_notification_enabled() or not TOKEN or not CHAT_ID:
        return

    now_str = datetime.now().strftime("%H:%M:%S")
    elapsed_str = f" (ใช้เวลา {elapsed_seconds:.1f} วินาที)" if elapsed_seconds is not None else ""
    icon = "🏁" if success else "❌"
    tag = "JOB FINISHED" if success else "JOB FAILED"
    status_text = "✅ ทำงานเสร็จสมบูรณ์" if success else f"❌ ล้มเหลว: {html.escape(str(error))}"
    summary_line = f"• สรุปผล: {html.escape(str(summary))}\n" if summary else ""

    msg = (
        f"{icon} [{tag}] <b>{html.escape(str(job_name))}</b>\n"
        f"• เวลา: <code>{now_str}</code>{elapsed_str}\n"
        f"• สถานะ: {status_text}\n"
        f"{summary_line}"
    ).strip()

    try:
        bot = _get_bot()
        if bot:
            await bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="HTML")
    except Exception as e:
        print(f"[JOB NOTIFIER ERROR] notify_job_finish failed for {job_name}: {e}")

def notify_job_start_sync(job_name: str, details: str = None):
    """ส่งข้อความแจ้งเตือนเมื่อ Job เริ่มต้นทำงาน (Sync Wrapper)"""
    try:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            asyncio.ensure_future(notify_job_start(job_name, details))
        else:
            asyncio.run(notify_job_start(job_name, details))
    except Exception as e:
        print(f"[JOB NOTIFIER ERROR] notify_job_start_sync failed: {e}")

def notify_job_finish_sync(job_name: str, elapsed_seconds: float = None, summary: str = None, success: bool = True, error: str = None):
    """ส่งข้อความแจ้งเตือนเมื่อ Job ทำงานสิ้นสุด (Sync Wrapper)"""
    try:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            asyncio.ensure_future(notify_job_finish(job_name, elapsed_seconds, summary, success, error))
        else:
            asyncio.run(notify_job_finish(job_name, elapsed_seconds, summary, success, error))
    except Exception as e:
        print(f"[JOB NOTIFIER ERROR] notify_job_finish_sync failed: {e}")
