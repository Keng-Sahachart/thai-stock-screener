#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
========================================================================================
FILE NAME   : master_scheduler.py
LOCATION    : Root directory (thai-stock-screener/)
DESCRIPTION : Master Automation Scheduler & Supervisor
              ระบบควบคุมและรันงานอัตโนมัติแบบครบวงจรในไฟล์เดียว (All-in-One Runner)
              รองรับทั้งระบบปฏิบัติการ Windows และ Linux / Raspberry Pi
========================================================================================

========================================================================================
📌 ตารางสรุป Automation Scheduling (ตารางงานอัตโนมัติประจำวัน)
========================================================================================
| ลำดับ | ช่วงเวลา (Time)    | สคริปต์ที่ต้องรัน (Target Script) | ความถี่ / รูปแบบ     | หน้าที่การทำงาน                                                                                  |
| :---: | :----------------- | :--------------------------------- | :-------------------- | :----------------------------------------------------------------------------------------------- |
|   0   | ตลอด 24 ชม.         | bot/telegram_app.py                | Daemon / Supervisor   | บริการ Background รอดักฟังคำสั่งแชท (/port, /order, ฯลฯ) และปุ่มกด Approve/Reject สัญญาณซื้อ    |
|   1   | 09:00 น.           | jobs/run_port_scanner.py           | One-shot (วันทำการ)   | [Optional] สแกนหุ้นในพอร์ต วาดกราฟเทคนิคอลและสรุปกำไร/ขาดทุนส่งเข้า Telegram ก่อนตลาดเปิด        |
|   2   | 09:15 น.           | jobs/run_buy_scanner.py            | One-shot (วันทำการ)   | สแกนหาหุ้นที่เข้าเกณฑ์ซื้อ วาดกราฟ Candlestick + MACD + RSI แล้วยิงการ์ดขอ Approve ทาง Telegram   |
|   3   | 10:00 - 12:30 น.   | jobs/run_sell_monitor.py           | ทุก 10 นาที (ตลาดเช้า) | ตรวจสอบ Stop Loss, Trailing Stop และ Hard Cut Loss (-10%) หากหลุดเกณฑ์จะตัดขายทันที            |
|   4   | 10:00 - 12:30 น.   | jobs/sync_order_status.py          | ทุก 5 นาที (ตลาดเช้า)  | ตรวจสอบสถานะการจับคู่คำสั่ง (FILLED, QUEUING, CANCELLED) และปรับปรุงต้นทุนจริงลงพอร์ต            |
|   5   | 14:30 - 16:30 น.   | jobs/run_sell_monitor.py           | ทุก 10 นาที (ตลาดบ่าย) | เฝ้าระวังพอร์ตช่วงตลาดบ่ายต่อเนื่อง ตัดขายอัตโนมัติเมื่อราคาหลุดแนวรับหรือเข้าเงื่อนไขขาย         |
|   6   | 14:30 - 16:30 น.   | jobs/sync_order_status.py          | ทุก 5 นาที (ตลาดบ่าย)  | ตรวจสอบสถานะคำสั่งซื้อ/ขายในกระดานอย่างต่อเนื่อง                                                  |
|   7   | 16:40 น.           | jobs/sync_order_status.py          | One-shot (หลังปิดตลาด) | ซิงค์คำสั่งรอบสุดท้ายของวัน เคลียร์ Order ที่หมดอายุ (Expired) หรือถูกยกเลิก                     |
|   8   | 19:15 น.           | taskUpdate.py                      | One-shot (วันทำการ)   | ดึงราคา EOD, อัปเดตงบ/พอร์ต, คำนวณ Indicators (EMA, MACD, RSI, ATR), ซิงค์ Trailing Stop สิ้นวัน |
========================================================================================

วิธีใช้งาน (Usage):
    1. รันครบทุกระบบ (บอท Telegram + ตัวตั้งเวลางานทั้งหมด):
       python master_scheduler.py

    2. รันเฉพาะตัวตั้งเวลา (ไม่เปิดบอท Telegram ซ้อน ในกรณีที่รันบอทเป็น systemd อยู่แล้ว):
       python master_scheduler.py --no-bot

    3. รันเฉพาะตัวควบคุมบอท Telegram (Supervisor คอย Auto-restart ให้ถ้าบอทหลุด):
       python master_scheduler.py --only-bot

    4. สั่งรันงานตัวใดตัวหนึ่งทันที เพื่อทดสอบ (Manual Run):
       python master_scheduler.py --run buy_scanner
       python master_scheduler.py --run sell_monitor
       python master_scheduler.py --run sync_orders
       python master_scheduler.py --run port_scanner
       python master_scheduler.py --run task_update

    5. ตรวจสอบสถานะและคิวงานถัดไป:
       python master_scheduler.py --status
========================================================================================
"""

import os
import sys
import time
import signal
import logging
import argparse
import subprocess
import threading
from datetime import datetime, date, time as dtime
from pathlib import Path

# ปรับ Standard Output Encoding สำหรับ Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# กำหนด Root Directory และโฟลเดอร์ Logs
ROOT_DIR = Path(__file__).resolve().parent
LOGS_DIR = ROOT_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# ตั้งค่า Logging หลักของ Master Scheduler
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOGS_DIR / "master_scheduler.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("MasterScheduler")

# รายการและตำแหน่งสคริปต์ของงานแต่ละตัว
JOB_SCRIPTS = {
    "telegram_bot": ROOT_DIR / "bot" / "telegram_app.py",
    "port_scanner": ROOT_DIR / "jobs" / "run_port_scanner.py",
    "buy_scanner": ROOT_DIR / "jobs" / "run_buy_scanner.py",
    "sell_monitor": ROOT_DIR / "jobs" / "run_sell_monitor.py",
    "sync_orders": ROOT_DIR / "jobs" / "sync_order_status.py",
    "task_update": ROOT_DIR / "taskUpdate.py",
}

# กำหนดช่วงเวลาตลาดหุ้นเปิดทำการ (SET Market Trading Hours)
MORNING_SESSION_START = dtime(10, 0, 0)
MORNING_SESSION_END   = dtime(12, 30, 0)
AFTERNOON_SESSION_START = dtime(14, 30, 0)
AFTERNOON_SESSION_END   = dtime(16, 30, 0)

# Flag สำหรับควบคุมการปิดระบบอย่างปลอดภัย (Graceful Shutdown)
shutdown_event = threading.Event()
running_subprocesses = {}
job_locks = {name: threading.Lock() for name in JOB_SCRIPTS}


def is_weekend() -> bool:
    """ตรวจสอบว่าเป็นวันเสาร์หรืออาทิตย์หรือไม่ (0=จันทร์, ..., 4=ศุกร์, 5=เสาร์, 6=อาทิตย์)"""
    return date.today().weekday() >= 5


def is_market_hours(current_time: dtime) -> bool:
    """ตรวจสอบว่าอยู่ในช่วงเวลาซื้อขายของตลาดหุ้นไทยหรือไม่ (10:00-12:30 หรือ 14:30-16:30)"""
    return (MORNING_SESSION_START <= current_time <= MORNING_SESSION_END) or \
           (AFTERNOON_SESSION_START <= current_time <= AFTERNOON_SESSION_END)


def run_job_subprocess(job_name: str, script_path: Path):
    """รันสคริปต์งานแบบ Subprocess พร้อมเก็บบันทึกประวัติลงไฟล์ log แยกเฉพาะแต่ละงาน"""
    if not job_locks[job_name].acquire(blocking=False):
        logger.warning(f"⏳ งาน [{job_name}] กำลังรันค้างอยู่รอบก่อนหน้า ข้ามรอบนี้เพื่อป้องกันรันชนกัน")
        return

    log_file_path = LOGS_DIR / f"{job_name}.log"
    logger.info(f"🚀 เริ่มรันงาน: [{job_name}] -> {script_path.relative_to(ROOT_DIR)}")

    try:
        with open(log_file_path, "a", encoding="utf-8") as log_f:
            header = f"\n{'='*70}\n[START] {job_name} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'='*70}\n"
            log_f.write(header)
            log_f.flush()

            proc = subprocess.Popen(
                [sys.executable, str(script_path)],
                cwd=str(ROOT_DIR),
                stdout=log_f,
                stderr=subprocess.STDOUT,
                env=os.environ.copy()
            )
            running_subprocesses[job_name] = proc
            return_code = proc.wait()

            footer = f"\n[FINISH] {job_name} exited with code {return_code} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            log_f.write(footer)
            log_f.flush()

        if return_code == 0:
            logger.info(f"✅ งาน [{job_name}] เสร็จสมบูรณ์ (Exit Code 0)")
        else:
            logger.error(f"❌ งาน [{job_name}] จบด้วยข้อผิดพลาด (Exit Code {return_code}) ดูรายละเอียดที่: {log_file_path}")

    except Exception as e:
        logger.exception(f"💥 เกิดข้อผิดพลาดในการรันงาน [{job_name}]: {e}")
    finally:
        running_subprocesses.pop(job_name, None)
        job_locks[job_name].release()


class TelegramBotSupervisor(threading.Thread):
    """Supervisor เฝ้าระวัง Telegram Bot ให้ทำงานตลอดเวลา (Auto-restart on crash)"""
    def __init__(self):
        super().__init__(daemon=True, name="TelegramBotSupervisor")
        self.process = None

    def run(self):
        script_path = JOB_SCRIPTS["telegram_bot"]
        log_file_path = LOGS_DIR / "telegram_bot.log"

        logger.info("🤖 Telegram Bot Supervisor เริ่มต้นการทำงาน...")

        while not shutdown_event.is_set():
            logger.info("🔌 กำลังสตาร์ท Telegram Bot Service...")
            try:
                with open(log_file_path, "a", encoding="utf-8") as log_f:
                    log_f.write(f"\n[START BOT] at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    log_f.flush()

                    self.process = subprocess.Popen(
                        [sys.executable, str(script_path)],
                        cwd=str(ROOT_DIR),
                        stdout=log_f,
                        stderr=subprocess.STDOUT,
                        env=os.environ.copy()
                    )
                    running_subprocesses["telegram_bot"] = self.process

                # รอจนกว่า Process ของ Bot จะจบ
                exit_code = self.process.wait()

                if shutdown_event.is_set():
                    break

                logger.warning(f"⚠️ Telegram Bot หยุดทำงานกะทันหัน (Exit Code: {exit_code}) จะทำการรีสตาร์ทใน 5 วินาที...")
                time.sleep(5)

            except Exception as e:
                logger.error(f"💥 Telegram Bot Supervisor เกิดข้อผิดพลาด: {e}")
                time.sleep(5)

        logger.info("🛑 Telegram Bot Supervisor สิ้นสุดการทำงานแล้ว")

    def stop(self):
        if self.process and self.process.poll() is None:
            logger.info("🛑 กำลังหยุดการทำงานของ Telegram Bot...")
            try:
                self.process.terminate()
                self.process.wait(timeout=5)
            except Exception:
                self.process.kill()


class SchedulerEngine(threading.Thread):
    """Scheduler Engine ตรวจสอบรอบเวลาและสั่งรันงานตามกำหนดการอัตโนมัติ"""
    def __init__(self):
        super().__init__(daemon=True, name="SchedulerEngine")
        # บันทึก timestamp ที่แต่ละงานรันล่าสุด เพื่อไม่ให้รันซ้ำในนาทีเดียวกัน
        self.last_run_minute = {
            "port_scanner": "",
            "buy_scanner": "",
            "sell_monitor": "",
            "sync_orders": "",
            "task_update": "",
            "sync_orders_close": ""
        }

    def run(self):
        logger.info("⏰ Scheduler Engine เริ่มต้นระบบจับเวลา...")

        while not shutdown_event.is_set():
            now = datetime.now()
            current_minute_str = now.strftime("%Y-%m-%d %H:%M")
            cur_time = now.time()
            cur_minute = now.minute

            # ข้ามวันหยุดเสาร์-อาทิตย์ สำหรับงานตลาดหุ้น
            if not is_weekend():
                # -------------------------------------------------------------
                # 1. 09:00 น. -> Run Port Scanner (สรุปกราฟหุ้นในพอร์ตตอนเช้า)
                # -------------------------------------------------------------
                if now.hour == 9 and now.minute == 0:
                    if self.last_run_minute["port_scanner"] != current_minute_str:
                        self.last_run_minute["port_scanner"] = current_minute_str
                        threading.Thread(
                            target=run_job_subprocess,
                            args=("port_scanner", JOB_SCRIPTS["port_scanner"]),
                            daemon=True
                        ).start()

                # -------------------------------------------------------------
                # 2. 09:15 น. -> Run Buy Scanner (สแกนหาหุ้น & ส่งการ์ด Approve)
                # -------------------------------------------------------------
                if now.hour == 9 and now.minute == 15:
                    if self.last_run_minute["buy_scanner"] != current_minute_str:
                        self.last_run_minute["buy_scanner"] = current_minute_str
                        threading.Thread(
                            target=run_job_subprocess,
                            args=("buy_scanner", JOB_SCRIPTS["buy_scanner"]),
                            daemon=True
                        ).start()

                # -------------------------------------------------------------
                # 3. ช่วงเวลาตลาดเปิดทำการ (10:00-12:30 และ 14:30-16:30 น.)
                # -------------------------------------------------------------
                if is_market_hours(cur_time):
                    # 3.1 Sync Order Status: รันทุกๆ 5 นาที (00, 05, 10, ..., 55)
                    if cur_minute % 5 == 0:
                        if self.last_run_minute["sync_orders"] != current_minute_str:
                            self.last_run_minute["sync_orders"] = current_minute_str
                            threading.Thread(
                                target=run_job_subprocess,
                                args=("sync_orders", JOB_SCRIPTS["sync_orders"]),
                                daemon=True
                            ).start()

                    # 3.2 Sell Monitor: เฝ้าระวัง Stop Loss ทุกๆ 10 นาที (00, 10, 20, ..., 50)
                    if cur_minute % 10 == 0:
                        if self.last_run_minute["sell_monitor"] != current_minute_str:
                            self.last_run_minute["sell_monitor"] = current_minute_str
                            threading.Thread(
                                target=run_job_subprocess,
                                args=("sell_monitor", JOB_SCRIPTS["sell_monitor"]),
                                daemon=True
                            ).start()

                # -------------------------------------------------------------
                # 4. 16:40 น. -> Sync Orders รอบปิดตลาด (ตรวจ Order Expired)
                # -------------------------------------------------------------
                if now.hour == 16 and now.minute == 40:
                    if self.last_run_minute["sync_orders_close"] != current_minute_str:
                        self.last_run_minute["sync_orders_close"] = current_minute_str
                        threading.Thread(
                            target=run_job_subprocess,
                            args=("sync_orders", JOB_SCRIPTS["sync_orders"]),
                            daemon=True
                        ).start()

                # -------------------------------------------------------------
                # 5. 19:15 น. -> Task Update (ดึง EOD, คำนวณ Indicators, Trailing Stop)
                # -------------------------------------------------------------
                if now.hour == 19 and now.minute == 15:
                    if self.last_run_minute["task_update"] != current_minute_str:
                        self.last_run_minute["task_update"] = current_minute_str
                        threading.Thread(
                            target=run_job_subprocess,
                            args=("task_update", JOB_SCRIPTS["task_update"]),
                            daemon=True
                        ).start()

            # ตรวจสอบทุกๆ 5 วินาที
            for _ in range(5):
                if shutdown_event.is_set():
                    break
                time.sleep(1)


def handle_signals(signum, frame):
    """ดักจับสัญญาณปิดโปรแกรม (Ctrl+C หรือ SIGTERM) เพื่อปิด subprocess ทั้งหมดให้เรียบร้อย"""
    sig_name = signal.Signals(signum).name if hasattr(signal, "Signals") else str(signum)
    logger.info(f"\n🛑 ได้รับสัญญาณหยุดการทำงาน ({sig_name}) กำลังปิดระบบ...")
    shutdown_event.set()

    # ยุติ Child Subprocesses ทั้งหมดที่ยังรันอยู่
    for name, proc in list(running_subprocesses.items()):
        if proc and proc.poll() is None:
            logger.info(f"  - กำลังปิดงาน: [{name}] (PID {proc.pid})")
            try:
                proc.terminate()
            except Exception:
                pass

    time.sleep(1)
    logger.info("👋 ปิดระบบ Master Scheduler เรียบร้อยแล้ว")
    sys.exit(0)


def print_status():
    """แสดงข้อมูลสถานะและการตั้งเวลาปัจจุบัน"""
    now = datetime.now()
    weekend_tag = " (วันหยุดสุดสัปดาห์)" if is_weekend() else " (วันทำการ)"
    market_tag = "🟢 ตลาดเปิดทำการ" if is_market_hours(now.time()) and not is_weekend() else "🔴 ตลาดปิดทำการ"

    print("\n" + "="*70)
    print("📊 ข้อมูลสถานะ Master Automation Scheduler")
    print("="*70)
    print(f"• เวลาปัจจุบัน        : {now.strftime('%Y-%m-%d %H:%M:%S')} {weekend_tag}")
    print(f"• สถานะตลาดหุ้นไทย    : {market_tag}")
    print(f"• ไดเรกทอรีโปรเจกต์   : {ROOT_DIR}")
    print(f"• ไดเรกทอรี Logs      : {LOGS_DIR}")
    print(f"• Python Interpreter  : {sys.executable}")
    print("-"*70)
    print("📋 กำหนดการทำงานประจำวัน:")
    print("  [0] 24 ชั่วโมง        : bot/telegram_app.py (Supervisor เฝ้าระวังอัตโนมัติ)")
    print("  [1] 09:00 น.          : jobs/run_port_scanner.py (ตรวจชาร์ตหุ้นในพอร์ต)")
    print("  [2] 09:15 น.          : jobs/run_buy_scanner.py (สแกนหาจังหวะซื้อ)")
    print("  [3] 10:00-12:30 น.    : jobs/sync_order_status.py (ทุก 5 นาที)")
    print("                          jobs/run_sell_monitor.py (ทุก 10 นาที)")
    print("  [4] 14:30-16:30 น.    : jobs/sync_order_status.py (ทุก 5 นาที)")
    print("                          jobs/run_sell_monitor.py (ทุก 10 นาที)")
    print("  [5] 16:40 น.          : jobs/sync_order_status.py (รอบปิดตลาด)")
    print("  [6] 19:15 น.          : taskUpdate.py (EOD Price, Indicators, Trailing Stop)")
    print("="*70 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Master Automation Scheduler for Thai Stock Screener",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--no-bot", action="store_true", help="รันเฉพาะ Scheduler โดยไม่เปิด Telegram Bot")
    parser.add_argument("--only-bot", action="store_true", help="รันเฉพาะ Telegram Bot Supervisor")
    parser.add_argument("--run", type=str, choices=list(JOB_SCRIPTS.keys()), help="สั่งรันสคริปต์งานที่ระบุทันที 1 ครั้งเพื่อทดสอบ")
    parser.add_argument("--status", action="store_true", help="แสดงสถานะและกำหนดการทำงานปัจจุบัน")

    args = parser.parse_args()

    if args.status:
        print_status()
        return

    if args.run:
        job = args.run
        target_script = JOB_SCRIPTS[job]
        logger.info(f"🎯 กำลังสั่งรันแบบ Manual Run สำหรับงาน: [{job}] ...")
        run_job_subprocess(job, target_script)
        return

    # ลงทะเบียนดักจับสัญญาณ Exit
    signal.signal(signal.SIGINT, handle_signals)
    if sys.platform != "win32":
        signal.signal(signal.SIGTERM, handle_signals)

    print_status()
    logger.info("🛡️ ระบบ Master Scheduler กำลังเริ่มต้นการทำงาน (กด Ctrl+C เพื่อหยุด)...")

    bot_supervisor = None
    scheduler_engine = None

    # เริ่มรัน Telegram Bot Supervisor
    if not args.no_bot:
        bot_supervisor = TelegramBotSupervisor()
        bot_supervisor.start()

    # เริ่มรัน Scheduler Engine
    if not args.only_bot:
        scheduler_engine = SchedulerEngine()
        scheduler_engine.start()

    # Main Loop รอรับคำสั่งจนกว่าจะกด Ctrl+C
    try:
        while not shutdown_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        handle_signals(signal.SIGINT, None)


if __name__ == "__main__":
    main()
