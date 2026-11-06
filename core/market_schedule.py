#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : market_schedule.py
LOCATION    : core/
DESCRIPTION : SET Market Schedule, Holiday Calendar & Real-time Failover Manager
              ระบบจัดการปฏิทินวันหยุดตลาดหลักทรัพย์แห่งประเทศไทย (SET)
              และระบบตรวจจับสถานะตลาดจริง (Hybrid + Real-time Failover)
==============================================================================
"""

import os
import json
import logging
from datetime import datetime, date, time as dtime
from pathlib import Path
from collections import Counter
from typing import Tuple, Optional, Dict, Any

logger = logging.getLogger("MarketSchedule")

ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT_DIR / "config" / "market_holidays.json"


def load_holidays() -> Dict[str, Dict[str, str]]:
    """โหลดข้อมูลวันหยุดตลาดหลักทรัพย์จากไฟล์ JSON"""
    if not CONFIG_PATH.exists():
        return {}
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load market holidays from {CONFIG_PATH}: {e}")
        return {}


def save_holidays(holidays_data: Dict[str, Dict[str, str]]) -> bool:
    """บันทึกข้อมูลวันหยุดตลาดหลักทรัพย์ลงไฟล์ JSON"""
    try:
        CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(holidays_data, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        logger.error(f"Failed to save market holidays to {CONFIG_PATH}: {e}")
        return False


def is_weekend(target_date: Optional[date] = None) -> bool:
    """
    ตรวจสอบว่าเป็นวันหยุดเสาร์-อาทิตย์หรือไม่
    :param target_date: วันที่ต้องการตรวจ (default: วันปัจจุบัน)
    :return: True หากเป็นวันเสาร์ (5) หรือวันอาทิตย์ (6)
    """
    if target_date is None:
        target_date = date.today()
    return target_date.weekday() >= 5


def get_holiday_desc(target_date: Optional[date] = None) -> Optional[str]:
    """
    ตรวจสอบว่าวันที่กำหนดเป็นวันหยุดนักขัตฤกษ์ของตลาดหลักทรัพย์หรือไม่
    :param target_date: วันที่ต้องการตรวจ (default: วันปัจจุบัน)
    :return: ชื่อวันหยุด (str) หากเป็นวันหยุด, หรือ None หากเป็นวันปกติ
    """
    if target_date is None:
        target_date = date.today()
    
    date_str = target_date.strftime("%Y-%m-%d")
    year_str = str(target_date.year)
    
    holidays = load_holidays()
    year_holidays = holidays.get(year_str, {})
    return year_holidays.get(date_str)


def is_calendar_holiday(target_date: Optional[date] = None) -> Tuple[bool, Optional[str]]:
    """
    ตรวจสอบว่าอยู่ในปฏิทินวันหยุดตลาดหลักทรัพย์หรือไม่
    :return: (is_holiday: bool, description: Optional[str])
    """
    desc = get_holiday_desc(target_date)
    if desc:
        return True, desc
    return False, None


def is_trading_day_calendar(target_date: Optional[date] = None) -> Tuple[bool, str]:
    """
    ตรวจสอบความพร้อมของวันทำการตามปฏิทิน (เสาร์-อาทิตย์ + ปฏิทินวันหยุด SET)
    :return: (is_trading_day: bool, reason: str)
    """
    if target_date is None:
        target_date = date.today()

    if is_weekend(target_date):
        day_name = "เสาร์" if target_date.weekday() == 5 else "อาทิตย์"
        return False, f"วันหยุดสุดสัปดาห์ (วัน{day_name})"

    is_hol, hol_desc = is_calendar_holiday(target_date)
    if is_hol:
        return False, f"วันหยุดตลาดหลักทรัพย์ ({hol_desc})"

    return True, "วันทำการตามปฏิทินปกติ"


def verify_market_open_live(min_consensus: float = 0.6) -> Tuple[bool, str, float]:
    """
    ตรวจสอบสถานะตลาดสดผ่าน Settrade Open API (Real-time Failover Check)
    โดยตรวจสถานะ marketStatus ของหุ้น Big Cap 10 ตัว
    
    :param min_consensus: สัดส่วนหุ้นขั้นต่ำที่ต้องมีสถานะ Open1 หรือ Open2 (default 60%)
    :return: (is_open: bool, summary_reason: str, open_ratio: float)
    """
    try:
        from settrade_v2 import Investor
        import initialApp as cfg
    except Exception as e:
        logger.warning(f"Could not import Settrade Investor/config: {e}")
        return True, f"ข้ามการเช็คสดเนื่องจากโมดูล API ไม่พร้อม: {e}", 1.0

    symbols = [
        "PTT", "CPALL", "DELTA", "AOT", "SCB", 
        "KBANK", "ADVANC", "BDMS", "GULF", "PTTEP"
    ]
    open_statuses = {"Open1", "Open2"}
    status_records = []

    try:
        investor = Investor(**cfg.args_Investor)
        market = investor.MarketData()

        for sym in symbols:
            try:
                quote = market.get_quote_symbol(sym)
                status = quote.get("marketStatus")
                if status:
                    status_records.append(status)
            except Exception:
                continue

    except Exception as e:
        logger.warning(f"Settrade API Connection error in verify_market_open_live: {e}")
        # หากต่อ API ไม่ได้จากเน็ตเวิร์กชั่วคราว ให้ถือว่าไม่บล็อกงานปกติ แต่แจ้งเตือน
        return True, f"เกิดข้อผิดพลาดในการเชื่อมต่อ Settrade ({e})", 1.0

    if not status_records:
        return True, "ไม่สามารถดึงข้อมูลสถานะจากหุ้นตัวใดได้เลย (ถือว่าผ่านตามปฏิทิน)", 1.0

    counts = Counter(status_records)
    total_valid = len(status_records)
    open_count = sum(counts[st] for st in open_statuses if st in counts)
    open_ratio = open_count / total_valid
    dominant_status, dominant_count = counts.most_common(1)[0]

    if open_ratio >= min_consensus:
        reason = f"ตลาดเปิดทำการปกติ (สถานะส่วนใหญ่: {dominant_status} {dominant_count}/{total_valid}, {open_ratio:.0%})"
        return True, reason, open_ratio
    else:
        reason = f"ตลาดปิดทำการ (สถานะส่วนใหญ่: {dominant_status} {dominant_count}/{total_valid}, เปิด {open_ratio:.0%})"
        return False, reason, open_ratio


def add_holiday(date_str: str, description: str) -> Tuple[bool, str]:
    """
    เพิ่มวันหยุดนักขัตฤกษ์ลงในปฏิทิน
    :param date_str: รูปแบบ 'YYYY-MM-DD'
    :param description: คำอธิบาย เช่น 'วันหยุดพิเศษ ครม.'
    """
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return False, "รูปแบบวันที่ไม่ถูกต้อง (ต้องเป็น YYYY-MM-DD เช่น 2026-10-16)"

    holidays = load_holidays()
    year_str = str(d.year)
    if year_str not in holidays:
        holidays[year_str] = {}

    holidays[year_str][date_str] = description.strip()
    if save_holidays(holidays):
        return True, f"เพิ่มวันหยุด {date_str} ({description}) สำเร็จ"
    return False, "เกิดข้อผิดพลาดในการบันทึกไฟล์"


def remove_holiday(date_str: str) -> Tuple[bool, str]:
    """
    ลบวันหยุดออกจากปฏิทิน
    :param date_str: รูปแบบ 'YYYY-MM-DD'
    """
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return False, "รูปแบบวันที่ไม่ถูกต้อง (ต้องเป็น YYYY-MM-DD เช่น 2026-10-16)"

    holidays = load_holidays()
    year_str = str(d.year)
    if year_str in holidays and date_str in holidays[year_str]:
        del holidays[year_str][date_str]
        if save_holidays(holidays):
            return True, f"ลบวันหยุด {date_str} เรียบร้อยแล้ว"
    return False, f"ไม่พบวันหยุด {date_str} ในปฏิทิน"
