#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : audit_logger.py
LOCATION    : core/
DESCRIPTION : ระบบบันทึกประวัติการทำงาน (Audit Trail & Event Logger)
              ทำหน้าที่บันทึกเหตุการณ์สำคัญลงตาราง public.bot_system_logs
              และแสดงผลออกหน้าจอ console อย่างปลอดภัย (Non-blocking)
==============================================================================
"""

import os
import sys
import json
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv

if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

load_dotenv()

DDL = """
CREATE TABLE IF NOT EXISTS public.bot_system_logs (
    log_id          BIGSERIAL PRIMARY KEY,
    event_type      VARCHAR(50) NOT NULL,
    level           VARCHAR(20) DEFAULT 'INFO',
    symbol          VARCHAR(20),
    message         TEXT NOT NULL,
    raw_payload     JSONB,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT timezone('Asia/Bangkok', now())
);
CREATE INDEX IF NOT EXISTS idx_bot_system_logs_event_date ON public.bot_system_logs(event_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_bot_system_logs_symbol ON public.bot_system_logs(symbol) WHERE symbol IS NOT NULL;
"""

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("posql_host", "localhost"),
        port=os.getenv("posql_port", "5432"),
        dbname=os.getenv("posql_db", "stocks"),
        user=os.getenv("posql_user", "postgres"),
        password=os.getenv("posql_password", "postgres")
    )

def ensure_log_table(conn=None):
    """ตรวจสอบและสร้างตาราง bot_system_logs หากยังไม่มี"""
    own_conn = False
    if conn is None or conn.closed:
        try:
            conn = get_db_connection()
            own_conn = True
        except Exception as e:
            print(f"[AUDIT LOG INIT ERROR] ไม่สามารถเชื่อมต่อ DB: {e}")
            return False

    try:
        with conn.cursor() as cur:
            cur.execute(DDL)
        if own_conn:
            conn.commit()
        return True
    except Exception as e:
        print(f"[AUDIT LOG DDL ERROR] {e}")
        return False
    finally:
        if own_conn and conn:
            conn.close()

def log_event(
    event_type: str,
    message: str,
    symbol: str = None,
    level: str = "INFO",
    raw_payload: dict = None,
    conn=None
) -> bool:
    """
    บันทึกเหตุการณ์สำคัญลงตาราง bot_system_logs
    
    :param event_type: ประเภทเหตุการณ์ เช่น 'ORDER_BUY', 'ORDER_SELL', 'REJECTED_BUDGET', 'EXPIRED_SIGNAL'
    :param message: ข้อความอธิบายเหตุการณ์
    :param symbol: ชื่อย่อหุ้นที่เกี่ยวข้อง (ถ้ามี)
    :param level: ระดับความสำคัญ 'INFO', 'WARNING', 'ERROR'
    :param raw_payload: ข้อมูลดิบหรือพารามิเตอร์เกี่ยวข้อง (จะถูกแปลงเป็น JSONB)
    :param conn: Connection ที่มีอยู่เดิม (ถ้าไม่มีจะเปิดใหม่และปิดเอง)
    """
    level_icons = {
        "INFO": "ℹ️",
        "WARNING": "⚠️",
        "ERROR": "❌",
        "SUCCESS": "✅"
    }
    icon = level_icons.get(level.upper(), "📝")
    sym_tag = f"[{symbol}] " if symbol else ""
    try:
        print(f"{icon} [AUDIT:{level.upper()}] [{event_type}] {sym_tag}{message}")
    except Exception:
        pass

    own_conn = False
    if conn is None or conn.closed:
        try:
            conn = get_db_connection()
            own_conn = True
        except Exception as e:
            print(f"[AUDIT LOG DB ERROR] ไม่สามารถเปิด DB เพื่อบันทึก: {e}")
            return False

    try:
        payload_val = Json(raw_payload) if raw_payload is not None else None
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO public.bot_system_logs (
                    event_type, level, symbol, message, raw_payload
                ) VALUES (%s, %s, %s, %s, %s);
            """, (event_type, level.upper(), symbol, message, payload_val))
        if own_conn:
            conn.commit()
        return True
    except Exception as e:
        # ป้องกันไม่ให้ error ของการทำ Log ไปทำให้ระบบส่งคำสั่งหยุดชะงัก
        print(f"[AUDIT LOG WRITE ERROR] บันทึกเหตุการณ์ล้มเหลว: {e}")
        return False
    finally:
        if own_conn and conn:
            conn.close()

if __name__ == "__main__":
    ensure_log_table()
    log_event("SYSTEM_TEST", "ทดสอบระบบ Audit Logger", symbol="TEST", level="SUCCESS", raw_payload={"test": True})
