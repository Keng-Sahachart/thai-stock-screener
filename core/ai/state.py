#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : state.py
LOCATION    : core/ai/
OCCASION    : จัดเก็บและเรียกดูผลการประเมิน AI ล่าสุด เพื่อแชร์ข้าม Process / Module
DESCRIPTION : AI State Persistence Layer
==============================================================================
"""

import os
import json
from pathlib import Path

STATE_FILE = Path(__file__).resolve().parent.parent.parent / "logs" / "last_ai_evaluation.json"

def save_last_evaluation(ai_result: dict):
    """บันทึกผลการประเมินล่าสุดลงไฟล์ JSON"""
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(ai_result, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[AI State ERROR] ไม่สามารถบันทึก state: {e}")

def get_last_evaluation() -> dict:
    """ดึงผลการประเมินล่าสุดจากไฟล์ JSON"""
    try:
        if STATE_FILE.exists():
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        print(f"[AI State ERROR] ไม่สามารถอ่าน state: {e}")
    return {}
