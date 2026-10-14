#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : config_manager.py
LOCATION    : core/
DESCRIPTION : โมดูลกลางจัดการอ่านและเขียนไฟล์การตั้งค่า config/bot_config.json
              - จัดการ Excluded Symbols (ดูรายชื่อ, เพิ่ม, ลบ)
              - จัดการ Job Notifications Toggle
              - จัดการ Trading Mode และ Risk Controls
              - รับประกันการบันทึกแบบ UTF-8 Indent 4 สวยงาม ปลอดภัย
==============================================================================
"""

import os
import json
from pathlib import Path
from typing import List, Tuple, Dict, Any

ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT_DIR / "config" / "bot_config.json"


def load_bot_config() -> Dict[str, Any]:
    """โหลดข้อมูลการตั้งค่าทั้งหมดจาก bot_config.json"""
    try:
        if not CONFIG_PATH.exists():
            return {}
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[CONFIG_MANAGER] Error reading config: {e}")
        return {}


def save_bot_config(config_data: Dict[str, Any]) -> bool:
    """บันทึกข้อมูลการตั้งค่าลง bot_config.json"""
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(config_data, f, indent=4, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"[CONFIG_MANAGER] Error saving config: {e}")
        return False


def get_excluded_symbols() -> List[str]:
    """ดึงรายชื่อหุ้นที่ได้รับการยกเว้นทั้งหมด"""
    cfg = load_bot_config()
    return cfg.get("excluded_symbols", [])


def add_excluded_symbols(symbols: List[str]) -> Tuple[List[str], List[str], List[str]]:
    """
    เพิ่มรายชื่อหุ้นเข้า excluded_symbols
    Returns:
        (added_list, already_in_list, current_full_list)
    """
    cfg = load_bot_config()
    current_list = cfg.get("excluded_symbols", [])
    current_set = set(s.upper() for s in current_list)

    added = []
    already_in = []

    for s in symbols:
        sym_clean = s.strip().upper()
        if not sym_clean:
            continue
        if sym_clean in current_set:
            already_in.append(sym_clean)
        else:
            current_list.append(sym_clean)
            current_set.add(sym_clean)
            added.append(sym_clean)

    if added:
        cfg["excluded_symbols"] = current_list
        save_bot_config(cfg)

    return added, already_in, current_list


def remove_excluded_symbols(symbols: List[str]) -> Tuple[List[str], List[str], List[str]]:
    """
    นำรายชื่อหุ้นออกจาก excluded_symbols
    Returns:
        (removed_list, not_found_list, current_full_list)
    """
    cfg = load_bot_config()
    current_list = cfg.get("excluded_symbols", [])
    current_set = set(s.upper() for s in current_list)

    removed = []
    not_found = []

    for s in symbols:
        sym_clean = s.strip().upper()
        if not sym_clean:
            continue
        if sym_clean in current_set:
            removed.append(sym_clean)
            current_set.remove(sym_clean)
        else:
            not_found.append(sym_clean)

    if removed:
        # รักษาลำดับเดิมไว้เฉพาะตัวที่ไม่ได้ถูกลบ
        new_list = [s for s in current_list if s.upper() not in set(removed)]
        cfg["excluded_symbols"] = new_list
        save_bot_config(cfg)
        return removed, not_found, new_list

    return removed, not_found, current_list


# ==============================================================================
# AUTO-SELL CONTROLS MANAGEMENT
# ==============================================================================

AUTO_SELL_FIELD_INFO = {
    "enable_auto_cut_loss": {
        "code": "cut_loss",
        "name": "Hard Cut Loss (-10%)",
        "short_name": "Cut Loss",
        "icon": "🛑",
        "aliases": ["cut_loss", "cutloss", "cut", "hard", "hard_cut", "enable_auto_cut_loss", "ตัดขาดทุน"]
    },
    "enable_auto_stop_loss": {
        "code": "stop_loss",
        "name": "Initial Stop Loss",
        "short_name": "Stop Loss",
        "icon": "🛡️",
        "aliases": ["stop_loss", "stoploss", "sl", "initial", "initial_sl", "enable_auto_stop_loss", "สต็อป"]
    },
    "enable_auto_trailing_stop": {
        "code": "trailing_stop",
        "name": "Trailing Stop Loss",
        "short_name": "Trailing Stop",
        "icon": "📈",
        "aliases": ["trailing_stop", "trailingstop", "trailing", "trail", "ts", "enable_auto_trailing_stop", "เทรลลิ่ง"]
    },
    "enable_auto_technical_sell": {
        "code": "technical_sell",
        "name": "Technical Sell Signal",
        "short_name": "Tech Sell",
        "icon": "📊",
        "aliases": ["technical_sell", "technicalsell", "tech", "technical", "signal", "enable_auto_technical_sell", "เทคนิคอล"]
    }
}


def resolve_auto_sell_field(key: str) -> Tuple[str, Dict[str, Any]]:
    """แปลง alias หรือชื่อสวิตช์เป็นชื่อ field ใน bot_config.json"""
    if not key:
        return None, None
    k_clean = key.strip().lower()
    for field, info in AUTO_SELL_FIELD_INFO.items():
        if k_clean == field.lower() or k_clean in info["aliases"]:
            return field, info
    return None, None


def get_auto_sell_controls() -> Dict[str, bool]:
    """ดึงสถานะสวิตช์ Auto-Sell ทั้งหมด"""
    cfg = load_bot_config()
    ctrl = cfg.get("auto_sell_controls", {})
    return {
        field: bool(ctrl.get(field, False))
        for field in AUTO_SELL_FIELD_INFO
    }


def set_auto_sell_control(key: str, enabled: bool) -> Tuple[bool, str, str]:
    """
    เปิดหรือปิดสวิตช์ Auto-Sell รายตัว
    Returns: (success, field_name, display_name)
    """
    field, info = resolve_auto_sell_field(key)
    if not field:
        return False, "", ""
    cfg = load_bot_config()
    if "auto_sell_controls" not in cfg:
        cfg["auto_sell_controls"] = {}
    cfg["auto_sell_controls"][field] = enabled
    ok = save_bot_config(cfg)
    return ok, field, info["name"]


def toggle_auto_sell_control(key: str) -> Tuple[bool, bool, str, str]:
    """
    สลับสถานะ (Toggle) สวิตช์ Auto-Sell รายตัว
    Returns: (success, new_state, field_name, display_name)
    """
    field, info = resolve_auto_sell_field(key)
    if not field:
        return False, False, "", ""
    cfg = load_bot_config()
    if "auto_sell_controls" not in cfg:
        cfg["auto_sell_controls"] = {}
    current_val = bool(cfg["auto_sell_controls"].get(field, False))
    new_val = not current_val
    cfg["auto_sell_controls"][field] = new_val
    ok = save_bot_config(cfg)
    return ok, new_val, field, info["name"]


def set_all_auto_sell_controls(enabled: bool) -> bool:
    """เปิดหรือปิดสวิตช์ Auto-Sell ทุกตัวพร้อมกัน"""
    cfg = load_bot_config()
    if "auto_sell_controls" not in cfg:
        cfg["auto_sell_controls"] = {}
    for field in AUTO_SELL_FIELD_INFO:
        cfg["auto_sell_controls"][field] = enabled
    return save_bot_config(cfg)

