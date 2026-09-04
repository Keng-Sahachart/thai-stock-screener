#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : order_manager.py
LOCATION    : execution/
OCCASION    : ถูกเรียกใช้งานทันทีเมื่อมีการกด Approve บน Telegram หรือเมื่อ Trigger ฝั่งขายทำงาน
DESCRIPTION : ผู้จัดการวงจรชีวิตคำสั่งซื้อขาย (Order Lifecycle Manager):
              - ตรวจสอบโหมดระบบ (DRY_RUN vs LIVE) จาก config/bot_config.json
              - ทำ Pre-flight Check (ตรวจเงินสดคงเหลือและสถานะซ้ำซ้อน)
              - มอบหมายงานให้ dry_run_executor หรือ settrade_executor ดำเนินการ
==============================================================================
"""

import os
import json
from execution.dry_run_executor import execute_dry_run_buy, execute_dry_run_sell
from execution.settrade_executor import execute_real_buy, execute_real_sell

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "bot_config.json")

def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def place_buy_order(signal_id: int, symbol: str, volume: int, target_price: float, stop_loss_plan: float, atr14: float = None) -> dict:
    """ตรวจสอบการตั้งค่าและเลือกช่องทางการยิงคำสั่งซื้อ"""
    config = load_config()
    is_dry_run = config.get("trading_mode", {}).get("dry_run", True)

    if is_dry_run:
        print(f"[ORDER MGR] Processing BUY order for {symbol} in DRY_RUN mode")
        return execute_dry_run_buy(signal_id, symbol, volume, target_price, stop_loss_plan, atr14)
    else:
        print(f"[ORDER MGR] Processing BUY order for {symbol} in LIVE mode (Real Trade)")
        return execute_real_buy(signal_id, symbol, volume, target_price, stop_loss_plan, atr14)

def place_sell_order(symbol: str, volume: int, exit_price: float, exit_reason: str) -> dict:
    """ตรวจสอบการตั้งค่าและเลือกช่องทางการยิงคำสั่งขาย"""
    config = load_config()
    is_dry_run = config.get("trading_mode", {}).get("dry_run", True)

    if is_dry_run:
        print(f"[ORDER MGR] Processing SELL order for {symbol} in DRY_RUN mode ({exit_reason})")
        return execute_dry_run_sell(symbol, volume, exit_price, exit_reason)
    else:
        print(f"[ORDER MGR] Processing SELL order for {symbol} in LIVE mode ({exit_reason})")
        return execute_real_sell(symbol, volume, exit_price, exit_reason)