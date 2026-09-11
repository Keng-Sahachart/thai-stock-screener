#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : tick_utils.py
LOCATION    : core/
OCCASION    : ถูกเรียกใช้เมื่อต้องการคำนวณหาราคาเสนอซื้อ-ขายตามขั้นราคาตลาดหลักทรัพย์
DESCRIPTION : คำนวณ Tick Size และปรับราคาขึ้น/ลงตาม Board Lot ของ SET
==============================================================================
"""

def get_tick_size(price: float, direction: int = 1) -> float:
    """คืนค่าขนาดช่องราคา (Tick) ตามกฎ SET โดยพิจารณาทิศทางการขยับราคา"""
    check_p = price if direction > 0 else round(price - 0.0001, 4)
    if check_p < 2.0:
        return 0.01
    elif check_p < 5.0:
        return 0.02
    elif check_p < 10.0:
        return 0.05
    elif check_p < 25.0:
        return 0.10
    elif check_p < 100.0:
        return 0.25
    elif check_p < 200.0:
        return 0.50
    elif check_p < 400.0:
        return 1.00
    else:
        return 2.00

def adjust_price_by_ticks(base_price: float, ticks: int) -> float:
    """
    ปรับราคาตามจำนวน Ticks
    - ticks > 0: ขยับราคาขึ้น (เช่น ขาย Take Profit)
    - ticks < 0: ขยับราคาลง (เช่น ขายรีบ Match ที่ Bid)
    """
    if ticks == 0 or base_price <= 0:
        return round(base_price, 2)

    curr = round(base_price, 2)
    step = 1 if ticks > 0 else -1
    for _ in range(abs(ticks)):
        t = get_tick_size(curr, step)
        curr = round(curr + (step * t), 2)
    return curr


