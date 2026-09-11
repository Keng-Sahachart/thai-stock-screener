#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : risk_manager.py
LOCATION    : core/ หรือ root
OCCASION    : ถูกเรียกใช้โดย aggregator.py, callback_handlers.py
DESCRIPTION : คำนวณ Position Sizing ควบคุมความเสี่ยงและงบประมาณ 4 ชั้น:
              1. Risk Sizing (ยอมเสีย 1% ของพอร์ต)
              2. Cash Limit (ไม่เกิน line_available)
              3. Max Amount Cap (ไม่เกินเพดานเงินบาทต่อไม้)
              4. Max Shares Cap (ไม่เกินเพดานจำนวนหุ้นต่อไม้)
==============================================================================
"""

import pandas as pd
import numpy as np

# def calculate_position_size(symbol, entry_price, atr14, total_equity, risk_percent=0.01):
#     """
#     คำนวณจำนวนหุ้นที่ควรซื้อตามหลัก Risk Management
#     risk_percent: 0.01 คือยอมเสีย 1% ของพอร์ต
#     """
#     if pd.isna(atr14) or atr14 <= 0:
#         # ถ้าไม่มี ATR ให้ใช้ Default Risk ที่ 5% ของราคา
#         stop_loss = entry_price * 0.95 
#     else:
#         stop_loss = entry_price - (atr14 * 2) # ระยะ 2 เท่าของความผันผวน

#     risk_per_share = entry_price - stop_loss
    
#     if risk_per_share <= 0: return 0, stop_loss
    
#     # คำนวณจำนวนหุ้นจากเงินที่ยอมเสียได้
#     risk_amount = total_equity * risk_percent
#     shares_to_buy = int((risk_amount / risk_per_share) // 100) * 100
    
#     return shares_to_buy, stop_loss

def calculate_position_size_v2(
    symbol: str,
    entry_price: float,
    atr14: float,
    total_equity: float,
    available_cash: float,
    risk_percent: float = 0.01,
    max_position_pct: float = 0.20,
    default_shares: int = 0,
    fee_buffer_pct: float = 0.0025
):
    """
    คำนวณจำนวนหุ้นที่ควรซื้อตามหลัก Risk Management และ Budget Limits 3 ชั้น:
    1. Risk Sizing (ยอมเสีย 1% ของพอร์ต)
    2. Cash Limit (ไม่เกิน line_available)
    3. Max Position Cap (ไม่เกินเพดาน % ของพอร์ต)
    Parameters:
    - symbol: str, ชื่อหุ้น
    - entry_price: float, ราคาซื้อ
    - atr14: float, ATR 14 วัน
    - total_equity: float, เงินทุนรวม
    - available_cash: float, เงินสดที่มี
    - risk_percent: float, เปอร์เซ็นต์ความเสี่ยง
    - max_position_pct: float, เพดาน % ของพอร์ตต่อไม้
    - default_shares: int, จำนวนหุ้นคงที่ต่อไม้ (0 = ปรับตาม Position Sizing)
    - fee_buffer_pct: float, อัตราค่าธรรมเนียม buffer
    Returns:
    - final_shares: int, จำนวนหุ้นที่ควรซื้อ (ปัดเศษลงเป็น Board Lot)
    - stop_loss: float, จุด Stop Loss ที่คำนวณได้
    """
    if entry_price <= 0:
        return 0, 0.0

    # 1. กำหนดระยะ Stop Loss พื้นฐาน
    if pd.isna(atr14) or atr14 <= 0:
        stop_loss = entry_price * 0.95
    else:
        stop_loss = entry_price - (atr14 * 2.0)

    # ป้องกัน Stop Loss แคบหรือกว้างเกินไป (Safety Clamp 4% - 8%)
    loss_pct = ((entry_price - stop_loss) / entry_price) * 100.0
    if loss_pct < 4.0:
        stop_loss = entry_price * 0.96
    elif loss_pct > 8.0:
        stop_loss = entry_price * 0.92

    risk_per_share = entry_price - stop_loss
    if risk_per_share <= 0:
        return 0, round(stop_loss, 4)

    # 2. กรณีตั้งค่าจำนวนหุ้นคงที่ (Fixed Shares Override)
    if default_shares > 0:
        cost_estimate = default_shares * entry_price * (1.0 + fee_buffer_pct)
        if cost_estimate <= available_cash:
            return default_shares, round(stop_loss, 4)
        else:
            # ถ้าเงินไม่พอซื้อจำนวนคงที่ ให้คำนวณถอยกลับมาเท่าที่เงินซื้อได้
            max_shares_by_cash = int(available_cash / (entry_price * (1.0 + fee_buffer_pct)) // 100) * 100
            return max_shares_by_cash, round(stop_loss, 4)

    # 3. การคำนวณแบบ Dynamic (เพดาน 3 ชั้น)
    # ชั้นที่ 1: เพดานความเสี่ยง (Risk-based size)
    risk_amount = total_equity * risk_percent
    shares_by_risk = risk_amount / risk_per_share

    # ชั้นที่ 2: เพดานจำกัดน้ำหนักหุ้นต่อตัว (เช่น ไม่เกิน 20% ของพอร์ตรวม)
    max_capital_per_stock = total_equity * max_position_pct
    shares_by_cap = max_capital_per_stock / entry_price

    # ชั้นที่ 3: เพดานอำนาจซื้อเงินสดจริง (Line Available หักเผื่อค่าคอม)
    shares_by_cash = available_cash / (entry_price * (1.0 + fee_buffer_pct))

    # เลือกค่าที่เข้มงวดที่สุดใน 3 เงื่อนไข
    allowed_shares = min(shares_by_risk, shares_by_cap, shares_by_cash)

    # ปัดเศษลงเป็น Board Lot ของตลาดหุ้นไทย (100 หุ้น)
    final_shares = int(allowed_shares // 100) * 100

    return max(0, final_shares), round(stop_loss, 4)

def calculate_position_size(
    symbol: str,
    entry_price: float,
    atr14: float,
    total_equity: float,
    available_cash: float,
    risk_percent: float = 0.01,
    max_amount_per_trade: float = 500.0,
    max_shares_per_trade: int = 200,
    fee_buffer_pct: float = 0.0025
):
    '''
    คำนวณจำนวนหุ้นที่ควรซื้อตามหลัก Risk Management และ Budget Limits 4 ชั้น:
    1. Risk Sizing (ยอมเสีย 1% ของพอร์ต)
    2. Cash Limit (ไม่เกิน line_available)
    3. Max Amount Cap (ไม่เกินเพดานเงินบาทต่อไม้)
    4. Max Shares Cap (ไม่เกินเพดานจำนวนหุ้นต่อไม้)
    Parameters:
    - symbol: str, ชื่อหุ้น
    - entry_price: float, ราคาซื้อ
    - atr14: float, ATR 14 วัน
    - total_equity: float, เงินทุนรวม
    - available_cash: float, เงินสดที่มี
    - risk_percent: float, เปอร์เซ็นต์ความเสี่ยง
    - max_amount_per_trade: float, เพดานเงินบาทต่อไม้
    - max_shares_per_trade: int, เพดานจำนวนหุ้นต่อไม้
    - fee_buffer_pct: float, อัตราค่าธรรมเนียม buffer
    Returns:
    - final_shares: int, จำนวนหุ้นที่ควรซื้อ (ปัดเศษลงเป็น Board Lot)
    - stop_loss: float, จุด Stop Loss ที่คำนวณได้
    '''
    if entry_price <= 0:
        return 0, 0.0

    # 1. คำนวณ Stop Loss พร้อม Safety Clamp (4% - 8%)
    if pd.isna(atr14) or atr14 <= 0:
        stop_loss = entry_price * 0.95
    else:
        stop_loss = entry_price - (atr14 * 2.0)

    # ป้องกัน Stop Loss แคบหรือกว้างเกินไป (Safety Clamp 4% - 8%)
    loss_pct = ((entry_price - stop_loss) / entry_price) * 100.0
    if loss_pct < 4.0:
        stop_loss = entry_price * 0.96
    elif loss_pct > 8.0:
        stop_loss = entry_price * 0.92

    risk_per_share = entry_price - stop_loss
    if risk_per_share <= 0:
        return 0, round(stop_loss, 4)

    # 2. คำนวณเพดาน 4 มิติ
    # มิติที่ 1: เพดานความเสี่ยง (Risk-based)
    shares_by_risk = (total_equity * risk_percent) / risk_per_share

    # มิติที่ 2: เพดานอำนาจซื้อเงินสดจริง (Line Available)
    shares_by_cash = available_cash / (entry_price * (1.0 + fee_buffer_pct))

    # มิติที่ 3: เพดานเงินบาทต่อไม้ (เช่น ไม่เกิน 500 บ.)
    shares_by_amount_cap = max_amount_per_trade / (entry_price * (1.0 + fee_buffer_pct))

    # มิติที่ 4: เพดานจำนวนหุ้นต่อไม้ (เช่น ไม่เกิน 200 หุ้น)
    shares_by_share_cap = float(max_shares_per_trade)

    # เลือกจำนวนที่น้อยที่สุดในทุกมิติ
    allowed_shares = min(shares_by_risk, shares_by_cash, shares_by_amount_cap, shares_by_share_cap)

    # ปัดเศษลงเป็น Board Lot (คูณด้วย 100)
    final_shares = int(allowed_shares // 100) * 100

    return max(0, final_shares), round(stop_loss, 4)

def check_portfolio_risk(df_portfolio, df_indicators, total_equity):
    """
    [NEW] ตรวจสอบความเสี่ยงรวมของพอร์ต (Portfolio Heat)
    เพื่อเช็คว่าตอนนี้พอร์ต 'แบก' ความเสี่ยงคัทลอสรวมกี่ % ของเงินต้น
    """
    if df_portfolio.empty: return 0.0
    
    # 1. Join ข้อมูลพอร์ตกับ Indicator เพื่อเอาค่า ATR มาหาจุด Stop Loss ปัจจุบัน
    df_risk = df_portfolio.merge(df_indicators, on='symbol', how='left')
    
    # 2. คำนวณความเสี่ยงรายตัว: (ราคาเฉลี่ย - จุด Stop Loss) * จำนวนหุ้น
    # เราใช้ ATR ล่าสุดมาหาจุดหนีปัจจุบัน
    df_risk['current_sl'] = df_risk['market_price'] - (df_risk['atr14'] * 2)
    df_risk['risk_value'] = (df_risk['average_price'] - df_risk['current_sl']) * df_risk['current_volume']
    
    # เฉพาะตัวที่ราคาอยู่เหนือ SL (ถ้าหลุด SL ไปแล้วถือว่าความเสี่ยงเกิดขึ้นจริงแล้ว)
    total_risk_val = df_risk[df_risk['risk_value'] > 0]['risk_value'].sum()
    
    # 3. แปลงเป็น % ของพอร์ตรวม
    risk_pct = (total_risk_val / total_equity) * 100
    return risk_pct

def is_market_trading_time() -> tuple[bool, str]:
    """
    ตรวจสอบว่าเวลาปัจจุบันอยู่ในช่วงเวลาเปิดทำการซื้อขายของตลาดหุ้นไทย (SET) หรือไม่:
    - วันทำการ: จันทร์ - ศุกร์ (weekday 0 ถึง 4)
    - รอบเช้า: 09:55 - 12:35 น.
    - รอบบ่าย: 14:25 - 16:35 น.
    :return: (is_open: bool, reason: str)
    """
    from datetime import datetime, time
    now = datetime.now()
    if now.weekday() >= 5:
        return False, "ตลาดปิดทำการ (วันหยุดสุดสัปดาห์ เสาร์-อาทิตย์)"

    cur_time = now.time()
    m_open = time(9, 55)
    m_close = time(12, 35)
    a_open = time(14, 25)
    a_close = time(16, 35)

    if (m_open <= cur_time <= m_close) or (a_open <= cur_time <= a_close):
        return True, "ตลาดเปิดทำการปกติ"

    if cur_time < m_open:
        return False, f"ตลาดปิดทำการ (ก่อนเวลาเปิดตลาดรอบเช้า เวลาปัจจุบัน {now.strftime('%H:%M:%S')} น.)"
    elif m_close < cur_time < a_open:
        return False, f"ตลาดปิดทำการ (ช่วงพักกลางวัน Intermission เวลาปัจจุบัน {now.strftime('%H:%M:%S')} น.)"
    else:
        return False, f"ตลาดปิดทำการ (Off-hour ตลาดปิดแล้ว เวลาปัจจุบัน {now.strftime('%H:%M:%S')} น.)"

#ปรับแต่งตรงไหนได้บ้าง?
# Risk Percent (ความเสี่ยงต่อไม้):

# สายเซฟ: ปรับเป็น 0.005 (0.5%)

# สายมาตรฐาน: ปรับเป็น 0.01 (1%)

# สายซิ่ง: ปรับเป็น 0.02 (2%) (ไม่แนะนำให้เกินนี้)

# ATR Multiplier (ตัวคูณความผันผวน):

# หุ้นพื้นฐานดี (นิ่ง): ใช้ 2 เท่าของ ATR

# หุ้นปั่น/หุ้นซิ่ง (ผันผวนสูง): อาจปรับเป็น 2.5 หรือ 3 เพื่อให้จุด Stop Loss อยู่ห่างออกไป ป้องกันการโดนสะบัดหลุด

# Total Equity Base:

# คุณเลือกได้ว่าจะคำนวณจาก "เงินสดที่เหลือ" หรือ "มูลค่าพอร์ตทั้งหมด"

# แนะนำ: คำนวณจากมูลค่าพอร์ตทั้งหมดเพื่อให้ Position Size ขยายตัวตามความมั่งคั่งที่เพิ่มขึ้น (Compounding Effect)