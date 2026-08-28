import pandas as pd
import numpy as np

def calculate_position_size(symbol, entry_price, atr14, total_equity, risk_percent=0.01):
    """
    คำนวณจำนวนหุ้นที่ควรซื้อตามหลัก Risk Management
    risk_percent: 0.01 คือยอมเสีย 1% ของพอร์ต
    """
    if pd.isna(atr14) or atr14 <= 0:
        # ถ้าไม่มี ATR ให้ใช้ Default Risk ที่ 5% ของราคา
        stop_loss = entry_price * 0.95 
    else:
        stop_loss = entry_price - (atr14 * 2) # ระยะ 2 เท่าของความผันผวน

    risk_per_share = entry_price - stop_loss
    
    if risk_per_share <= 0: return 0, stop_loss
    
    # คำนวณจำนวนหุ้นจากเงินที่ยอมเสียได้
    risk_amount = total_equity * risk_percent
    shares_to_buy = int((risk_amount / risk_per_share) // 100) * 100
    
    return shares_to_buy, stop_loss

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