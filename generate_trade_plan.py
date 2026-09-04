#(ระบบวางแผนการเทรด)
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse
# import psycopg2
import os
from dotenv import load_dotenv
import risk_manager as rm
load_dotenv()

# ตั้งค่าความเสี่ยงที่คุณรับได้ (ปรับแต่งได้ที่นี่)
RISK_PER_TRADE = 0.01  # ยอมเสีย 1% ของพอร์ตต่อหนึ่งไม้
ATR_MULTIPLIER = 2.0   # ระยะ Stop Loss เป็น 2 เท่าของความผันผวน (ATR)

# def get_db_connection():
#     return psycopg2.connect(
#         host=os.getenv('posql_host'),
#         port=os.getenv('posql_port'),
#         dbname=os.getenv('posql_db'),
#         user=os.getenv('posql_user'),
#         password=os.getenv('posql_password')
#     )

def get_db_engine():
    # โหลดค่าจาก .env
    user = os.getenv("posql_user")
    password = urllib.parse.quote_plus(os.getenv("posql_password")) # จัดการตัวอักษรพิเศษในรหัสผ่าน
    host = os.getenv("posql_host")
    port = os.getenv("posql_port", "5432")
    dbname = os.getenv("posql_db")
    
    # สร้าง Connection String สำหรับ SQLAlchemy
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(conn_str)

def fetch_all_data():
    engine = get_db_engine()
    
    with engine.connect() as conn:
        # 1. ดึงมูลค่าพอร์ตรวม (Total Equity)
        q_equity = text("SELECT SUM(market_value) FROM portfolio_stock;")
        total_equity = conn.execute(q_equity).scalar() or 1000000.0

        # 2. ดึงหุ้นที่ผ่านเกณฑ์ Hybrid (จาก View)
        q_targets = text("""
            SELECT * FROM v_hybrid_stock_selection 
            WHERE trade_date = (SELECT MAX(trade_date) FROM stock_signal);
        """)
        df_targets = pd.read_sql(q_targets, conn)

        # 3. ดึงข้อมูลพอร์ตปัจจุบัน
        q_portfolio = text("SELECT symbol, average_price, current_volume, market_price FROM portfolio_stock;")
        df_portfolio = pd.read_sql(q_portfolio, conn)

        # 4. ดึง ATR ล่าสุดของทุกตัว
        q_inds = text("""
            SELECT symbol, atr14 FROM mv_stock_indicators 
            WHERE trade_date = (SELECT MAX(trade_date) FROM stock_indicator_jsonb);
        """)
        df_all_inds = pd.read_sql(q_inds, conn)

    return total_equity, df_targets, df_portfolio, df_all_inds

def main():
    total_equity, df_targets, df_portfolio, df_all_inds = fetch_all_data()
    
    # [STEP 1] เช็คความเสี่ยงรวมปัจจุบัน (Portfolio Heat)
    # กฎเหล็ก: ถ้าเสี่ยงรวมเกิน 6% ของเงินต้น ห้ามเพิ่มสถานะใหม่
    current_heat = rm.check_portfolio_risk(df_portfolio, df_all_inds, total_equity)
    print(f"🔥 Current Portfolio Heat: {current_heat:.2f}%")
    
    if current_heat > 6.0: 
        print("⚠️ Risk Limit Exceeded: พอร์ตมีความเสี่ยงรวมสูงเกิน 6% แล้ว งดเพิ่มหุ้นใหม่")
        return
    
    if df_targets.empty:
        print("คัดกรองแล้ว: วันนี้ไม่มีหุ้นที่ผ่านเกณฑ์ Hybrid (พื้นฐานดี + เทคนิค BUY)")
        return

    print(f"\n=== Trading Plan (Total Equity: {total_equity:,.2f} THB) ===")
    print(f"Risk Per Trade: {RISK_PER_TRADE*100}% ({total_equity * RISK_PER_TRADE:,.0f} THB/ไม้)")
    print("-" * 90)

    # [STEP 2] คำนวณแผนการซื้อสำหรับหุ้นที่ติดโผ
    results = []
    for _, r in df_targets.iterrows():
        # เรียกใช้ฟังก์ชันจาก risk_manager โดยตรง
        final_shares, stop_loss = rm.calculate_position_size(
            symbol=r['symbol'],
            entry_price=float(r['last_price']),
            atr14=float(r['atr14']) if r['atr14'] else None,
            total_equity=total_equity,
            risk_percent=RISK_PER_TRADE
        )

        if final_shares > 0:
            total_cost = final_shares * float(r['last_price'])
            results.append({
                "Symbol": r['symbol'],
                "Signal": r['signal_type'],
                "Score": round(r['value_score'], 2),
                "Buy Price": f"{float(r['last_price']):.2f}",
                "Stop Loss": f"{stop_loss:.2f}",
                "Amount": f"{final_shares:,}",
                "Total Cost": f"{total_cost:,.2f}"
            })

    if results:
        df_plan = pd.DataFrame(results)
        print(df_plan.to_string(index=False))
    else:
        print("ไม่มีหุ้นที่เหมาะสมกับขนาดพอร์ตปัจจุบัน (Position Size เล็กเกินไป)")

    print("-" * 90)
    print("Strategy: BUY at open if price not gap up too much. Set Stop Loss immediately.")
    
if __name__ == "__main__":
    main()