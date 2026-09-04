#ไฟล์นี้เป็นสคริปต์หลักสำหรับรันงานอัพเดตข้อมูลหุ้นต่างๆ โดยจะเรียกใช้ฟังก์ชันจากโมดูลอื่นๆ เพื่ออัพเดตข้อมูลรายชื่อหุ้น, ข้อมูลพื้นฐาน, คะแนนมูลค่าหุ้น, ราคาหุ้น, ตัวชี้วัดทางเทคนิค และสัญญาณการซื้อขาย
import asyncio
from datetime import date, datetime, timedelta

import update.updateStockList as usl
import update.updateStockInfo_siamChart as usi
import update.stockScore_siamChart as ssc
import update.updateStockPrice as usp
import update.updatePort as uport
import update.update_Port_info as uportinfo

# import compute_indicators_v3 as com_ind
import indicators.compute_indicators_v5 as com_ind_v5 # jsonb version
import indicators.compute_signalsV2 as com_sig
from jobs.run_eod_sync import sync_eod_portfolio

def main():
    print("=========================================")
    todayYYYYMMDD_hhmmss = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Task Update started at {todayYYYYMMDD_hhmmss}")

    # ข้ามการรันสคริปต์ในวันหยุดสุดสัปดาห์
    if date.today().weekday() >=5:
        # 1= Monday, 2=Tuesday, ..., 5=Saturday, 6=Sunday
        print(f"at {todayYYYYMMDD_hhmmss} => ❌ Today is weekend. Exiting...")
        return

    # อัพเดทรายชื่อหุ้น เดือนละครั้ง ทุกวันที่ X หรือวันแรกทำการของเดือน หรืออาจจะต้องปรับ ให้เช็ค แหล่งข้อมูลถูกอัพเดตหรือยัง?
    if date.today().day == 5: 
        usl.main()  # Update stock list from settrade
        usi.main()  # Update stock info from SiamChart
        ssc.main()  # Compute stock scores from SiamChart
        todayYYYYMMDD_hhmmss = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"at {todayYYYYMMDD_hhmmss} => ✅ Stock list, info, and scores updated.")

    # อัพเดทราคาหุ้นรายวัน
    uport.UpdatePortfolio()  # Update portfolio stock data
    uportinfo.main()  # Update portfolio account info

    # ถ้าเวลาเกิน 18:00 น. ให้ดึงราคาหุ้นสิ้นวัน ของวันนั้นเลย แต่ถ้ายังไม่ถึง 18:00 น. ให้ดึงราคาหุ้นของวันก่อนหน้า (กรณีวันนั้นยังไม่ปิดตลาด)
    endDateFix = None if datetime.now().time() > datetime.strptime("19:00:00", "%H:%M:%S").time() else (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    usp.main(endDateFix=endDateFix)  # Update stock prices

    # compute technical indicators and trading signals
    ################################ com_ind.main()  # Compute technical indicators
    com_ind_v5.main()  # Compute technical indicators ONLY v5 
    com_sig.main() # Compute trading signals
    
    print("🔄 Syncing EOD Trailing Stop & Sending Telegram Summary...")
    asyncio.run(sync_eod_portfolio())

    todayYYYYMMDD_hhmmss = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"at {todayYYYYMMDD_hhmmss} => ✅ Stock prices, indicators, and signals updated.")

    # อัพเดตพอร์ต => *** ยังไม่ทำ ***
    # คำนวนหุ้นที่จะซื้อขาย โดยพิจารณาจากข้อมูลพอร์ต,signal,score แล้วบันทึกลง table หุ้นที่จะซื้อ-ขาย ในวันถัดไป => *** ยังไม่ทำ ***
    # Period วันถัดไป ส่งคำสั่งซื้อขาย => *** ยังไม่ทำ ***
    print(f"Task Update finished at {todayYYYYMMDD_hhmmss}")
    print("=========================================")

    
if __name__ == "__main__":
    main()