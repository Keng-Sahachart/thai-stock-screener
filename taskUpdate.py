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
import time
from core.job_notifier import notify_job_start_sync, notify_job_finish_sync

def main(force: bool = False):
    print("=========================================")
    todayYYYYMMDD_hhmmss = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Task Update started at {todayYYYYMMDD_hhmmss} (force={force})")

    # ข้ามการรันสคริปต์ในวันหยุดสุดสัปดาห์
    if not force and date.today().weekday() > 4:
        # 0 = Monday, 1=Tuesday, ...4=Friday, 5=Saturday, 6=Sunday
        print(f"at {todayYYYYMMDD_hhmmss} => ❌ Today is weekend. Exiting...")
        notify_job_finish_sync("Task Update", summary="ข้ามการทำงาน (วันหยุดสุดสัปดาห์)")
        return {"status": "skipped", "reason": "วันหยุดสุดสัปดาห์ (Weekend)"}
    
    #เช็ควันหยุดนักขัตฤกษ์(เช็คตอนเช้า) (ถ้าอยากให้รันก็สามารถคอมเมนต์บรรทัดนี้ออกได้)
    if not force and date.today().weekday() <= 4 and not usp.is_market_open() and  datetime.strptime("09:30:00", "%H:%M:%S").time() <= datetime.now().time() <= datetime.strptime("12:30:00", "%H:%M:%S").time() and  datetime.strptime("14:30:00", "%H:%M:%S").time() <= datetime.now().time() <= datetime.strptime("17:00:00", "%H:%M:%S").time():
        print(f"at {todayYYYYMMDD_hhmmss} => ❌ Today is not a market day. Exiting...")
        notify_job_finish_sync("Task Update", summary="ข้ามการทำงาน (ตลาดปิดทำการ)")
        return {"status": "skipped", "reason": "ตลาดปิดทำการ (Holiday/Closed)"}

    start_t = time.time()
    notify_job_start_sync("Task Update", "เริ่มอัพเดตข้อมูลราคา, Indicators และประมวลผลสิ้นวัน")

    try:
        # อัพเดทรายชื่อหุ้น เดือนละครั้ง ทุกวันที่ 1 และ 15
        if date.today().day in [1, 15]:
            try:
                print("🔄 [MONTHLY UPDATE] Updating stock list & SiamChart scores (Day 1/15)...")
                usl.main()  # Update stock list from settrade
                usi.main()  # Update stock info from SiamChart
                ssc.main()  # Compute stock scores from SiamChart
                todayYYYYMMDD_hhmmss = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"at {todayYYYYMMDD_hhmmss} => ✅ Stock list, info, and scores updated.")
            except Exception as m_err:
                todayYYYYMMDD_hhmmss = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"at {todayYYYYMMDD_hhmmss} => ⚠️ [MONTHLY UPDATE WARNING] {m_err}")
                print("⚠️ ข้ามไปยังขั้นตอนอัพเดตราคาประจำวันและคำนวณ Signals ต่อเนื่อง...")

        # อัพเดทราคาหุ้นรายวัน
        uport.UpdatePortfolio()  # Update portfolio stock data
        uportinfo.main()  # Update portfolio account info

        # ถ้าเวลาเกิน 18:00 น. ให้ดึงราคาหุ้นสิ้นวัน ของวันนั้นเลย แต่ถ้ายังไม่ถึง 17:00 น. ให้ดึงราคาหุ้นของวันก่อนหน้า (กรณีวันนั้นยังไม่ปิดตลาด)
        endDateFix = None if datetime.now().time() > datetime.strptime("17:00:00", "%H:%M:%S").time() else (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        usp.main(endDateFix=endDateFix)  # Update stock prices

        # compute technical indicators and trading signals
        ################################ com_ind.main()  # Compute technical indicators
        com_ind_v5.main()  # Compute technical indicators ONLY v5 
        com_sig.main() # Compute trading signals
        
        print("🔄 Syncing EOD Trailing Stop & Sending Telegram Summary...")
        asyncio.run(sync_eod_portfolio())

        todayYYYYMMDD_hhmmss = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"at {todayYYYYMMDD_hhmmss} => ✅ Stock prices, indicators, and signals updated.")

        print(f"Task Update finished at {todayYYYYMMDD_hhmmss}")
        print("=========================================")
        elapsed = time.time() - start_t
        notify_job_finish_sync("Task Update", elapsed_seconds=elapsed, summary="อัพเดตราคาหุ้น, Indicators, Signals และซิงค์พอร์ตเรียบร้อย")
        return {"status": "ok", "elapsed_seconds": elapsed}
    except Exception as e:
        elapsed = time.time() - start_t
        notify_job_finish_sync("Task Update", elapsed_seconds=elapsed, success=False, error=str(e))
        raise e

    
if __name__ == "__main__":
    main()