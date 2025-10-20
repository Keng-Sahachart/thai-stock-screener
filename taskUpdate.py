from datetime import date, datetime
import updateStockList as usl
import updateStockInfo_siamChart as usi
import stockScore_siamChart as ssc

import updateStockPrice as usp
import compute_indicators_v3 as com_ind
import compute_signals as com_sig
import updatePort as uport

def main():
    print("=========================================")
    todayYYYYMMDD_hhmmss = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Task Update started at {todayYYYYMMDD_hhmmss}")

    # ข้ามการรันสคริปต์ในวันหยุดสุดสัปดาห์
    if date.today().weekday() >=5:
        # 1= Monday, 2=Tuesday, ..., 5=Saturday, 6=Sunday
        print(f"at {todayYYYYMMDD_hhmmss} => ❌ Today is weekend. Exiting...")
        return

    # อัพเดทรายชื่อหุ้น เดือนละครั้ง ทุกวันที่ 5 หรือวันแรกทำการของเดือน หรืออาจจะต้องปรับ ให้เช็ค แหล่งข้อมูลถูกอัพเดตหรือยัง?
    if date.today().day == 5:
        usl.main()  # Update stock list from settrade
        usi.main()  # Update stock info from SiamChart
        ssc.main()  # Compute stock scores from SiamChart
        todayYYYYMMDD_hhmmss = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"at {todayYYYYMMDD_hhmmss} => ✅ Stock list, info, and scores updated.")


    # อัพเดทราคาหุ้นรายวัน
    usp.main()  # Update stock prices
    com_ind.main()  # Compute technical indicators
    com_sig.main() # Compute trading signals
    uport.main()  # Update portfolio stock data

    todayYYYYMMDD_hhmmss = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"at {todayYYYYMMDD_hhmmss} => ✅ Stock prices, indicators, and signals updated.")

    # อัพเดตพอร์ต => *** ยังไม่ทำ ***
    # คำนวนหุ้นที่จะซื้อขาย โดยพิจารณาจากข้อมูลพอร์ต,signal,score แล้วบันทึกลง table หุ้นที่จะซื้อ-ขาย ในวันถัดไป => *** ยังไม่ทำ ***
    # Period วันถัดไป ส่งคำสั่งซื้อขาย => *** ยังไม่ทำ ***
    print(f"Task Update finished at {todayYYYYMMDD_hhmmss}")
    print("=========================================")



    
if __name__ == "__main__":
    main()