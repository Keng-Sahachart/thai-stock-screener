from datetime import date
import updateStockList as usl
import updateStockInfo_siamChart as usi
import stockScore_siamChart as ssc

import updateStockPrice as usp
import compute_indicators_v3 as com_ind
import compute_signals as com_sig


def main():

    # ข้ามการรันสคริปต์ในวันหยุดสุดสัปดาห์
    if date.today().weekday() >=5:
        # 1= Monday, 2=Tuesday, ..., 5=Saturday, 6=Sunday
        print("❌ Today is weekend. Exiting...")
        return

    # อัพเดทรายชื่อหุ้น เดือนละครั้ง ทุกวันที่ 1 หรือวันแรกทำการของเดือน หรืออาจจะต้องปรับ ให้เช็ค แหล่งข้อมูลถูกอัพเดตหรือยัง?
    if date.today().day == 1:
        usl.main()  # Update stock list from settrade
        usi.main()  # Update stock info from SiamChart
        ssc.main()  # Compute stock scores from SiamChart


    # อัพเดทราคาหุ้นรายวัน
    usp.main()  # Update stock prices
    com_ind.main()  # Compute technical indicators
    com_sig.main() # Compute trading signals


    # อัพเดตพอร์ต => *** ยังไม่ทำ ***
    # คำนวนหุ้นที่จะซื้อขาย โดยพิจารณาจากข้อมูลพอร์ต,signal,score แล้วบันทึกลง table หุ้นที่จะซื้อ-ขาย ในวันถัดไป => *** ยังไม่ทำ ***
    # Period วันถัดไป ส่งคำสั่งซื้อขาย => *** ยังไม่ทำ ***


if __name__ == "__main__":
    main()