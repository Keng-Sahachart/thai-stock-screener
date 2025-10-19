
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
import time

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# from lxml import html
# import requests

import pandas as pd

import initialApp as cfg
from dotenv import load_dotenv
from PyN_Library import fncPostgres as fPgSql
import psycopg2
from sqlalchemy import create_engine
load_dotenv()

from selenium.webdriver.common.action_chains import ActionChains

# ดึงข้อมูลจาก หน้าเว็บ http://siamchart.com/stock/
# แล้วอัพเดทลงตาราง stock_list_info_siamchart ใน postgresql
# ถ้ายังไม่มีตาราง ให้สร้างตารางนี้ก่อนรันสคริปต์นี้

def fetch_stock_info():
    # สร้าง instance ของ WebDriver (Chrome)
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--remote-allow-origins=*")
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 20)

    try:
        # เปิดหน้าเว็บที่ต้องการ
        driver.get("http://siamchart.com/stock/")
        wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
        # ดึงข้อมูลที่ต้องการจากหน้าเว็บ
        table_xpath = '//*[@id="table_data"]/table'
        # รอให้ตารางข้อมูลปรากฏ
        wait.until(EC.presence_of_element_located((By.XPATH, table_xpath)))
        wait.until(lambda d: len(d.find_elements(By.XPATH, table_xpath + "/tbody/tr")) > 0)
        table_html = driver.find_element(By.XPATH, table_xpath).get_attribute("outerHTML")
        df = pd.read_html(table_html)[0]
        
    finally:
        driver.quit()
        if df is None:
            return []
        return df

        
def main():
    # ดึงข้อมูลหุ้นจากเว็บ
    df = fetch_stock_info()
    df['import_datetime'] = datetime.now().strftime("%Y-%m-%d")
    print( df.head() )


    # เชื่อมต่อฐานข้อมูล PostgreSQL

    conn = psycopg2.connect(**cfg.postgresqldb_args)
    cur = conn.cursor()

    # # สร้างตารางถ้ายังไม่มี
    #<thead><tr class="head2"><td title="ชื่อย่อหุ้น" style="width: 62.8px;"><span style="cursor:pointer;" onclick="print_table(1);">Name</span></td><td style="width: 18.8px;">No.</td><td title="ลิงค์ข้อมูลสำคัญต่างๆ" style="width: 56.8px;"><span style="cursor:pointer;" onclick="print_table(3);">Links</span></td><td title="เครื่องหมายต่างๆ XD=Excluding Dividend / SP=Trading Suspension / NP=Notice Pending / NC=Non-Compliance" style="width: 54.8px;"><span style="cursor:pointer;" onclick="print_table(4);">Sign</span></td><td title="ราคาปิด" style="width: 28.8px;"><span style="cursor:pointer;" onclick="print_table(5);">Last</span></td><td title="อัตราเปลี่ยนแปลงของราคา" style="width: 35.8px;"><span style="cursor:pointer;" onclick="print_table(6);">Chg%</span></td><td title="ปริมาณการซื้อขายของวัน" style="width: 74.8px;"><span style="cursor:pointer;" onclick="print_table(7);">Volume</span></td><td title="มูลค่าการซื้อขายของวัน" style="width: 51.8px;"><span style="cursor:pointer;" onclick="print_table(8);">Value (k)</span></td><td title="มูลค่าหลักทรัพย์ตามราคาตลาด" style="width: 52.8px;"><span style="cursor:pointer;" onclick="print_table(9);">MCap (M)</span></td><td title="อัตราส่วนราคาต่อกำไร (Price/Earning per Share) [ยิ่งต่ำยิ่งดี]" style="width: 51.8px;"><span style="cursor:pointer;" onclick="print_table(10);">P/E</span></td><td title="อัตราส่วนราคาต่อมูลค่าทางบัญชี (Price/Book Value) [ยิ่งต่ำยิ่งดี]" style="width: 28.8px;"><span style="cursor:pointer;" onclick="print_table(11);">P/BV</span></td><td title="อัตราส่วนหนี้สินต่อส่วนของผู้ถือหุ้น (Debt/Equity) [ยิ่งต่ำยิ่งดี]" style="width: 32.8px;"><span style="cursor:pointer;" onclick="print_table(12);">D/E</span></td><td title="เงินปันผลต่อหุ้น (Dividend Per Share) [ยิ่งสูงยิ่งดี]" style="width: 28.8px;"><span style="cursor:pointer;" onclick="print_table(13);">DPS</span></td><td title="กำไรสุทธิต่อหุ้น (Earnings Per Share) [ยิ่งสูงยิ่งดี]" style="width: 32.8px;"><span style="cursor:pointer;" onclick="print_table(14);">EPS</span></td><td title="อัตราผลตอบแทนจากสินทรัพย์รวม (Return On Assets) [ยิ่งสูงยิ่งดี]" style="width: 35.8px;"><span style="cursor:pointer;" onclick="print_table(15);">ROA%</span></td><td title="อัตราผลตอบแทนผู้ถือหุ้น (Return on Equity) [ยิ่งสูงยิ่งดี]" style="width: 38.8px;"><span style="cursor:pointer;" onclick="print_table(16);">ROE%</span></td><td title="อัตรากำไรสุทธิ (Net Profit Margin) [ยิ่งสูงยิ่งดี]" style="width: 48.8px;"><span style="cursor:pointer;" onclick="print_table(17);">NPM%</span></td><td title="อัตราส่วนเงินปันผลตอบแทน (Dividend Yield) [ยิ่งสูงยิ่งดี]" style="width: 37.8px;"><span style="cursor:pointer;" onclick="print_table(18);">Yield%</span></td><td title="อัตตราส่วนจำนวนหุ้นที่ซื้อขายในตลาด (Free Float %)" style="width: 44.8px;"><span style="cursor:pointer;" onclick="print_table(19);">FFloat%</span></td><td title="อัตตราส่วนจำนวนหลักทรัพย์ ที่วางเป็นประกัน ต่อจำหน่ายได้แล้ว (Margin %)" style="width: 28.8px;"><span style="cursor:pointer;" onclick="print_table(20);">MG%</span></td><td title="Magic Formula Rank Score P/E+ROE [ยิ่งต่ำยิ่งดี]" style="width: 38.8px;"><span style="cursor:pointer;" onclick="print_table(21);">Magic1</span></td><td title="Magic Formula Rank Score P/E+ROA [ยิ่งต่ำยิ่งดี]" style="width: 38.8px;"><span style="cursor:pointer;" onclick="print_table(22);">Magic2</span></td><td title="อัตราส่วนราคาต่อกำไร ต่อการเติบโต ((PE / Growth) โดยคำนวน Growth มาจากค่าเฉลี่ยย้อนหลังของ Net Profit 5 ปี [ควรมีค่าไม่เกิน 1 แต่ไม่ควรมีค่าเป็นลบ]" style="width: 38.8px;"><span style="cursor:pointer;" onclick="print_table(23);">PEG</span></td><td title="คะแนนรายงานการกำกับดูแลกิจการบริษัทจดทะเบียน (Corporate Governance Score) 5=Excellent / 4=Very Good / 3 = Good [ยิ่งสูงยิ่งดี]" style="width: 26.8px;"><span style="cursor:pointer;" onclick="print_table(24);">CG</span> <img src="/css/sort_down.gif"></td></tr></thead>
    sqlCrtTb = fPgSql.generate_create_table_script(df, 'stock_list_info_siamchart',False)
    print(sqlCrtTb)
    cur.execute(sqlCrtTb)
    conn.commit()

    sqlDel = "DELETE FROM stock_list_info_siamchart"
    with conn.cursor() as cursor:
        cursor.execute(sqlDel)
        conn.commit()

    fPgSql.bulk_copy_dataframe_to_postgres(df=df, table_name='stock_list_info_siamchart', db_args=cfg.postgresqldb_args, use_index=False)
    print("Data inserted into settrade_stocklist table")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()