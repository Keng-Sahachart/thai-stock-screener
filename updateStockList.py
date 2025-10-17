
from selenium import webdriver
from selenium.webdriver.common.by import By
import time

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from lxml import html
# import requests

import pandas as pd

import initialApp as cfg
from dotenv import load_dotenv
from PyN_Library import fncPostgres as fPgSql
import psycopg2
from sqlalchemy import create_engine
load_dotenv()

from selenium.webdriver.common.action_chains import ActionChains


def fetch_symbolList_settrade_get_quote_v2(headless: bool = True, timeout: int = 20) -> pd.DataFrame:
    """
    เปิดหน้า https://www.settrade.com/th/get-quote แบบ headless,
    คลิก dropdown (ตาม XPath ที่ผู้ใช้ให้มา), เลือก option ลำดับที่ 5,
    แล้วอ่านตารางผลลัพธ์เป็น DataFrame

    return: pandas.DataFrame
    """
    # --- 1) สร้าง Chrome แบบเสถียรใน headless ---
    options = webdriver.ChromeOptions()
    # if headless:
        # options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--remote-allow-origins=*")

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, timeout)

    try:
        # --- 2) เปิดหน้า ---
        driver.get("https://www.settrade.com/th/get-quote")
        # รอ DOM พร้อม
        wait.until(lambda d: d.execute_script("return document.readyState") == "complete")

        # --- 3) ปิดแบนเนอร์คุกกี้/โอเวอร์เลย์ถ้ามี (เงียบ ๆ ถ้าไม่เจอ) ---
        # for locator in [
        #     (By.ID, "onetrust-accept-btn-handler"),
        #     (By.CSS_SELECTOR, "button[aria-label='Accept all']"),
        #     (By.CSS_SELECTOR, "button.cookie-accept"),
        # ]:
        #     try:
        #         btn = wait.until(EC.element_to_be_clickable(locator))
        #         btn.click()
        #         break
        #     except Exception:
        #         pass

        # --- 4) หา dropdown แล้วเลื่อนเข้ากลางจอ จากนั้นคลิก ---
        dropdown_xpath = '/html/body/div[1]/div/div/div[2]/div/div[2]/div[2]/div[2]/div[1]/div/div[3]/div[1]/div/div[2]/div/div[2]/span'
        dropdown = wait.until(EC.presence_of_element_located((By.XPATH, dropdown_xpath)))
        driver.execute_script("arguments[0].scrollIntoView({block:'center', inline:'nearest'});", dropdown)
        # กัน header บัง
        driver.execute_script("window.scrollBy(0, -80);")

        # รอให้คลิกได้
        wait.until(EC.element_to_be_clickable((By.XPATH, dropdown_xpath)))
        try:
            ActionChains(driver).move_to_element(dropdown).pause(0.05).click().perform() # ใช้ ActionChains เผื่อ element บัง
        except Exception:
            driver.execute_script("arguments[0].click();", dropdown) # สำรองด้วย JavaScript

        # --- 5) เลือก option ในเมนู (ลำดับที่ 5 ตามโค้ดเดิม) ---
        # หากคุณรู้ข้อความ เช่น 'หุ้นทั้งหมด' แนะนำเปลี่ยนเป็น:
        # option = wait.until(EC.element_to_be_clickable((By.XPATH, "//ul/li/span[normalize-space()='หุ้นทั้งหมด']")))
        option_xpath = '/html/body/div[1]/div/div/div[2]/div/div[2]/div[2]/div[2]/div[1]/div/div[3]/div[1]/div/div[2]/div/div[3]/ul/li[5]/span'
        option_el = wait.until(EC.element_to_be_clickable((By.XPATH, option_xpath)))
        try:
            ActionChains(driver).move_to_element(option_el).pause(0.05).click().perform()
        except Exception:
            driver.execute_script("arguments[0].click();", option_el)

        # --- 6) รอตารางปรากฏ/โหลดข้อมูลเสร็จ แล้วดึง HTML ---
        table_xpath = "/html/body/div[1]/div/div/div[2]/div/div[2]/div[2]/div[2]/div[1]/div/div[1]/div[2]/table"
        # รอให้มี TR อย่างน้อย 1 แถว (บางเว็บใส่ตารางก่อน แล้วค่อยเติมแถว)
        wait.until(EC.presence_of_element_located((By.XPATH, table_xpath)))
        wait.until(lambda d: len(d.find_elements(By.XPATH, table_xpath + "/tbody/tr")) > 0)

        table_html = driver.find_element(By.XPATH, table_xpath).get_attribute("outerHTML")
        df = pd.read_html(table_html)[0]

        return df

    finally:
        driver.quit()

def fetch_symbolList_settrade_get_quote_v1_2():
    # สร้าง instance ของ WebDriver (Chrome)
    timeout = 20
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--remote-allow-origins=*")
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, timeout)
    # เปิดหน้าเว็บที่ต้องการ
    driver.get("https://www.settrade.com/th/get-quote")  # เปลี่ยน URL เป็นหน้าเว็บที่คุณต้องการ

    # รอให้โหลดหน้าเว็บ
    wait.until(lambda d: d.execute_script("return document.readyState") == "complete")

    # ค้นหา span element โดยใช้ XPath หรือ CSS Selector
    # คลิกที่ span ที่เปิด dropdown
    dropdown_xpath = '/html/body/div[1]/div/div/div[2]/div/div[2]/div[2]/div[2]/div[1]/div/div[3]/div[1]/div/div[2]/div/div[2]/span'
    dropdown_span = wait.until(EC.presence_of_element_located((By.XPATH, dropdown_xpath)))
    # เลื่อนหน้าจอลงไปยังองค์ประกอบที่ค้นพบ
    driver.execute_script("arguments[0].scrollIntoView();", dropdown_span)
    time.sleep(0.6)  # รอให้เลื่อนหน้าจอเสร็จ
    # กัน header บัง
    driver.execute_script("window.scrollBy(0, -80);")
    time.sleep(0.5)  # รอให้เลื่อนหน้าจอเสร็จ
    wait.until(EC.element_to_be_clickable((By.XPATH, dropdown_xpath)))
    # driver.execute_script("arguments[0].click();", dropdown_span) # สำรองด้วย JavaScript
    dropdown_span.click()

    # รอให้ dropdown โหลดตัวเลือก
    time.sleep(0.5)

    # ค้นหาตัวเลือกใน dropdown และคลิกที่มัน
    # option = driver.find_element(By.XPATH, '/html/body/div[1]/div/div/div[2]/div/div[2]/div[2]/div[2]/div[1]/div/div[3]/div[1]/div/div[2]/div/div[3]/ul/li[5]/span')  # เปลี่ยนข้อความให้ตรงกับตัวเลือกที่คุณต้องการ
    option_xpath = '/html/body/div[1]/div/div/div[2]/div/div[2]/div[2]/div[2]/div[1]/div/div[3]/div[1]/div/div[2]/div/div[3]/ul/li[5]/span'
    option = wait.until(EC.element_to_be_clickable((By.XPATH, option_xpath)))
    option.click()
    # driver.execute_script("arguments[0].click();", option) 

    # รอให้เห็นผลลัพธ์ก่อนปิด
    # time.sleep(1)

    # ค้นหาแท็ก <tr> ทั้งหมดตาม XPath ที่กำหนด
    # rows = driver.find_elements(By.XPATH, "/html/body/div[1]/div/div/div[2]/div/div[2]/div[2]/div[2]/div[1]/div/div[1]/div[2]/table/tbody/tr")
    # # หาจำนวนแท็ก <tr>
    # row_count = len(rows)
    # print(f"จำนวนแท็ก <tr> ทั้งหมด: {row_count}")

    table_xpath = "/html/body/div[1]/div/div/div[2]/div/div[2]/div[2]/div[2]/div[1]/div/div[1]/div[2]/table"
    # ดึง HTML ของตาราง
    # table_html = driver.find_element(By.XPATH, table_xpath).get_attribute('outerHTML')
    wait.until(EC.presence_of_element_located((By.XPATH, table_xpath)))
    wait.until(lambda d: len(d.find_elements(By.XPATH, table_xpath + "/tbody/tr")) > 0)
    # ใช้ Pandas เพื่ออ่าน HTML table และแปลงเป็น DataFrame
    # df = pd.read_html(table_html)[0]  # ดึง DataFrame แรกจากรายการ DataFrames
    table_html = driver.find_element(By.XPATH, table_xpath).get_attribute("outerHTML")
    df = pd.read_html(table_html)[0]
    print(df)

    driver.quit()
    return df


def fetch_symbolList_settrade_get_quote():
    # สร้าง instance ของ WebDriver (Chrome)

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)
    
    # เปิดหน้าเว็บที่ต้องการ
    driver.get("https://www.settrade.com/th/get-quote")  # เปลี่ยน URL เป็นหน้าเว็บที่คุณต้องการ

    # รอให้โหลดหน้าเว็บ
    time.sleep(1)

    # ค้นหา span element โดยใช้ XPath หรือ CSS Selector
    # คลิกที่ span ที่เปิด dropdown
    dropdown_span = driver.find_element(By.XPATH, '/html/body/div[1]/div/div/div[2]/div/div[2]/div[2]/div[2]/div[1]/div/div[3]/div[1]/div/div[2]/div/div[2]/span')  # เปลี่ยนเป็น id หรือ selector ที่ถูกต้อง
    # เลื่อนหน้าจอลงไปยังองค์ประกอบที่ค้นพบ
    driver.execute_script("arguments[0].scrollIntoView();", dropdown_span)
    time.sleep(0.5)  # รอให้เลื่อนหน้าจอเสร็จ
    dropdown_span.click()

    # รอให้ dropdown โหลดตัวเลือก
    time.sleep(0.5)

    # ค้นหาตัวเลือกใน dropdown และคลิกที่มัน
    option = driver.find_element(By.XPATH, '/html/body/div[1]/div/div/div[2]/div/div[2]/div[2]/div[2]/div[1]/div/div[3]/div[1]/div/div[2]/div/div[3]/ul/li[5]/span')  # เปลี่ยนข้อความให้ตรงกับตัวเลือกที่คุณต้องการ
    option.click()

    # รอให้เห็นผลลัพธ์ก่อนปิด
    time.sleep(1)

    # ค้นหาแท็ก <tr> ทั้งหมดตาม XPath ที่กำหนด
    rows = driver.find_elements(By.XPATH, "/html/body/div[1]/div/div/div[2]/div/div[2]/div[2]/div[2]/div[1]/div/div[1]/div[2]/table/tbody/tr")
    # หาจำนวนแท็ก <tr>
    row_count = len(rows)
    print(f"จำนวนแท็ก <tr> ทั้งหมด: {row_count}")
    # ดึง HTML ของตาราง
    table_html = driver.find_element(By.XPATH, "/html/body/div[1]/div/div/div[2]/div/div[2]/div[2]/div[2]/div[1]/div/div[1]/div[2]/table").get_attribute('outerHTML')
    # ใช้ Pandas เพื่ออ่าน HTML table และแปลงเป็น DataFrame
    df = pd.read_html(table_html)[0]  # ดึง DataFrame แรกจากรายการ DataFrames
    print(df)

    driver.quit()
    return df


def main():

    df = fetch_symbolList_settrade_get_quote_v1_2()
    df = df.rename(columns={df.columns[0]: 'symbol'}) 
    df = df.rename(columns={df.columns[1]: 'name_th'})
    df = df.rename(columns={df.columns[2]: 'name_en'})  
    df = df.rename(columns={df.columns[3]: 'market'})


    sqlCrtTb = fPgSql.generate_create_table_script(df, 'settrade_stocklist',False)
    print(sqlCrtTb)

    pg_conn = psycopg2.connect(**cfg.postgresqldb_args)

    with pg_conn.cursor() as cursor:
        cursor.execute(sqlCrtTb)
        pg_conn.commit()

    sqlDel = "DELETE FROM settrade_stocklist"
    with pg_conn.cursor() as cursor:
        cursor.execute(sqlDel)
        pg_conn.commit()

    fPgSql.bulk_copy_dataframe_to_postgres(df=df, table_name='settrade_stocklist', db_args=cfg.postgresqldb_args, use_index=False)
    print("Data inserted into settrade_stocklist table")


if __name__ == "__main__":
    main()
