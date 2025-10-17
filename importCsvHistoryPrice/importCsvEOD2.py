# pip install tqdm psycopg2-binary
import pandas as pd
import numpy as np
from tqdm import tqdm
import psycopg2
from psycopg2.extras import execute_values

import os
from dotenv import load_dotenv
load_dotenv()

# ----- เตรียม DataFrame (ตัวอย่าง) -----
df = pd.read_csv('importCsvHistoryPrice\\PriceHistoryEOD20241220.csv',
    dtype={
        "TICKER": str,
        "DTYYYYMMDD": str,
        "OPEN": float,
        "HIGH": float,
        "LOW": float,
        "CLOSE": float,
        # "VOL": int
    }, na_values=['', 'NA']  # กำหนดค่า NA ให้เป็น NaN
    )

df["date"] = pd.to_datetime(df["DTYYYYMMDD"], format="%Y%m%d").dt.date
# ทำความสะอาด dtype ให้ insert ได้เร็ว
for c in ["OPEN","HIGH","LOW","CLOSE","VOL"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")

# ----- SQL สำหรับ upsert -----
SQL = """
INSERT INTO stock_price_history (symbol, date, open, high, low, close, volume)
VALUES %s
ON CONFLICT (symbol, date) DO UPDATE SET
  open   = EXCLUDED.open,
  high   = EXCLUDED.high,
  low    = EXCLUDED.low,
  close  = EXCLUDED.close,
  volume = EXCLUDED.volume;
"""

# ----- generator: ไม่สร้าง list ใหญ่ใน RAM -----
def gen_rows(frame: pd.DataFrame):
    it = frame.itertuples(index=False)
    for r in it:
        # r = (TICKER, DTYYYYMMDD, OPEN, HIGH, LOW, CLOSE, VOL, date)
        yield (
            r.TICKER.strip() if isinstance(r.TICKER, str) else str(r.TICKER),
            r.date,  # datetime.date
            None if pd.isna(r.OPEN)  else float(r.OPEN),
            None if pd.isna(r.HIGH)  else float(r.HIGH),
            None if pd.isna(r.LOW)   else float(r.LOW),
            None if pd.isna(r.CLOSE) else float(r.CLOSE),
            0    if pd.isna(r.VOL)   else int(r.VOL),
        )

# ----- เชื่อมต่อ DB -----
conn_str = (
    f"host={os.getenv('posql_host')} "
    f"port={os.getenv('posql_port')} "
    f"dbname={os.getenv('posql_db')} "
    f"user={os.getenv('posql_user')} "
    f"password={os.getenv('posql_password')}"
)
conn = psycopg2.connect(conn_str)
cur = conn.cursor()

# ----- แบ่งล็อต + แสดง progress -----
batch_size = 10000
batch = []
total = len(df)

try:
    for i, row in enumerate(tqdm(gen_rows(df), total=total, desc="Upserting"), 1):
        batch.append(row)
        if len(batch) >= batch_size:
            try:
                execute_values(cur, SQL, batch, page_size=batch_size)
                conn.commit()          # ✅ คอมมิตทันทีที่สำเร็จ
                batch.clear()          # ✅ เคลียร์แบทช์
            except psycopg2.Error as e:
                print(f"❌ Error at batch ending row #{i:,}: {e}")
                conn.rollback()        # ✅ ย้อนเฉพาะก้อนที่พัง
                # (ทางเลือก) ลด batch ลงทีละครึ่งเพื่อตามหาบรรทัดปัญหา
                batch.clear()          # ข้ามก้อนนี้ไปก่อน

    # ก้อนสุดท้าย (ถ้ามี)
    if batch:
        try:
            execute_values(cur, SQL, batch, page_size=batch_size)
            conn.commit()              # ✅ คอมมิตก้อนสุดท้าย
        except psycopg2.Error as e:
            print(f"❌ Error at final batch: {e}")
            conn.rollback()
finally:
    cur.close()
    conn.close()
