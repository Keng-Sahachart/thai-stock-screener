'''
อัพเดตข้อมูลพอร์ต ยอดเงินคงเหลือ, วงเงินซื้อขาย, วงเงินเครดิต, และข้อมูลอื่น ๆ ของบัญชีเทรดจาก Settrade API
ตัวอย่างข้อมูลที่ได้จาก API:
{'lineAvailable': 324.99, 'creditLimit': 600000.0, 'cashBalance': 313.01, 'accountType': 'CASH_BALANCE_FOR_TURNOVERLIST', 'clientType': 'INDIVIDUAL', 'customerType': 'CUSTOMER', 'canBuy': True, 'canSell': True, 'crossingKey': '960018178', 'initialCreditLimit': 600000.0, 'initialCashBalance': 313.01, 'initialLineAvailable': 324.99, 'netSettlementLine': 0.0, 'cashType': 'CASH_DEPOSIT', 'collateral': 0.0, 'creditBalance': False}
'''
import os
import re
from datetime import datetime
import sys
import psycopg2
from psycopg2 import sql
from settrade_v2 import Investor
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
import initialApp as cfg
# 1. การตั้งค่าการเชื่อมต่อฐานข้อมูล
PG_CONN_STR = (
    f"host={os.getenv('posql_host', 'localhost')} "
    f"port={os.getenv('posql_port', '5432')} "
    f"dbname={os.getenv('posql_db', 'stocks')} "
    f"user={os.getenv('posql_user', 'postgres')} "
    f"password={os.getenv('posql_password', 'postgres')}"
)

ACCOUNT_NO = account_no=os.getenv("account_no")


def camel_to_snake(name: str) -> str:
    """แปลง key แบบ camelCase ให้เป็น snake_case สำหรับชื่อคอลัมน์ใน Postgres"""
    s = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
    return s.replace("__", "_")

def ensure_table_columns(cur, table_name: str, data: dict):
    """ตรวจสอบและเพิ่มคอลัมน์ใหม่เข้า Table อัตโนมัติ หาก API ส่ง field ที่ยังไม่มีมา"""
    cur.execute(
        """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = %s;
    """,
        (table_name,),
    )
    existing_cols = {row[0] for row in cur.fetchall()}

    for col, val in data.items():
        if col not in existing_cols:
            # อนุมาน Data Type เบื้องต้น
            if isinstance(val, bool):
                col_type = "BOOLEAN"
            elif isinstance(val, (int, float)):
                col_type = "NUMERIC(18, 4)"
            elif isinstance(val, datetime):
                col_type = "TIMESTAMP WITH TIME ZONE"
            else:
                col_type = "TEXT"

            cur.execute(
                sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}").format(
                    sql.Identifier(table_name),
                    sql.Identifier(col),
                    sql.SQL(col_type),
                )
            )
            existing_cols.add(col)

def init_db(conn):
    """สร้างตารางเฉพาะฟิลด์หลักที่จำเป็น ส่วนฟิลด์ข้อมูลจะให้ ensure_table_columns สร้างตามจริง"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS account_info_history (
        id SERIAL PRIMARY KEY,
        account_no VARCHAR(50) NOT NULL,
        is_disabled BOOLEAN NOT NULL DEFAULT FALSE,
        import_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_account_active ON account_info_history(account_no, is_disabled);
    """
    with conn.cursor() as cur:
        cur.execute(create_table_query)
    conn.commit()


def save_account_info(account_no: str, raw_info: dict):
    data = {camel_to_snake(k): v for k, v in raw_info.items()}
    data["account_no"] = account_no
    data["is_disabled"] = False
    data["import_date"] = datetime.now()

    with psycopg2.connect(PG_CONN_STR) as conn:
        with conn.cursor() as cur:
            # 1. ตรวจสอบและเพิ่มคอลัมน์ใหม่อัตโนมัติ (เช่น initial_credit_limit)
            ensure_table_columns(cur, "account_info_history", data)

            # 2. ปิดการใช้งานข้อมูลเดิม (Soft-delete / Mark disabled)
            cur.execute(
                """
                UPDATE account_info_history
                SET is_disabled = TRUE
                WHERE account_no = %s AND is_disabled = FALSE;
                """,
                (account_no,),
            )

            # 3. Insert ข้อมูลชุดล่าสุด
            columns = list(data.keys())
            values = [data[col] for col in columns]

            insert_query = sql.SQL(
                "INSERT INTO account_info_history ({fields}) VALUES ({placeholders})"
            ).format(
                fields=sql.SQL(", ").join([sql.Identifier(col) for col in columns]),
                placeholders=sql.SQL(", ").join([sql.Placeholder()] * len(columns)),
            )

            cur.execute(insert_query, values)

        conn.commit()
        print(f"บันทึกข้อมูลพอร์ต {account_no} เรียบร้อยแล้ว")

def main():
    # เชื่อมต่อ Settrade API
    investor = Investor(**cfg.args_Investor    )

    equity = investor.Equity(account_no=ACCOUNT_NO)
    account_info = equity.get_account_info()

    # ตรวจสอบและสร้างตาราง (รันครั้งแรก)
    with psycopg2.connect(PG_CONN_STR) as db_conn:
        init_db(db_conn)

    # บันทึกลงฐานข้อมูล
    save_account_info(ACCOUNT_NO, account_info)


if __name__ == "__main__":
    main()