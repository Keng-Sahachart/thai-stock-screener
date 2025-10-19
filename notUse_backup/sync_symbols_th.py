# file: sync_symbols_th.py
import os, time, json, math, requests, pandas as pd, psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# ----- Config -----
load_dotenv()
PGCFG = dict(
    host=os.getenv("posql_host"),
    port=int(os.getenv("posql_port")),
    dbname=os.getenv("posql_db"),
    user=os.getenv("posql_user"),
    password=os.getenv("posql_password"),
)
TWELVE_APIKEY = os.getenv("TWELVEDATA_API_KEY")
EXCHANGE_CODE = "XBKK"  # ตลาดหุ้นไทยในผู้ให้บริการสากล

# settrade_v2 (อาจไม่จำเป็นต้อง import ถ้าจะรันเฉพาะดึง list)
try:
    from settrade_v2 import Investor
    HAS_SETTRADE = True
except Exception:
    HAS_SETTRADE = False

def pg_conn():
    return psycopg2.connect(**PGCFG)

DDL = """
CREATE TABLE IF NOT EXISTS dim_symbol_th (
  symbol        text        NOT NULL,
  exchange      text        NOT NULL,
  full_code     text,
  name_en       text,
  currency      text,
  asset_type    text,
  market        text,
  board         text,
  sector        text,
  industry      text,
  lot_size      integer,
  tick_size     numeric(18,6),
  is_tradable   boolean      DEFAULT NULL,
  provider_raw  jsonb,
  updated_at    timestamptz  DEFAULT now(),
  PRIMARY KEY (symbol, exchange)
);
CREATE INDEX IF NOT EXISTS ix_dim_symbol_th_market ON dim_symbol_th(market);
"""

UPSERT_SQL = """
INSERT INTO dim_symbol_th
(symbol, exchange, full_code, name_en, currency, asset_type, market, board,
 sector, industry, lot_size, tick_size, is_tradable, provider_raw)
VALUES %s
ON CONFLICT (symbol, exchange) DO UPDATE SET
  full_code   = EXCLUDED.full_code,
  name_en     = EXCLUDED.name_en,
  currency    = EXCLUDED.currency,
  asset_type  = EXCLUDED.asset_type,
  market      = EXCLUDED.market,
  board       = EXCLUDED.board,
  sector      = COALESCE(EXCLUDED.sector, dim_symbol_th.sector),
  industry    = COALESCE(EXCLUDED.industry, dim_symbol_th.industry),
  lot_size    = COALESCE(EXCLUDED.lot_size, dim_symbol_th.lot_size),
  tick_size   = COALESCE(EXCLUDED.tick_size, dim_symbol_th.tick_size),
  is_tradable = COALESCE(EXCLUDED.is_tradable, dim_symbol_th.is_tradable),
  provider_raw= EXCLUDED.provider_raw,
  updated_at  = now();
"""


def ensure_table():
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(DDL)
        conn.commit()

def fetch_symbols_twelvedata(exchange=EXCHANGE_CODE):
    base = f"https://api.twelvedata.com/stocks?exchange={exchange}"
    if TWELVE_APIKEY:
        base += f"&apikey={TWELVE_APIKEY}"
    r = requests.get(base, timeout=30)
    r.raise_for_status()
    data = r.json()
    if "data" not in data:
        raise RuntimeError(f"TwelveData unexpected response: {data}")
    df = pd.DataFrame(data["data"])
    # คอลัมน์ที่คาดหวัง: symbol, name, currency, exchange, type, mic_code
    # full_code สำหรับ yfinance: .BK
    if "symbol" not in df.columns:
        return pd.DataFrame(columns=["symbol","name","currency","exchange","type","full_code"])
    df["full_code"] = df["symbol"].astype(str) + ".BK"
    # Normalize ชื่อคอลัมน์
    df = df.rename(columns={"name":"name_en","type":"asset_type"})
    use_cols = ["symbol","exchange","full_code","name_en","currency","asset_type"]
    for c in use_cols:
        if c not in df.columns: df[c] = None
    return df[use_cols]

def init_settrade():
    if not HAS_SETTRADE:
        return None, None
    app_id     = os.getenv("app_id")
    app_secret = os.getenv("app_secret")
    broker_id  = "023"
    app_code   = "ALGO_EQ"
    account_no = os.getenv("account_no")
    inv = Investor(app_id=app_id, app_secret=app_secret, broker_id=broker_id,
                   app_code=app_code, is_auto_queue=True)
    eq = inv.Equity(account_no=account_no) if account_no else inv.Equity()
    return inv, eq

def enrich_with_settrade(eq, symbol):
    """
    พยายามดึงรายละเอียดจาก settrade_v2:
      - market (SET/mai)
      - board (B/F/R ...)
      - lot_size, tick_size
      - sector/industry (ถ้ามีในผลลัพธ์; บาง broker อาจไม่ส่ง)
    ถ้าเรียกไม่ได้ ให้คืนค่า is_tradable=None และฟิลด์อื่นๆเป็น None
    """
    info = None
    try:
        # ลอง get_symbol_info ก่อน
        info = eq.get_symbol_info(symbol)
        # บางระบบจะตอบเป็น dict หรือ obj; normalize ให้เป็น dict
        if hasattr(info, "__dict__"):
            info = info.__dict__
    except Exception:
        # สำรองด้วย search_symbol
        try:
            lst = eq.search_symbol(symbol)
            if lst and isinstance(lst, list):
                info = lst[0]
        except Exception:
            info = None

    out = dict(market=None, board=None, lot_size=None, tick_size=None,
               sector=None, industry=None, is_tradable=None, raw=None)
    if not info:
        return out

    # สกัดค่าแบบปลอดภัย (คีย์ขึ้นกับโบรก/เวอร์ชัน)
    out["market"]     = info.get("market") or info.get("marketId") or info.get("marketCode")
    out["board"]      = info.get("board") or info.get("boardId") or info.get("boardCode")
    out["lot_size"]   = (info.get("lotSize") or info.get("roundLot") or
                         info.get("lot_size"))
    tick = (info.get("tickSize") or info.get("priceTick") or info.get("tick_size"))
    try:
        out["tick_size"] = float(tick) if tick is not None else None
    except Exception:
        out["tick_size"] = None

    out["sector"]     = info.get("sector")
    out["industry"]   = info.get("industry")
    # ถ้าเรียกสำเร็จ แปลว่าเทรดได้ในบริบทบัญชีนี้
    out["is_tradable"]= True
    out["raw"]        = info
    return out

def main():
    ensure_table()
    df12 = fetch_symbols_twelvedata()
    print(f"Fetched from TwelveData: {len(df12)} symbols")

    # เตรียม settrade_v2 (optional)
    inv, eq = init_settrade()
    use_settrade = eq is not None

    rows = []
    for i, r in df12.iterrows():
        sym = str(r["symbol"])
        extra = dict(market=None, board=None, lot_size=None, tick_size=None,
                     sector=None, industry=None, is_tradable=None, raw=None)
        if use_settrade:
            extra = enrich_with_settrade(eq, sym)
            # เบรกจังหวะเล็กน้อย กัน rate-limit
            time.sleep(0.08)

        provider_raw = {
            "twelvedata": {k: (None if pd.isna(v) else v) for k,v in r.to_dict().items()},
            "settrade_v2": extra["raw"]
        }

        rows.append((
            sym,                       # symbol
            r["exchange"] or "XBKK",   # exchange
            r["full_code"],            # full_code
            r["name_en"],              # name_en
            r["currency"],             # currency
            r["asset_type"],           # asset_type
            extra["market"],           # market
            extra["board"],            # board
            extra["sector"],           # sector
            extra["industry"],         # industry
            extra["lot_size"],         # lot_size
            extra["tick_size"],        # tick_size
            extra["is_tradable"],      # is_tradable
            json.dumps(provider_raw)   # provider_raw
        ))
        if len(rows) % 50 == 0:
            print(f"Prepared {len(rows)} rows...")

    with pg_conn() as conn, conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows, page_size=1000)
        conn.commit()
    print(f"Upserted {len(rows)} rows into dim_symbol_th")

if __name__ == "__main__":
    main()
