import os
import urllib
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# -----------------------------
#  Config
# -----------------------------
load_dotenv()  # โหลด .env ถ้ามี

PGHOST =  os.getenv("posql_host")
PGPORT = os.getenv("posql_port", "5432")
PGDATABASE = os.getenv("posql_db")
PGUSER = os.getenv("posql_user")
PGPASSWORD = os.getenv("posql_password")

SRC_TABLE = "public.stock_list_info_siamchart"
DEST_TABLE = "public.stock_value_score"   # ตารางผลลัพธ์

# น้ำหนักรวม (จะถูก normalize อีกครั้งตาม metric ที่มีจริง)
WEIGHTS = {
    # Profitability 40%
    "roe_score": 0.40/4,
    "roa_score": 0.40/4,
    "npm_score": 0.40/4,
    "eps_score": 0.40/4,
    # Valuation 25%
    "pe_score": 0.25/3,
    "pbv_score": 0.25/3,
    "peg_score": 0.25/3,
    # Stability 15%
    "de_score": 0.15,
    # Shareholder return 10%
    "yield_score": 0.10/2,
    "dps_score": 0.10/2,
    # Growth 10% (ถ้าไม่มีก็ถ่ายน้ำหนักไปให้ mg_score)
    "cg_score": 0.10/2,   # optional
    "mg_score": 0.10/2,   # ถ้าไม่มี cg_score จะถูกปรับน้ำหนักให้ mg_score แทน
}

# -----------------------------
#  Connect
# -----------------------------
# conn_str = f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"
conn_str = f"postgresql+psycopg2://{PGUSER}:{urllib.parse.quote_plus(PGPASSWORD)}@{PGHOST}:{PGPORT}/{PGDATABASE}"
engine = create_engine(conn_str)

# -----------------------------
#  Load source
# -----------------------------
with engine.connect() as con:
    df = pd.read_sql(text(f"SELECT * FROM {SRC_TABLE} --limit 10 --WHERE name in ('COMAN','COM7') "), con)

if df.empty:
    raise SystemExit(f"No data found in {SRC_TABLE}")

# -----------------------------
#  Normalize columns & alias map
# -----------------------------
df.columns = [c.strip().lower() for c in df.columns]

aliases = {
    "name":   ["name","sign","symbol","ticker","stock"],
    "pe":     ["pe","p/e"],
    "pbv":    ["pbv","p/bv"],
    "peg":    ["peg"],
    "de":     ["de","d/e","de_ratio"],
    "roe":    ["roe","roe%","roe_pct"],
    "roa":    ["roa","roa%","roa_pct"],
    "npm":    ["npm","npm%","net_profit_margin","net_margin"],
    "eps":    ["eps"],
    "yield":  ["yield","yield%","dividend_yield","dy"],
    "dps":    ["dps","dividend_per_share"],
    "mg":     ["mg","gross_margin","gm"],
    "cg":     ["cg","growth","profit_growth","np_growth"],   # optional
    "magic1": ["magic1"],    # optional
    "magic2": ["magic2"],    # optional
}

colmap = {}
for k, opts in aliases.items():
    for o in opts:
        if o in df.columns:
            colmap[k] = o
            break

required_any = ["name"]
missing_req = [x for x in required_any if x not in colmap]
if missing_req:
    raise ValueError(f"Missing required columns in source: {missing_req}  (found: {list(df.columns)})")

# สร้าง working frame
metrics = ["pe","pbv","peg","de","roe","roa","npm","eps","yield","dps","mg","cg","magic1","magic2"]
present = [m for m in metrics if m in colmap]
work = df[[colmap["name"]] + [colmap[m] for m in present]].copy()
work.columns = ["name"] + present

# แปลงเป็นตัวเลข
for m in present:
    work[m] = pd.to_numeric(work[m], errors="coerce")

# -----------------------------
#  Scoring (percentile rank)
# -----------------------------
lower_better = {"pe","pbv","peg","de","magic1","magic2"}
higher_better = {"roe","roa","npm","eps","yield","dps","mg","cg"}

# ฟังก์ชันคำนวณสกอร์แบบ percentile rank
def pct_score(series: pd.Series, higher_is_better: bool) -> pd.Series:
    s = series.copy()
    n = s.notna().sum()
    if n == 0:
        # ทั้งคอลัมน์ไม่มีข้อมูลเลย → ให้ 0 หมด
        return pd.Series(0.0, index=s.index)

    # จัดอันดับเป็นเปอร์เซ็นไทล์
    r = s.rank(ascending=True, pct=True)

    if higher_is_better:
        score = r                      # ค่าสูง → สกอร์สูง
    else:
        score = 1 - r + (1.0 / n)      # ค่าต่ำ → สกอร์สูง

    # ถ้าไม่มีข้อมูลในแถว → ให้ 0
    return score.where(s.notna(), 0.0)



scores = pd.DataFrame(index=work.index)
for m in present:
    if m in higher_better:
        scores[f"{m}_score"] = pct_score(work[m], higher_is_better=True)
    elif m in lower_better:
        scores[f"{m}_score"] = pct_score(work[m], higher_is_better=False)
    else:
        # default ให้ถือว่า higher is better
        scores[f"{m}_score"] = pct_score(work[m], higher_is_better=True)

# -----------------------------
#  Weight normalization
# -----------------------------
# ถ้าไม่มี cg_score ให้เทน้ำหนัก growth ทั้งหมดไปที่ mg_score
use_weights = WEIGHTS.copy()
if "cg_score" not in scores.columns:
    mg_weight = use_weights.get("mg_score", 0.0) + use_weights.get("cg_score", 0.0)
    use_weights["mg_score"] = mg_weight
    use_weights["cg_score"] = 0.0

available = {k: v for k, v in use_weights.items() if k in scores.columns and v > 0}
w_sum = sum(available.values())
norm_weights = {k: v / w_sum for k, v in available.items()} if w_sum > 0 else {}

# -----------------------------
#  Compute value_score & rank
# -----------------------------
value_score = np.zeros(len(scores))
for k, w in norm_weights.items():
    value_score += scores[k].values * w

out = pd.concat([work[["name"]], pd.Series(value_score, name="value_score", index=work.index)], axis=1)

# แนบ component score ที่ใช้จริงเพื่อความโปร่งใส
for k in sorted(norm_weights.keys()):
    out[k] = scores[k]

# อันดับ (1 ดีสุด)
out["rank"] = out["value_score"].rank(ascending=False, method="min").astype(int)

# -----------------------------
#  Save to Postgres
# -----------------------------
with engine.begin() as con:
    # สร้างตารางปลายทาง (replace)
    create_sql = f"""
    DROP TABLE IF EXISTS {DEST_TABLE};
    CREATE TABLE {DEST_TABLE} (
        name TEXT,
        value_score DOUBLE PRECISION,
        rank INTEGER,
        {", ".join([f"{k} DOUBLE PRECISION" for k in sorted(norm_weights.keys())])}
    );
    """
    con.execute(text(create_sql))

    # เขียนข้อมูลลงตารางปลายทาง
    out.to_sql(DEST_TABLE.split(".")[-1], con, schema=DEST_TABLE.split(".")[0],
               if_exists="append", index=False)

    # ดัชนีช่วยค้น (ทางเลือก)
    con.execute(text(f"CREATE INDEX ON {DEST_TABLE} (rank);"))
    con.execute(text(f"CREATE INDEX ON {DEST_TABLE} (name);"))

print(f"Done. Wrote {len(out)} rows to {DEST_TABLE}")
