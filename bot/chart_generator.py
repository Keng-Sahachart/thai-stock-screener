'''
โมดูลสร้างชาร์ตกราฟ (bot/chart_generator.py)
ทำหน้าที่ดึงแท่งเทียนย้อนหลัง 60 วันจาก stock_price_history 
และวาดแท่งเทียนคู่กับเส้น EMA 20/50, Dual MACD Histogram และแถบ Volume เป็นรูปภาพใน Memory Buffer โดยไม่ต้องเขียนไฟล์ลงดิสก์:
'''

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import io
import os
import psycopg2
import pandas as pd
import mplfinance as mpf
import matplotlib
matplotlib.use("Agg")  # สำหรับ headless server
import matplotlib.pyplot as plt
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("posql_host", "localhost"),
        port=os.getenv("posql_port", "5432"),
        dbname=os.getenv("posql_db", "stocks"),
        user=os.getenv("posql_user", "postgres"),
        password=os.getenv("posql_password", "postgres")
    )

def generate_stock_chart(symbol: str, lookback_days: int = 65) -> io.BytesIO:
    conn = get_db_connection()
    try:
        query = """
            SELECT 
                p.date, p.open, p.high, p.low, p.close, p.volume,
                --m.ema20, m.ema50,
                m.ema12, m.ema26,
                m.macd_12_26_9, m.macd_12_26_9_signal, m.macd_12_26_9_hist,
                m.macd_19_39_9_hist
            FROM public.stock_price_history p
            LEFT JOIN public.mv_stock_indicators m 
                ON p.symbol = m.symbol AND p.date = m.trade_date
            WHERE p.symbol = %s
            ORDER BY p.date DESC
            LIMIT %s;
        """
        df = pd.read_sql(query, conn, params=(symbol, lookback_days))
        if df.empty or len(df) < 15:
            return None

        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        df.sort_index(inplace=True)

        for col in ["open", "high", "low", "close", "volume", "ema12", "ema26", 
                    "macd_12_26_9", "macd_12_26_9_signal", "macd_12_26_9_hist", "macd_19_39_9_hist"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # กำหนดแถบและเส้น Indicators เพิ่มเติม
        added_plots = [
            mpf.make_addplot(df["ema12"], color="#f39c12", width=1.0, panel=0),
            mpf.make_addplot(df["ema26"], color="#2980b9", width=1.0, panel=0),
            # MACD 12/26/9
            mpf.make_addplot(df["macd_12_26_9"], color="#8e44ad", width=1.0, panel=2, ylabel="MACD"),
            mpf.make_addplot(df["macd_12_26_9_signal"], color="#e74c3c", width=0.8, panel=2),
            mpf.make_addplot(df["macd_12_26_9_hist"], type="bar", color=["#2ecc71" if v >= 0 else "#e74c3c" for v in df["macd_12_26_9_hist"]], panel=2)
        ]

        mc = mpf.make_marketcolors(
            up="#2ecc71", 
            down="#e74c3c", 
            edge="inherit", 
            wick="inherit", 
            volume="in"
        )
        
        custom_style = mpf.make_mpf_style(
            base_mpf_style="yahoo",
            marketcolors=mc,  # ใช้หรือไม่ใช้ก็ได้ / หากต้องการกำหนดสีขอบของแท่งเทียนให้ชัดเจนเป็นพิเศษ สามารถใช้ mpf.make_marketcolors() ควบคู่กันได้
            gridcolor="#ecf0f1",
            facecolor="#ffffff",
            # edge="black" -- error: edge is not a valid style parameter
        )

        buf = io.BytesIO()
        fig, _ = mpf.plot(
            df,
            type="candle",
            style=custom_style,
            addplot=added_plots,
            volume=True,
            panel_ratios=(5, 1.5, 2.5),
            figsize=(10, 6.5),
            title=f"\n{symbol} Technical Context (EMA12/26, Volume, MACD)",
            returnfig=True
        )
        fig.savefig(buf, format="png", bbox_inches="tight", dpi=100)
        plt.close(fig)
        buf.seek(0)
        return buf
    finally:
        conn.close()