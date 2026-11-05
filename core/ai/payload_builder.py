#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : payload_builder.py
LOCATION    : core/ai/
OCCASION    : ถูกเรียกโดย AI Router ก่อนส่งข้อมูลให้โมเดล AI ใดๆ
DESCRIPTION : Data Preparation Layer: ดึงและรวบรวมข้อมูลหุ้นผู้สมัคร
              - สัญญาณซื้อสด (Trigger Details จาก v_bot_buy_opportunities)
              - งบการเงิน & คุณภาพ (จาก stock_list_info_siamchart & stock_value_score)
              - ประวัติแท่งเทียนและ Indicator 10 วันทำการ (จาก stock_indicator_jsonb & history)
              แปลงเป็น Standardized JSON Payload ที่เป็นอิสระจาก AI Provider ใดๆ
==============================================================================
"""

import os
import json
from datetime import date, datetime
from decimal import Decimal
import psycopg2
from psycopg2.extras import RealDictCursor
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

def build_candidate_payload(conn, symbols: list = None, target_date: str = None, max_candidates: int = 10, config: dict = None) -> dict:
    """
    สร้าง Standardized Data Payload สำหรับส่งให้ AI วิเคราะห์
    - หากไม่ระบุ symbols จะดึงจาก v_bot_buy_opportunities ของวันที่ระบุ
    - ดึงประวัติ 10 วันทำการย้อนหลังพร้อม Indicators ครบทุกตัว
    """
    if config is None:
        config = {}

    budget_cfg = config.get("budget_limits", {})
    trade_limit_thb = budget_cfg.get("max_amount_per_trade_thb", 1500.0)
    day_limit_thb = budget_cfg.get("max_amount_per_day_thb", 4000.0)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # กำหนดวันที่ล่าสุดหากไม่ได้ระบุ (เช็คจาก bot_trade_signals ก่อนเพื่อให้ตรงกับ /scan)
        if not target_date:
            cur.execute("SELECT MAX(trade_date) AS max_date FROM public.bot_trade_signals WHERE status = 'PENDING';")
            row = cur.fetchone()
            if row and row["max_date"]:
                target_date = str(row["max_date"])
            else:
                cur.execute("SELECT MAX(trade_date) AS max_date FROM public.v_bot_buy_opportunities;")
                row = cur.fetchone()
                target_date = str(row["max_date"]) if row and row["max_date"] else date.today().isoformat()

        # หากไม่ได้ระบุ symbols: ดึงผู้สมัครตามลำดับคิวของ /scan (bot_trade_signals) ก่อนเป็นอันดับแรก
        if not symbols:
            cur.execute("""
                SELECT symbol
                FROM public.bot_trade_signals
                WHERE status = 'PENDING'
                  AND trade_date = %s
                ORDER BY priority DESC, id ASC
                LIMIT %s;
            """, (target_date, max_candidates))
            rows = cur.fetchall()
            if rows:
                symbols = [r["symbol"] for r in rows]
            else:
                # กรณี bot_trade_signals ยังไม่มี ให้ fallback ไปยัง v_bot_buy_opportunities โดยเรียงเกณฑ์เดียวกับ /scan
                cur.execute("""
                    SELECT symbol
                    FROM public.v_bot_buy_opportunities
                    WHERE trade_date = %s
                    ORDER BY (CASE WHEN trigger_price <= 10.0 THEN 1 ELSE 2 END), priority DESC, macd_12_26_9_hist DESC
                    LIMIT %s;
                """, (target_date, max_candidates))
                symbols = [r["symbol"] for r in cur.fetchall()]

        payload = {
            "market_date": target_date,
            "market_context": "SET Index Trading Strategy: MACD Momentum + Breakout Scanner with ATR Risk Clamps",
            "budget_constraints": {
                "max_amount_per_trade_thb": trade_limit_thb,
                "max_amount_per_day_thb": day_limit_thb,
                "safety_clamp_stop_loss_pct": "4.0% - 5.5%"
            },
            "candidates_count": len(symbols),
            "candidates": []
        }

        for sym in symbols:
            # 1. ข้อมูล Trigger ปัจจุบัน (ค้นหาจาก v_bot_buy_opportunities ก่อน)
            cur.execute("""
                SELECT symbol, trade_date, trigger_price, volume, vol_surge_ratio, 
                       macd_12_26_9_hist_accel_pct, market_zone, dual_macd_confluence, 
                       atr14, strategy_name, priority, reason
                FROM public.v_bot_buy_opportunities
                WHERE symbol = %s AND trade_date = %s;
            """, (sym, target_date))
            trig = cur.fetchone()
            if not trig:
                # ลองค้นหาจาก bot_trade_signals (รองรับหุ้นจาก Hybrid Selection หรือ Signal อื่นๆ)
                cur.execute("""
                    SELECT symbol, trade_date, trigger_price, 0 as volume,
                           1.0 as vol_surge_ratio, 0.0 as macd_12_26_9_hist_accel_pct,
                           'BOT_SIGNAL' as market_zone, false as dual_macd_confluence,
                           0.0 as atr14, signal_source as strategy_name, priority, reason
                    FROM public.bot_trade_signals
                    WHERE symbol = %s AND trade_date = %s
                    ORDER BY priority DESC, id ASC LIMIT 1;
                """, (sym, target_date))
                trig = cur.fetchone()
            if not trig:
                # ดึงข้อมูลราคาล่าสุดจาก stock_price_history หากไม่มี Trigger สดในวันนี้
                cur.execute("""
                    SELECT symbol, date as trade_date, close as trigger_price, volume,
                           1.0 as vol_surge_ratio, 0.0 as macd_12_26_9_hist_accel_pct,
                           'MANUAL_QUERY' as market_zone, false as dual_macd_confluence,
                           0.0 as atr14, 'MANUAL_QUERY' as strategy_name, 0 as priority,
                           'ผู้ใช้วิเคราะห์หุ้นรายตัวตามคำขอ' as reason
                    FROM public.stock_price_history
                    WHERE symbol = %s
                    ORDER BY date DESC LIMIT 1;
                """, (sym,))
                trig = cur.fetchone()
                if not trig:
                    continue

            # 2. งบการเงิน & Value Score
            cur.execute("""
                SELECT s.pe, s.pbv, s.de, s.roe, s.roa, s.npm, s.yield, s.mcapm, s.ffloat,
                       v.value_score, v.rank as value_rank
                FROM public.stock_list_info_siamchart s
                LEFT JOIN public.stock_value_score v ON s.name = v.name
                WHERE s.name = %s;
            """, (sym,))
            fund = cur.fetchone() or {}

            # 3. ข้อมูลย้อนหลัง 10 วันทำการ (OHLCV + Indicators)
            cur.execute("""
                SELECT p.date, p.open, p.high, p.low, p.close, p.volume,
                       i.indicators
                FROM public.stock_price_history p
                LEFT JOIN public.stock_indicator_jsonb i ON p.symbol = i.symbol AND p.date = i.trade_date
                WHERE p.symbol = %s AND p.date <= %s
                ORDER BY p.date DESC
                LIMIT 10;
            """, (sym, target_date))
            hist_rows = cur.fetchall()
            hist_rows.reverse() # เรียงจากเก่าไปใหม่ (D-9 ไปหา D0)

            history_10d = []
            for idx, row in enumerate(hist_rows):
                ind = row.get("indicators") or {}
                day_offset = idx - (len(hist_rows) - 1)
                close_p = float(row["close"])
                open_p = float(row["open"])
                high_p = float(row["high"])
                low_p = float(row["low"])
                vol = int(row["volume"])
                chg_pct = round(((close_p - open_p) / open_p) * 100, 2) if open_p > 0 else 0.0

                e5 = round(float(ind.get("ema5", 0)), 2)
                e10 = round(float(ind.get("ema10", 0)), 2)
                e20 = round(float(ind.get("ema20", 0)), 2)
                e50 = round(float(ind.get("ema50", 0)), 2)
                e200 = round(float(ind.get("ema200", 0)), 2)

                if close_p > e5 >= e10 >= e20 >= e50 > e200:
                    ema_stack = "PERFECT_BULLISH_STACK (5>10>20>50>200)"
                elif close_p > e20 > e50 > e200:
                    ema_stack = "BULLISH_STACK (20>50>200)"
                elif close_p > e200:
                    ema_stack = "ABOVE_EMA200"
                else:
                    ema_stack = "BELOW_EMA200"

                b_up = round(float(ind.get("bb_upper", 0)), 2)
                b_mid = round(float(ind.get("bb_mid", 0)), 2)
                b_low = round(float(ind.get("bb_lower", 0)), 2)
                b_width = round(b_up - b_low, 2)
                if close_p >= b_up:
                    bb_state = "BREAKOUT_ABOVE_UPPER"
                elif (b_up - close_p) / (close_p or 1) <= 0.008:
                    bb_state = "TESTING_UPPER_BAND"
                elif close_p <= b_low:
                    bb_state = "OVERSOLD_BELOW_LOWER"
                else:
                    bb_state = "INSIDE_BANDS"

                m12 = round(float(ind.get("macd_12_26_9", 0)), 4)
                m12_sig = round(float(ind.get("macd_12_26_9_signal", 0)), 4)
                m12_hist = round(float(ind.get("macd_12_26_9_hist", 0)), 4)

                m19 = round(float(ind.get("macd_19_39_9", 0)), 4)
                m19_sig = round(float(ind.get("macd_19_39_9_signal", 0)), 4)
                m19_hist = round(float(ind.get("macd_19_39_9_hist", 0)), 4)

                rsi14 = round(float(ind.get("rsi14", 0)), 1)
                rsi21 = round(float(ind.get("rsi21", 0)), 1)
                rsi_state = "EXPANDING (RSI14 > RSI21)" if rsi14 > rsi21 else "CONTRACTING (RSI14 <= RSI21)"

                v_avg20 = int(float(ind.get("volume_avg20", 0)))
                v_e5 = int(float(ind.get("volume_ema5", 0)))
                v_e20 = int(float(ind.get("volume_ema20", 0)))
                v_ratio = round(vol / (v_avg20 or 1), 2)

                history_10d.append({
                    "day": f"D{day_offset}" if day_offset != 0 else "D0 (Today)",
                    "date": row["date"].isoformat(),
                    "price_action": {
                        "open": open_p, "high": high_p, "low": low_p, "close": close_p,
                        "change_pct": chg_pct,
                        "candle_type": "BULLISH_CANDLE" if close_p >= open_p else "BEARISH_CANDLE"
                    },
                    "moving_averages": {
                        "ema5": e5, "ema10": e10, "ema20": e20, "ema50": e50, "ema200": e200,
                        "structure": ema_stack
                    },
                    "dual_macd": {
                        "fast_12_26_9": {"macd": m12, "signal": m12_sig, "hist": m12_hist, "cross": "BULLISH" if m12 > m12_sig else "BEARISH"},
                        "slow_19_39_9": {"macd": m19, "signal": m19_sig, "hist": m19_hist, "cross": "BULLISH" if m19 > m19_sig else "BEARISH"}
                    },
                    "dual_rsi": {
                        "rsi14": rsi14, "rsi21": rsi21, "state": rsi_state
                    },
                    "bollinger_and_volatility": {
                        "bb_upper": b_up, "bb_mid": b_mid, "bb_lower": b_low, "bb_bandwidth": b_width,
                        "bb_state": bb_state, "atr14": round(float(ind.get("atr14", 0)), 3)
                    },
                    "volume_dynamics": {
                        "volume": vol, "vol_avg20": v_avg20, "vol_ema5": v_e5, "vol_ema20": v_e20,
                        "vol_surge_ratio": v_ratio
                    }
                })

            # คำนวณกรอบ Stop Loss เริ่มต้น พร้อม Clamp 4.0% - 5.5%
            trig_p = float(trig["trigger_price"])
            atr_v = float(trig["atr14"]) if trig["atr14"] else (trig_p * 0.03)
            sl_candidate = trig_p - (atr_v * 2.0)
            loss_pct = ((trig_p - sl_candidate) / trig_p) * 100
            if loss_pct < 4.0:
                final_sl = round(trig_p * 0.96, 2)
                loss_pct = 4.0
            elif loss_pct > 5.5:
                final_sl = round(trig_p * 0.945, 2)
                loss_pct = 5.5
            else:
                final_sl = round(sl_candidate, 2)

            payload["candidates"].append({
                "symbol": sym,
                "priority_stars": int(trig.get("priority", 2)),
                "trade_signal": {
                    "entry_price": trig_p,
                    "proposed_stop_loss": final_sl,
                    "planned_risk_pct": round(loss_pct, 2),
                    "market_zone": trig["market_zone"],
                    "dual_macd_confluence": trig["dual_macd_confluence"],
                    "vol_surge_ratio": float(trig["vol_surge_ratio"]),
                    "macd_hist_accel_pct": f"{float(trig['macd_12_26_9_hist_accel_pct']):+.2f}%" if trig['macd_12_26_9_hist_accel_pct'] else "N/A",
                    "signal_reason": trig["reason"]
                },
                "financial_health_and_valuation": {
                    "pe_ratio": float(fund.get("pe") or 0),
                    "pbv_ratio": float(fund.get("pbv") or 0),
                    "debt_to_equity_ratio": float(fund.get("de") or 0),
                    "net_profit_margin_pct": float(fund.get("npm") or 0),
                    "roe_pct": float(fund.get("roe") or 0),
                    "roa_pct": float(fund.get("roa") or 0),
                    "dividend_yield_pct": float(fund.get("yield") or 0),
                    "free_float_pct": float(fund.get("ffloat") or 0),
                    "market_cap_million_thb": float(fund.get("mcapm") or 0),
                    "value_score": round(float(fund.get("value_score") or 0), 3),
                    "value_rank": int(fund.get("value_rank") or 0)
                },
                "historical_10_trading_days": history_10d
            })

        return payload
