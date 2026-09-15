#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : settrade_executor.py
LOCATION    : execution/
OCCASION    : ถูกเรียกใช้งานโดย order_manager.py เมื่อระบบทำงานในโหมด dry_run: false
DESCRIPTION : ดึงราคาตลาดสด, ตรวจสอบ Line Available สดจากบัญชี, คำนวณ SL ใหม่,
              ทำ Pre-flight Check และส่งคำสั่งซื้อ-ขายหลักทรัพย์จริงเข้าตลาดหลักทรัพย์ผ่าน Settrade Open API
==============================================================================
"""

import datetime
from datetime import datetime
import os
import json
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
from settrade_v2 import Investor
from dotenv import load_dotenv

import initialApp as cfg
from core.tick_utils import adjust_price_by_ticks
import update.update_Port_info as uport_info
import update.updatePort as uport

load_dotenv()
ACCOUNT_NO = os.getenv("account_no")
PIN_ACTION = os.getenv("pin")  # PIN ของบัญชี Settrade สำหรับการส่งคำสั่งซื้อขายจริง
CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "bot_config.json"

def load_config():
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("posql_host", "localhost"),
        port=os.getenv("posql_port", "5432"),
        dbname=os.getenv("posql_db", "stocks"),
        user=os.getenv("posql_user", "postgres"),
        password=os.getenv("posql_password", "postgres")
    )


def get_realtime_quote(investor: Investor,symbol: str) -> dict:
    """ดึงราคาตลาดสด (Last, Best Bid, Best Offer) จาก Settrade API"""
    try:
        if not investor:
            investor = Investor(**cfg.args_Investor)
        market = investor.MarketData()
        quote = market.get_quote_symbol(symbol=symbol)
        bid_list = quote.get("bid", [])
        offer_list = quote.get("offer", [])
        return {
            "last": float(quote.get("last", 0.0) or 0.0),
            "bid": float(bid_list[0]) if bid_list else 0.0,
            "offer": float(offer_list[0]) if offer_list else 0.0
        }
    except Exception as e:
        print(f"[QUOTE ERROR] ไม่สามารถดึงราคาตลาดสด {symbol}: {e}")
        return {}

def recalculate_stop_loss(entry_price: float, atr14: float = None) -> float:
    """คำนวณ Stop Loss ใหม่ตามราคาตลาดสด พร้อม Safety Clamp 4% - 8%"""
    if atr14 and atr14 > 0:
        candidate_sl = entry_price - (atr14 * 2.0)
    else:
        candidate_sl = entry_price * 0.95

    loss_pct = ((entry_price - candidate_sl) / entry_price) * 100.0
    if loss_pct < 4.0:
        candidate_sl = entry_price * 0.96
    elif loss_pct > 8.0:
        candidate_sl = entry_price * 0.92

    return round(candidate_sl, 4)


def execute_real_buy(signal_id: int, symbol: str, volume: int, target_price: float, stop_loss_plan: float, atr14: float = None, buy_ticks: int = None, is_fixed_price: bool = None) -> dict:
    """ส่งคำสั่งซื้อจริงผ่าน Settrade Open API"""
    if buy_ticks is None:
        cfg_data = load_config()
        buy_ticks = cfg_data.get("tick_execution", {}).get("buy_ticks", 0)

    # ตัดสินใจประเภทคำสั่ง:
    # - ถ้ามีระบุ is_fixed_price มาชัดเจน ให้ยึดตามนั้น
    # - ถ้าไม่ระบุ: หาก signal_id มีค่า แปลว่ามาจาก Auto-Scanner (สัญญาณกราฟ) -> is_fixed_price = False (เอาราคาตลาดสด + buy_ticks)
    #               หากไม่มี signal_id แต่มี target_price > 0 แปลว่ามาจาก /buy -> is_fixed_price = True (Fix ราคา)
    if is_fixed_price is None:
        is_fixed_price = (signal_id is None and target_price is not None and float(target_price) > 0)

    conn = get_db_connection()
    try:
        investor = Investor(**cfg.args_Investor)
        equity = investor.Equity(account_no=ACCOUNT_NO)

        if is_fixed_price and target_price and float(target_price) > 0:
            # กรณีที่ 1: คำสั่ง Fix ราคาตามที่ผู้ใช้กำหนด (เช่น /buy etc 200 0.76)
            buy_price = round(float(target_price), 2)
            live_sl = round(float(stop_loss_plan), 4) if (stop_loss_plan and float(stop_loss_plan) > 0) else recalculate_stop_loss(buy_price, atr14)
        else:
            # กรณีที่ 2: อนุมัติจากสัญญาณ Auto-Scanner หรือคำสั่งที่ต้องการราคาตลาดสด
            live_quote = get_realtime_quote(investor, symbol) # ดึงราคาตลาด
            best_offer = live_quote.get("offer", 0.0) # ราคา Bid (ดีที่สุด)
            last_price = live_quote.get("last", 0.0) # ราคาล่าสุด
            base_p = best_offer if best_offer > 0 else (last_price if last_price > 0 else (target_price or 1.0)) # ราคาซื้อ = ราคา Bid > 0 ? ราคา Bid : ราคาล่าสุด > 0 ? ราคาล่าสุด : ราคาเป้าหมาย หรือ 1.0
            buy_price = adjust_price_by_ticks(base_p, ticks=buy_ticks) if base_p > 0 else 1.0 # ราคาซื้อ = ราคา Bid > 0 ? ราคา Bid : ราคาล่าสุด > 0 ? ราคาล่าสุด : ราคาเป้าหมาย หรือ 1.0 + buy_ticks
            live_sl = recalculate_stop_loss(buy_price, atr14) # คำนวณ Stop Loss ใหม่

        # 3. ดึงยอดเงินสด (Line Available) สดๆ จาก Settrade
        acc_info = equity.get_account_info()
        line_available = float(acc_info.get("lineAvailable", 0.0))
        uport_info.save_account_info(ACCOUNT_NO, acc_info)  # บันทึกลงตารางทันที

        # 4. Pre-flight Check เงินคงเหลือ vs ยอดซื้อ (รวม Buffer ค่าคอม 0.25%)
        estimated_cost = round(volume * buy_price * 1.0025, 2)
        if estimated_cost > line_available:
            diff = estimated_cost - line_available
            error_msg = (
                f"❌ ยอดเงินสดไม่พอส่งคำสั่งซื้อ {symbol}!\n"
                f"• ยอดที่ต้องใช้: {estimated_cost:,.2f} THB\n"
                f"• วงเงินซื้อคงเหลือ (Line): {line_available:,.2f} THB\n"
                f"• ขาดอีก: {diff:,.2f} THB"
            )
            print(f"[PRE-FLIGHT REJECTED] {error_msg}")
            return {"success": False, "error": error_msg}

        # 5. ส่งคำสั่ง Limit Buy ด้วยราคาตลาดสด (Best Offer)
        current_date = datetime.now().strftime('%Y-%m-%d')
        res = equity.place_order(
                            side= "Buy",
                            symbol= symbol,
                            trustee_id_type= "Local",
                            volume= volume,
                            qty_open= 0,
                            price= buy_price,
                            price_type= "Limit",
                            validity_type= "Day",
                            bypass_warning= True,
                            valid_till_date= current_date,
                            pin= PIN_ACTION
                            )

        broker_order_no = str(res.get("orderNo", ""))

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
             # บันทึกลง bot_orders เป็น SENT รอผลจาก sync_order_status
            cur.execute("""
                INSERT INTO public.bot_orders (
                    signal_id, symbol, side, order_type, volume,
                    target_price, status, broker_order_no
                ) VALUES (
                    %s, %s, 'BUY', 'LIMIT', %s,
                    %s, 'SENT', %s
                ) RETURNING order_id;
            """, (signal_id, symbol, volume, buy_price, broker_order_no))
            order_id = cur.fetchone()["order_id"]

            # สร้าง Position รอไว้ใน bot_active_positions (จะถูกอัปเดตราคา Match เมื่อ Filled)
            cur.execute("""
                INSERT INTO public.bot_active_positions (
                    symbol, entry_date, entry_price, entry_atr14,
                    initial_stop_loss, max_price_reached, current_volume,
                    is_managed_by_bot, status
                ) VALUES (
                    %s, CURRENT_DATE, %s, %s,
                    %s, %s, %s,
                    TRUE, 'OPEN'
                )
                ON CONFLICT (symbol) WHERE status = 'OPEN'
                DO UPDATE SET
                    current_volume = bot_active_positions.current_volume + EXCLUDED.current_volume,
                    updated_at = timezone('Asia/Bangkok', now());
            """, (symbol, buy_price, atr14, live_sl, buy_price, volume))

            conn.commit()

            # ซิงค์ยอดเงินล่าสุดหลังส่งคำสั่งซื้อสำเร็จ เพื่ออัปเดต Line Available ใน account_info_history ทันที
            try:
                post_acc_info = equity.get_account_info()
                uport_info.save_account_info(ACCOUNT_NO, post_acc_info)
                print(f"[SETTRADE BUY] อัปเดตยอดเงินคงเหลือล่าสุดหลังสั่งซื้อสำเร็จ (Line: {float(post_acc_info.get('lineAvailable', 0)):,.2f} THB)")
            except Exception as a_err:
                print(f"[SETTRADE BUY ACCOUNT SYNC WARNING] {a_err}")

        return {
            "success": True,
            "order_id": order_id,
            "broker_order_no": broker_order_no,
            "status": "SENT",
            "buy_price": buy_price,
            "stop_loss": live_sl
        }

    except Exception as e:
        conn.rollback()
        print(f"[SETTRADE BUY ERROR] {e}")
        return {"success": False, "error": str(e)}
    finally:
        conn.close()

def execute_real_sell(symbol: str, volume: int, exit_price: float, exit_reason: str, profit_ticks: int = None) -> dict:
    """ส่งคำสั่งขายจริงผ่าน Settrade Open API"""
    if profit_ticks is None:
        cfg_data = load_config()
        profit_ticks = cfg_data.get("tick_execution", {}).get("sell_profit_ticks", 1)

    conn = get_db_connection()
    try:
        investor = Investor(**cfg.args_Investor)
        equity = investor.Equity(account_no=ACCOUNT_NO)

        # 1. ดึงราคาตลาดสด ณ วินาทีที่จะสั่งขาย
        live_quote = get_realtime_quote(investor,symbol)
        live_last = live_quote.get("last", exit_price)
        best_bid = live_quote.get("bid", live_last)

        # 2. กำหนดราคาขายตามระดับความเร่งด่วน
        if exit_reason in ("HARD_CUT_LOSS", "INITIAL_SL_HIT", "PANIC_CIRCUIT_BREAKER"):
            # กรณีหนีตาย: ขายที่ราคา Best Bid หรือ Last เพื่อการันตีการ Match ทันที
            final_sell_price = best_bid if best_bid > 0 else live_last
        else:
            # กรณีขายปกติ/ขายทำกำไร: ขยับราคาขายขึ้นตาม Tick Size ที่กำหนด (เช่น +1 ถึง +3 Ticks)
            base_p = live_last if live_last > 0 else exit_price
            final_sell_price = adjust_price_by_ticks(base_p, ticks=profit_ticks)

        # 3. ยิงคำสั่ง Limit Sell เข้า Settrade
        current_date = datetime.now().strftime('%Y-%m-%d')
        res = equity.place_order(
                            pin= PIN_ACTION,
                            side= "Sell",
                            symbol= symbol,
                            trustee_id_type= "Local",
                            volume= volume,
                            qty_open= 0,
                            price= final_sell_price,
                            price_type= "Limit",
                            validity_type= "Day",
                            bypass_warning= False,
                            valid_till_date= current_date
                            )

        broker_order_no = str(res.get("orderNo", ""))

        # 4. บันทึกลง bot_orders และ bot_active_positions
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO public.bot_orders (
                    symbol, side, order_type, volume,
                    target_price, status, broker_order_no
                ) VALUES (%s, 'SELL', 'LIMIT', %s, %s, 'SENT', %s)
                RETURNING order_id;
            """, (symbol, volume, final_sell_price, broker_order_no))
            order_id = cur.fetchone()["order_id"]

            cur.execute("""
                UPDATE public.bot_active_positions
                SET status = 'CLOSED', closed_date = CURRENT_DATE,
                    closed_price = %s, exit_reason = %s,
                    updated_at = timezone('Asia/Bangkok', now())
                WHERE symbol = %s AND status = 'OPEN';
            """, (final_sell_price, exit_reason, symbol))

            #ถ้าไม่มีแถวที่ถูกอัปเดต (แปลว่าเป็นหุ้นพอร์ตเดิม/Manual) ให้ Insert แถวประวัติไว้
            if cur.rowcount == 0:
                cur.execute("""
                    INSERT INTO public.bot_active_positions (
                        symbol, entry_date, entry_price, initial_stop_loss,
                        max_price_reached, current_volume, is_managed_by_bot,
                        status, closed_date, closed_price, exit_reason
                    ) VALUES (
                        %s, CURRENT_DATE, %s, %s,
                        %s, %s, TRUE,
                        'CLOSED', CURRENT_DATE, %s, %s
                    );
                """, (symbol, final_sell_price, final_sell_price, final_sell_price, volume, final_sell_price, f"{exit_reason} (MANUAL_HOLDING)"))

            conn.commit()
            print(f"[SETTRADE SELL] {symbol} {volume:,} หุ้น @ {final_sell_price:.2f} THB (Order #{broker_order_no})")

            # ซิงค์พอร์ตหุ้นล่าสุดจาก Settrade ทันที เพื่อปรับ current_volume ใน portfolio_stock
            try:
                uport.UpdatePortfolio()
                print(f"[SETTRADE SELL] อัปเดตข้อมูลพอร์ตในฐานข้อมูลเรียบร้อยแล้ว")
            except Exception as p_err:
                print(f"[SETTRADE SELL PORTFOLIO SYNC WARNING] {p_err}")

            return {"success": True, "order_id": order_id, "broker_order_no": broker_order_no, "status": "SENT", "sell_price": final_sell_price}
    except Exception as e:
        conn.rollback()
        print(f"[SETTRADE SELL ERROR] {e}")
        return {"success": False, "error": str(e)}
    finally:
        conn.close()

def cancel_real_order(broker_order_no: str) -> dict:
    """ส่งคำสั่งยกเลิก Order จริงผ่าน Settrade Open API"""
    try:
        investor = Investor(**cfg.args_Investor)
        equity = investor.Equity(account_no=ACCOUNT_NO)
        res = equity.cancel_order(order_no=str(broker_order_no), pin=PIN_ACTION)
        print(f"[SETTRADE CANCEL] ยกเลิก Order #{broker_order_no} สำเร็จ: {res}")
        return {"success": True, "result": res}
    except Exception as e:
        print(f"[SETTRADE CANCEL ERROR] ไม่สามารถยกเลิก Order #{broker_order_no}: {e}")
        return {"success": False, "error": str(e)}
