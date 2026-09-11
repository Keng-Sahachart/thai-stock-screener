#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : test_system_suite.py
LOCATION    : Root directory
OCCASION    : รันทุกครั้งหลัง Git pull บน Raspberry Pi หรือหลังแก้ไขโค้ดโครงสร้าง
DESCRIPTION : ตรวจสอบความพร้อมของระบบทั้งหมดก่อนรันงานจริง:
              1. ตรวจสอบ Dependencies และตัวแปรใน .env
              2. ตรวจสอบการเชื่อมต่อ Database, Tables และ Views
              3. ตรวจสอบการ Import โมดูล (ป้องกัน Case-sensitive mismatch บน Linux)
              4. ทดสอบ Render กราฟในโหมด Headless (Agg)
              5. ทดสอบฟังก์ชันคุมความเสี่ยงและ Order Manager (Dry-Run)
==============================================================================
"""

import os
import sys
import importlib
from datetime import datetime
from dotenv import load_dotenv

if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

load_dotenv()

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"

results = []

def record_result(category: str, item: str, status: bool, note: str = ""):
    results.append({
        "category": category,
        "item": item,
        "status": "PASS" if status else "FAIL",
        "note": note
    })
    badge = f"{GREEN}[PASS]{RESET}" if status else f"{RED}[FAIL]{RESET}"
    print(f" {badge} {category} -> {item} {f'({note})' if note else ''}")

# ==============================================================================
# 1. TEST PYTHON PACKAGES
# ==============================================================================
def test_packages():
    print(f"\n{YELLOW}=== 1. Checking Required Python Packages ==={RESET}")
    required_packages = [
        ("psycopg2", "psycopg2"),
        ("pandas", "pandas"),
        ("numpy", "numpy"),
        ("matplotlib", "matplotlib"),
        ("mplfinance", "mplfinance"),
        ("telegram", "python-telegram-bot"),
        ("sqlalchemy", "sqlalchemy"),
        ("dotenv", "python-dotenv"),
        ("settrade_v2", "settrade-v2")
    ]
    for mod, pkg_name in required_packages:
        try:
            importlib.import_module(mod)
            record_result("Dependencies", pkg_name, True)
        except ImportError as e:
            record_result("Dependencies", pkg_name, False, f"Missing: pip install {pkg_name} ({e})")

# ==============================================================================
# 2. TEST ENVIRONMENT VARIABLES (.env)
# ==============================================================================
def test_env_variables():
    print(f"\n{YELLOW}=== 2. Checking .env Configurations ==={RESET}")
    required_keys = [
        "posql_host", "posql_port", "posql_db", "posql_user", "posql_password",
        "app_id", "app_secret", "account_no",
        "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"
    ]
    for key in required_keys:
        val = os.getenv(key)
        if val and val.strip():
            # ซ่อนข้อมูลสำคัญบางส่วนในการแสดงผล
            masked = val[:3] + "..." + val[-2:] if len(val) > 6 else "***"
            record_result("Environment", key, True, f"Value: {masked}")
        else:
            record_result("Environment", key, False, "Not set or empty in .env")

# ==============================================================================
# 3. TEST DATABASE CONNECTION & SCHEMA
# ==============================================================================
def test_database():
    print(f"\n{YELLOW}=== 3. Checking Database Connection, Tables & Views ==={RESET}")
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv("posql_host", "localhost"),
            port=os.getenv("posql_port", "5432"),
            dbname=os.getenv("posql_db", "stocks"),
            user=os.getenv("posql_user", "postgres"),
            password=os.getenv("posql_password", "postgres")
        )
        record_result("Database", "PostgreSQL Connection", True)
    except Exception as e:
        record_result("Database", "PostgreSQL Connection", False, str(e))
        return

    required_tables = [
        "portfolio_stock", "stock_price_history", "stock_indicator_jsonb",
        "master_stock_classification", "account_info_history",
        "bot_active_positions", "bot_trade_signals", "bot_orders",
        "bot_system_logs"
    ]
    required_views = [
        "mv_stock_indicators", "v_portfolio_with_signals",
        "v_macd_advanced_analysis", "v_bot_buy_opportunities", "v_bot_sell_triggers"
    ]

    with conn.cursor() as cur:
        # Check Tables
        for tbl in required_tables:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_name = %s
                );
            """, (tbl,))
            exists = cur.fetchone()[0]
            record_result("DB Table", tbl, exists, "Found" if exists else "Missing Table DDL")

        # Check Views & Materialized Views
        for vw in required_views:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM pg_matviews WHERE matviewname = %s
                ) OR EXISTS (
                    SELECT FROM information_schema.views WHERE table_schema = 'public' AND table_name = %s
                );
            """, (vw, vw))
            exists = cur.fetchone()[0]
            record_result("DB View", vw, exists, "Found" if exists else "Missing View DDL")

    conn.close()

# ==============================================================================
# 4. TEST MODULE IMPORTS (Linux Case-Sensitivity Check)
# ==============================================================================
def test_module_imports():
    print(f"\n{YELLOW}=== 4. Checking Project Module Imports ==={RESET}")
    modules = [
        "initialApp",
        "risk_manager",
        "core.audit_logger",
        "update.updateStockPrice",
        "update.updatePort",
        "update.update_Port_info",
        "indicators.compute_indicators_v5",
        "core.signals.base_signal",
        "core.signals.macd_signal",
        "core.signals.hybrid_signal",
        "core.signals.aggregator",
        "core.position_tracker",
        "execution.order_manager",
        "execution.dry_run_executor",
        "bot.chart_generator",
        "jobs.run_buy_scanner",
        "jobs.run_sell_monitor",
        "jobs.run_eod_sync"
    ]
    for mod in modules:
        try:
            importlib.import_module(mod)
            record_result("Module Import", mod, True)
        except Exception as e:
            record_result("Module Import", mod, False, f"Import Error: {e}")

# ==============================================================================
# 5. TEST HEADLESS CHART GENERATION (Matplotlib / Pi OS test)
# ==============================================================================
def test_chart_engine():
    print(f"\n{YELLOW}=== 5. Testing Chart Generator in Headless Environment ==={RESET}")
    try:
        from bot.chart_generator import generate_stock_chart
        import psycopg2
        
        conn = psycopg2.connect(
            host=os.getenv("posql_host", "localhost"),
            port=os.getenv("posql_port", "5432"),
            dbname=os.getenv("posql_db", "stocks"),
            user=os.getenv("posql_user", "postgres"),
            password=os.getenv("posql_password", "postgres")
        )
        with conn.cursor() as cur:
            cur.execute("SELECT symbol FROM public.stock_price_history ORDER BY date DESC LIMIT 1;")
            row = cur.fetchone()
            sample_sym = row[0] if row else "TDEX"
        conn.close()

        buf = generate_stock_chart(sample_sym, lookback_days=30)
        if buf and buf.getbuffer().nbytes > 1000:
            record_result("Graphics", f"Chart Render ({sample_sym})", True, f"Image size: {buf.getbuffer().nbytes:,} bytes")
        else:
            record_result("Graphics", f"Chart Render ({sample_sym})", False, "Generated empty buffer")
    except Exception as e:
        record_result("Graphics", "Chart Generator Engine", False, f"Crash: {e}")

# ==============================================================================
# 6. TEST RISK LOGIC & DRY RUN ENGINE
# ==============================================================================
def test_core_logic():
    print(f"\n{YELLOW}=== 6. Testing Risk Management & Sizing Logic ==={RESET}")
    try:
        import risk_manager as rm
        shares, sl = rm.calculate_position_size(
            symbol="TEST",
            entry_price=2.0,
            atr14=0.1,
            total_equity=100000.0,
            available_cash=5000.0,
            risk_percent=0.01,
            max_amount_per_trade=500.0,
            max_shares_per_trade=200
        )
        # ตรวจสอบการปัดเศษ Lot Size 100
        is_lot_valid = (shares % 100 == 0) and (shares > 0)
        record_result("Core Logic", "Position Sizing Clamp", is_lot_valid, f"Shares: {shares}, SL: {sl}")
    except Exception as e:
        record_result("Core Logic", "Position Sizing Formula", False, str(e))

    try:
        from core.audit_logger import log_event, ensure_log_table
        ensured = ensure_log_table()
        logged = log_event("DIAGNOSTIC_TEST", "ทดสอบการบันทึก Audit Log จาก Test Suite", level="INFO")
        record_result("Core Logic", "Audit Logger Functionality", ensured and logged, "Logged successfully")
    except Exception as e:
        record_result("Core Logic", "Audit Logger Functionality", False, str(e))

    try:
        from execution.order_manager import check_daily_budget_limits, get_db_connection, load_config
        conn = get_db_connection()
        cfg = load_config()
        # ทดสอบกรณีงบปกติ
        ok, err = check_daily_budget_limits(conn, "TEST_SYM", 100, 1.0, cfg)
        conn.close()
        record_result("Core Logic", "Daily Budget Limits Check", isinstance(ok, bool), f"Passed check (allowed={ok})")
    except Exception as e:
        record_result("Core Logic", "Daily Budget Limits Check", False, str(e))

# ==============================================================================
# SUMMARY SCOREBOARD
# ==============================================================================
def main():
    print(f"\n{GREEN}======================================================{RESET}")
    print(f"🚀  RUNNING DIAGNOSTIC TEST SUITE (OS: {sys.platform})")
    print(f"    Date/Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{GREEN}======================================================{RESET}")

    test_packages()
    test_env_variables()
    test_database()
    test_module_imports()
    test_chart_engine()
    test_core_logic()

    total = len(results)
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = total - passed

    print(f"\n{YELLOW}======================================================{RESET}")
    print(f"📊  FINAL TEST SUMMARY")
    print(f"    Total Checks : {total}")
    print(f"    {GREEN}Passed       : {passed}{RESET}")
    print(f"    {RED}Failed       : {failed}{RESET}")
    print(f"{YELLOW}======================================================{RESET}")

    if failed > 0:
        print(f"\n{RED}❌ พบข้อผิดพลาด {failed} รายการที่ต้องแก้ไขบนเครื่องนี้:{RESET}")
        for r in results:
            if r["status"] == "FAIL":
                print(f"  • [{r['category']}] {r['item']}: {r['note']}")
        sys.exit(1)
    else:
        print(f"\n{GREEN}🎉 ระบบทั้งหมดผ่านการทดสอบ 100% พร้อมใช้งานบน {sys.platform}!{RESET}\n")
        sys.exit(0)

if __name__ == "__main__":
    main()