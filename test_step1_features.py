#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import os
from pathlib import Path

# Add project root to sys.path
root_dir = Path(r"c:\drv_D\GitRepo\thai-stock-screener")
sys.path.insert(0, str(root_dir))

from execution.order_manager import check_daily_budget_limits, get_db_connection, load_config
from core.audit_logger import log_event
from core.signals.aggregator import expire_outdated_signals

def run_tests():
    print("--- 1. Testing Daily Budget Limit Rejection ---")
    conn = get_db_connection()
    cfg = load_config()
    # Test an exorbitant volume that exceeds 8000 THB
    ok, err = check_daily_budget_limits(conn, "TEST_HUGE", 100000, 100.0, cfg)
    print(f"Exorbitant order allowed: {ok}")
    print(f"Error message: {err}")
    assert ok is False, "Exorbitant order should be rejected!"

    print("\n--- 2. Testing Expire Outdated Signals Routine ---")
    with conn.cursor() as cur:
        # Insert a dummy expired signal
        cur.execute("""
            INSERT INTO public.bot_trade_signals (
                symbol, trade_date, signal_type, signal_source, trigger_price,
                stop_loss_plan, recommended_shares, reason, status, priority, expired_at
            ) VALUES (
                'TEST_EXP', CURRENT_DATE, 'BUY', 'TEST', 1.0,
                0.9, 100, 'Test Expire', 'PENDING', 1, timezone('Asia/Bangkok', now()) - INTERVAL '1 hour'
            ) RETURNING id;
        """)
        dummy_id = cur.fetchone()[0]
        conn.commit()
    print(f"Inserted dummy expired signal ID: {dummy_id}")

    # Run expire cleaner
    expire_outdated_signals(conn)

    with conn.cursor() as cur:
        cur.execute("SELECT status FROM public.bot_trade_signals WHERE id = %s;", (dummy_id,))
        status = cur.fetchone()[0]
        print(f"Signal ID {dummy_id} status after expire routine: {status}")
        assert status == 'EXPIRED', f"Signal should be EXPIRED but was {status}"

        # Clean up dummy signal
        cur.execute("DELETE FROM public.bot_trade_signals WHERE id = %s;", (dummy_id,))
        conn.commit()

    print("\n--- 3. Testing Audit Logs Retrieval ---")
    with conn.cursor() as cur:
        cur.execute("SELECT log_id, event_type, level, symbol, message, created_at FROM public.bot_system_logs ORDER BY log_id DESC LIMIT 3;")
        logs = cur.fetchall()
        for l in logs:
            print(f"Log #{l[0]}: [{l[1]}] [{l[2]}] symbol={l[3]} | {l[4]}")

    conn.close()
    print("\n✅ All Step 1 Targeted Tests Passed Successfully!")

if __name__ == "__main__":
    run_tests()