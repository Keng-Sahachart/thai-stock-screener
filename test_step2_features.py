#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
from pathlib import Path

# Add project root to sys.path
root_dir = Path(r"c:\drv_D\GitRepo\thai-stock-screener")
sys.path.insert(0, str(root_dir))

from execution.order_manager import cancel_order, get_db_connection
from psycopg2.extras import RealDictCursor

def run_tests():
    print("--- 1. Testing Cancel Order Functionality ---")
    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Create a dummy test order with status 'SENT'
        cur.execute("""
            INSERT INTO public.bot_orders (
                symbol, side, order_type, volume, target_price,
                status, broker_order_no, created_at
            ) VALUES (
                'TEST_CANCEL', 'BUY', 'LIMIT', 100, 2.50,
                'SENT', 'SIM_CANCEL_TEST_01', timezone('Asia/Bangkok', now())
            ) RETURNING order_id;
        """)
        dummy_order_id = cur.fetchone()["order_id"]
        conn.commit()

    print(f"Created dummy order #{dummy_order_id}")

    # Test cancellation
    cancel_res = cancel_order(dummy_order_id)
    print(f"Cancel result: {cancel_res}")
    assert cancel_res.get("success") is True, f"Cancel should succeed: {cancel_res}"

    # Verify status in database
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT status FROM public.bot_orders WHERE order_id = %s;", (dummy_order_id,))
        new_status = cur.fetchone()["status"]
        print(f"Order #{dummy_order_id} new status: {new_status}")
        assert new_status == "CANCELLED", f"Expected CANCELLED, got {new_status}"

    # Test canceling again (should fail because it's already CANCELLED)
    cancel_again = cancel_order(dummy_order_id)
    print(f"Second cancel result: {cancel_again}")
    assert cancel_again.get("success") is False, "Second cancellation should be rejected!"

    # Test canceling non-existent order
    cancel_nonexistent = cancel_order(999999999)
    print(f"Non-existent cancel result: {cancel_nonexistent}")
    assert cancel_nonexistent.get("success") is False, "Non-existent cancellation should be rejected!"

    # Verify audit log recorded the cancellation
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT log_id, event_type, level, symbol, message 
            FROM public.bot_system_logs 
            WHERE event_type = 'ORDER_CANCELLED' 
            ORDER BY log_id DESC LIMIT 1;
        """)
        audit_log = cur.fetchone()
        print(f"Recorded audit log: {audit_log}")
        assert audit_log is not None, "Audit log for cancellation should exist!"

        # Clean up test order
        cur.execute("DELETE FROM public.bot_orders WHERE order_id = %s;", (dummy_order_id,))
        conn.commit()

    print("\n--- 2. Testing Today Orders Query (cmd_order Query) ---")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT order_id, symbol, side, order_type, volume, target_price,
                   executed_price, status, broker_order_no, created_at, executed_at
            FROM public.bot_orders
            WHERE (created_at::date = CURRENT_DATE OR executed_at::date = CURRENT_DATE)
            ORDER BY order_id DESC;
        """)
        today_orders = cur.fetchall()
        print(f"Successfully queried today orders: found {len(today_orders)} orders.")

    conn.close()
    print("\n✅ All Step 2 Targeted Tests Passed Successfully!")

if __name__ == "__main__":
    run_tests()