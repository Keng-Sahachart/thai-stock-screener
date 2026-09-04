'''
Signal Adapters (MACD & Hybrid)
MACD Momentum Adapter (core/signals/macd_signal.py)
ดึงสัญญาณหุ้นที่โมเมนตัมกำลังเร่งตัวและ Volume ยืนยันจาก View v_bot_buy_opportunities:
'''
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Dict, Any
from psycopg2.extras import RealDictCursor
from core.signals.base_signal import BaseSignal

class MacdMomentumSignal(BaseSignal):
    def __init__(self):
        super().__init__(name="MACD_MOMENTUM")

    def scan(self, conn) -> List[Dict[str, Any]]:
        query = """
            SELECT 
                symbol,
                trade_date,
                priority,
                trigger_price,
                atr14,
                reason,
                CASE WHEN priority >= 4 THEN 'BUY-STRONG' ELSE 'BUY' END AS signal_type
            FROM public.v_bot_buy_opportunities
            LIMIT 15;
        """
        results = []
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            rows = cur.fetchall()
            for r in rows:
                results.append({
                    "symbol": r["symbol"],
                    "trade_date": r["trade_date"],
                    "signal_type": r["signal_type"],
                    "signal_source": self.name,
                    "trigger_price": float(r["trigger_price"]),
                    "atr14": float(r["atr14"]) if r["atr14"] else None,
                    "priority": int(r["priority"]),
                    "reason": r["reason"]
                })
        return results