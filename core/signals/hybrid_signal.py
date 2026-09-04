'''
3.2 Hybrid Selection Adapter (core/signals/hybrid_signal.py)
ดึงหุ้นพื้นฐานดีที่มีคะแนน Value Score สูงและเกิดสัญญาณเทคนิคอลพร้อมกันจาก v_hybrid_stock_selection:
'''

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Dict, Any
from psycopg2.extras import RealDictCursor
from core.signals.base_signal import BaseSignal

class HybridSelectionSignal(BaseSignal):
    def __init__(self):
        super().__init__(name="HYBRID_SELECTION")

    def scan(self, conn) -> List[Dict[str, Any]]:
        query = """
            SELECT 
                h.symbol,
                h.trade_date,
                h.signal_type,
                h.priority,
                h.last_price AS trigger_price,
                i.atr14,
                format('Value Score: %s (Rank %s) | %s', 
                       ROUND(h.value_score::numeric, 2), 
                       h.fundamental_rank, 
                       h.technical_reason) AS reason
            FROM public.v_hybrid_stock_selection h
            LEFT JOIN public.mv_stock_indicators i 
                ON h.symbol = i.symbol AND h.trade_date = i.trade_date
            WHERE h.trade_date = (SELECT MAX(trade_date) FROM public.stock_signal);
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