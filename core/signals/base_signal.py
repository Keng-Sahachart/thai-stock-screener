'''
Interface สัญญาณหลัก (core/signals/base_signal.py)
กำหนด Base Class เพื่อให้ทุกตัวสร้างสัญญาณ (Signal Engines) คืนค่าข้อมูลในโครงสร้างมาตรฐานเดียวกัน ทำให้เพิ่ม Indicator ใหม่ในอนาคตได้ทันที:
'''
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from abc import ABC, abstractmethod
from typing import List, Dict, Any

class BaseSignal(ABC):
    """Abstract Base Class สำหรับ Signal Engine ทุกประเภท"""
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def scan(self, conn) -> List[Dict[str, Any]]:
        """
        สแกนหาสัญญาณซื้อ
        ต้องคืนค่า list ของ Dict ที่มี keys ดังนี้:
        - symbol (str)
        - trade_date (date)
        - signal_type (str: 'BUY', 'BUY-STRONG')
        - signal_source (str)
        - trigger_price (float)
        - atr14 (float or None)
        - priority (int)
        - reason (str)
        """
        pass