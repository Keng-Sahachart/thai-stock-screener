#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : base_provider.py
LOCATION    : core/ai/
OCCASION    : เป็น Base Class สำหรับทุก AI Provider Adapter
DESCRIPTION : นิยาม Interface `BaseAIProvider` และฟังก์ชันสร้าง Prompt / Parse JSON
==============================================================================
"""

import json
from abc import ABC, abstractmethod

class BaseAIProvider(ABC):
    def __init__(self, name: str, config: dict = None):
        self.name = name
        self.config = config or {}

    def build_prompt(self, payload: dict) -> str:
        """สร้าง Universal Prompt แบบ 3 มิติ (Momentum Trap, VI Quality, Swing Risk)"""
        return f"""คุณคือหัวหน้านักวิเคราะห์การลงทุนและ Portfolio Manager หุ้นไทย
โปรดวิเคราะห์ข้อมูลหุ้นที่ผ่านระบบสแกนประจำวัน ({payload.get('market_date')}) เพื่อตัดสินใจเข้าซื้อ:

[ข้อมูลผู้สมัครและประวัติย้อนหลัง 10 วัน]:
{json.dumps(payload, ensure_ascii=False, indent=2)}

[ภารกิจของคุณ - วิเคราะห์ 3 มิติพร้อมกันในทุกตัว]:
มิติที่ 1: ตรวจสอบพฤติกรรมราคา (ขึ้นจริง หรือ ขึ้นหลอก / Trap Check):
  - แท่งเทียน D0 มี Upper Shadow (ไส้บนยาว) หรือโดนเทขายปิดลบหรือไม่? ถ้าใช่ = ส่อแววขึ้นหลอก/ออกของ (BULL_TRAP_SUSPECT)
  - ปริมาณการซื้อขาย (Volume) เข้าสม่ำเสมอยืนเหนือ EMA5/EMA20 หรือเข้าแค่วันเดียว?
มิติที่ 2: ตรวจสอบคุณภาพและงบการเงิน (VI Quality Check):
  - หนี้สินต่อทุน (D/E): ถือว่าปลอดภัย (< 1.0 - 1.5) หรือเสี่ยงหนี้ท่วม (> 2.0)?
  - ความสามารถทำกำไร: Net Profit Margin (NPM) สูงและ ROE ดี หรือมาร์จิ้นบางเฉียบ?
  - ให้เกรด VI: GRADE_A (พื้นฐานแกร่ง), GRADE_B (พอใช้), หรือ REJECT (เสี่ยงสูง)
มิติที่ 3: ความคุ้มค่าทางเทคนิคและจุดตัดขาดทุน (Swing Risk/Reward):
  - แผน Stop Loss (4.0% - 5.5%) และความคุ้มค่าเทียบกับแนวต้านถัดไป

[เกณฑ์การตัดสินใจ]:
- ให้คะแนน Confidence Score (0 ถึง 100) ทุกตัว
- ตัวที่ผ่านเกณฑ์ซื้อ (Decision = BUY) ต้องมี Confidence Score >= 80
- จัดอันดับ และเลือก 'Top Pick' เพียง 1 ตัวที่ดีที่สุด (หากไม่มีตัวใดผ่านเกณฑ์ ให้ระบุ "NONE")

ตอบกลับเป็น JSON เท่านั้น (Strict JSON) ตามโครงสร้างนี้:
{{
  "top_pick": "ชื่อย่อหุ้น หรือ NONE",
  "comparison_summary": "สรุปเปรียบเทียบภาพรวมสั้นๆ 2-3 ประโยค",
  "evaluations": [
    {{
      "symbol": "SYMBOL",
      "decision": "BUY หรือ PASS",
      "confidence_score": 0 ถึง 100,
      "momentum_trap_check": {{
        "verdict": "GENUINE_MARKUP หรือ BULL_TRAP_SUSPECT",
        "summary": "ชี้ขาดสั้นๆ ว่าขึ้นจริงหรือขึ้นหลอกเพราะอะไร"
      }},
      "vi_quality_check": {{
        "grade": "GRADE_A, GRADE_B หรือ REJECT",
        "summary": "สรุปจุดเด่น/จุดเสี่ยงเรื่องหนี้สินและกำไร"
      }},
      "swing_risk_reward": {{
        "verdict": "WORTH_RISK หรือ HIGH_RISK",
        "summary": "ความคุ้มค่าของจุด Stop Loss และ Risk %"
      }},
      "pros": ["จุดเด่นสำคัญ 1-2 ข้อ"],
      "risks": ["จุดเสี่ยงที่ต้องระวัง 1-2 ข้อ"],
      "verdict": "บทวิเคราะห์สรุปสั้น 1 ประโยค"
    }}
  ]
}}
"""

    def clean_and_parse_json(self, raw_text: str) -> dict:
        """ตัด Markdown Backticks ```json ... ``` ออกเพื่อแปลงเป็น Dict อย่างปลอดภัย"""
        if not raw_text:
            return {}
        text = raw_text.strip()
        if text.startswith("```json"):
            text = text[7:]
        elif text.startswith("```"):
            text = text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
        return json.loads(text)

    @abstractmethod
    def evaluate(self, payload: dict) -> dict:
        """ฟังก์ชันหลักที่ Subclass ต้อง Implement เพื่อส่ง Prompt ไปยัง Provider นั้นๆ"""
        pass
