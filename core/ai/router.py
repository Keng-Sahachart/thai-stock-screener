#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : router.py
LOCATION    : core/ai/
OCCASION    : จุดศูนย์กลางการเรียกใช้งาน AI สำหรับทั้งระบบ (Single Entrypoint)
DESCRIPTION : AI Router: สลับและกระจายงานไปยัง AI Provider ที่กำหนดใน bot_config.json
              - agy_pi5     : Antigravity CLI บน Raspberry Pi 5 (Active ค่าเริ่มต้น)
              - gemini_api  : Google Gemini Flash / Pro REST API
              - local_ollama: Local SLM / LLM ผ่าน Ollama HTTP API
==============================================================================
"""

import os
import sys
import json
import psycopg2
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from core.ai.payload_builder import build_candidate_payload, get_db_connection
from core.ai.base_provider import BaseAIProvider
from core.ai.providers.agy_pi5 import AgyPi5Provider
from core.ai.providers.gemini_api import GeminiApiProvider
from core.ai.providers.ollama_provider import OllamaProvider

CONFIG_PATH = ROOT_DIR / "config" / "bot_config.json"

def load_config() -> dict:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[AI Router WARN] ไม่สามารถโหลด bot_config.json ได้: {e}")
        return {}

def get_ai_provider(config: dict = None) -> BaseAIProvider:
    """Factory Function สำหรับสร้าง Instance ของ AI Provider ตามที่กำหนดใน config"""
    if config is None:
        config = load_config()

    ai_cfg = config.get("ai_evaluation", {})
    provider_name = ai_cfg.get("active_provider", "agy_pi5").lower()
    providers_cfg = ai_cfg.get("providers", {})
    specific_cfg = providers_cfg.get(provider_name, {})

    if provider_name == "agy_pi5":
        return AgyPi5Provider(config=specific_cfg)
    elif provider_name == "gemini_api":
        return GeminiApiProvider(config=specific_cfg)
    elif provider_name in ("local_ollama", "ollama"):
        return OllamaProvider(config=specific_cfg)
    else:
        print(f"[AI Router WARN] ไม่รู้จัก Provider '{provider_name}', สลับกลับมาใช้ agy_pi5")
        return AgyPi5Provider(config=providers_cfg.get("agy_pi5", {}))

def evaluate_candidates(symbols: list = None, target_date: str = None, conn = None) -> dict:
    """
    ฟังก์ชันหลักในการประเมินหุ้น:
    1. ดึงและจัดเตรียมข้อมูลดิบ (Payload Builder)
    2. ส่งให้ Provider ปัจจุบันประเมิน (AI Evaluation)
    3. ส่งคืนผลลัพธ์พร้อมข้อมูล Payload อ้างอิง
    """
    config = load_config()
    ai_cfg = config.get("ai_evaluation", {})

    if not ai_cfg.get("enabled", True):
        return {
            "status": "DISABLED",
            "message": "AI Evaluation ปิดการใช้งานอยู่ใน bot_config.json",
            "top_pick": "NONE",
            "evaluations": []
        }

    close_conn_at_end = False
    if conn is None:
        conn = get_db_connection()
        close_conn_at_end = True

    try:
        max_cands = ai_cfg.get("max_candidates_to_analyze", 10)
        # ขั้นที่ 1: เตรียม Payload (Data Preparation Layer)
        payload = build_candidate_payload(
            conn=conn,
            symbols=symbols,
            target_date=target_date,
            max_candidates=max_cands,
            config=config
        )

        if not payload.get("candidates"):
            return {
                "status": "EMPTY",
                "message": "ไม่มีหุ้นที่ผ่านเกณฑ์หรือไม่มีข้อมูลในวันที่ระบุ",
                "top_pick": "NONE",
                "evaluations": []
            }

        # ขั้นที่ 2: สั่งงานผ่าน Provider ที่เลือก (AI Routing Layer)
        provider = get_ai_provider(config)
        print(f"🤖 [AI Router] ส่งหุ้น {len(payload['candidates'])} ตัวให้ Provider: '{provider.name}'...")
        result = provider.evaluate(payload)

        # บรรจุข้อมูล Meta เสริม
        result["provider_used"] = provider.name
        result["market_date"] = payload.get("market_date")
        result["candidates_analyzed"] = [c["symbol"] for c in payload.get("candidates", [])]
        return result

    finally:
        if close_conn_at_end:
            conn.close()

if __name__ == "__main__":
    import sys
    test_symbols = sys.argv[1].split(",") if len(sys.argv) > 1 else None
    res = evaluate_candidates(symbols=test_symbols)
    print("\n[AI ROUTER EVALUATION RESULT]:")
    print(json.dumps(res, ensure_ascii=False, indent=2))
