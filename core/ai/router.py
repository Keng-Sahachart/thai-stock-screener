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

def create_provider_by_name(provider_name: str, config: dict = None) -> BaseAIProvider:
    """Factory Function สำหรับสร้าง Instance ของ AI Provider ตามชื่อที่ระบุ"""
    if config is None:
        config = load_config()

    ai_cfg = config.get("ai_evaluation", {})
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

def get_ai_provider(config: dict = None) -> BaseAIProvider:
    """Factory Function สำหรับสร้าง Instance ของ Primary AI Provider ตามที่กำหนดใน config"""
    if config is None:
        config = load_config()
    provider_name = config.get("ai_evaluation", {}).get("active_provider", "agy_pi5").lower()
    return create_provider_by_name(provider_name, config)

def get_fallback_provider(config: dict = None) -> BaseAIProvider:
    """สร้าง Instance ของ Fallback AI Provider หากกำหนดไว้ใน config"""
    if config is None:
        config = load_config()
    fallback_name = config.get("ai_evaluation", {}).get("fallback_provider")
    if not fallback_name:
        return None
    return create_provider_by_name(fallback_name.lower(), config)

def get_active_provider_name() -> str:
    config = load_config()
    return config.get("ai_evaluation", {}).get("active_provider", "agy_pi5").lower()

def get_ai_quota_status() -> dict:
    """ตรวจสอบโควตาและสถานะการเชื่อมต่อ (รองรับเฉพาะ Provider agy_pi5)"""
    config = load_config()
    ai_cfg = config.get("ai_evaluation", {})
    provider_name = ai_cfg.get("active_provider", "agy_pi5").lower()
    if provider_name != "agy_pi5":
        return {
            "supported": False,
            "active_provider": provider_name,
            "message": f"คำสั่งนี้รองรับเฉพาะเมื่อใช้งาน Provider 'agy_pi5' บน Pi 5 (ปัจจุบัน: '{provider_name}')"
        }
    provider = get_ai_provider(config)
    if hasattr(provider, "get_quota_status"):
        res = provider.get_quota_status()
        res["supported"] = True
        res["active_provider"] = provider_name
        return res
    return {
        "supported": False,
        "active_provider": provider_name,
        "message": "Provider ปัจจุบันไม่มีฟังก์ชันตรวจสอบโควตา"
    }

def evaluate_candidates(symbols: list = None, target_date: str = None, conn = None) -> dict:
    """
    ฟังก์ชันหลักในการประเมินหุ้น:
    1. ดึงและจัดเตรียมข้อมูลดิบ (Payload Builder)
    2. ส่งให้ Provider ปัจจุบันประเมิน (AI Routing Layer)
    3. หากเกิด Error หรือ High Traffic จะสลับไป Fallback Provider อัตโนมัติ (Self-Healing Fallback)
    4. ส่งคืนผลลัพธ์พร้อมข้อมูล Payload อ้างอิง
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

        # ขั้นที่ 2: สั่งงานผ่าน Primary Provider ที่เลือก (AI Routing Layer)
        provider = get_ai_provider(config)
        print(f"🤖 [AI Router] ส่งหุ้น {len(payload['candidates'])} ตัวให้ Provider: '{provider.name}'...")
        
        result = None
        try:
            result = provider.evaluate(payload)
        except Exception as e:
            print(f"❌ [AI Router ERROR] Exception จาก Primary Provider '{provider.name}': {e}")
            result = {"error": str(e), "status": "ERROR", "top_pick": "NONE", "evaluations": []}

        # ตรวจสอบว่าเกิด Error หรือไม่ เพื่อสลับไป Fallback Provider อัตโนมัติ
        has_error = (
            not result
            or result.get("status") == "ERROR"
            or bool(result.get("error"))
            or not result.get("evaluations")
        )

        fallback_provider = get_fallback_provider(config)
        if has_error and fallback_provider and fallback_provider.name != provider.name:
            orig_error = result.get("error") if result else "No response"
            model_info = getattr(fallback_provider, "model", fallback_provider.name)
            print(f"⚠️ [AI Router FALLBACK] Primary Provider '{provider.name}' ขัดข้อง ({orig_error})")
            print(f"🔄 [AI Router FALLBACK] กำลังสลับไปใช้ Fallback Provider: '{fallback_provider.name}' (Model: {model_info})...")

            try:
                fb_result = fallback_provider.evaluate(payload)
                if fb_result and not fb_result.get("error") and fb_result.get("evaluations"):
                    print(f"✅ [AI Router FALLBACK] Fallback ไปยัง '{fallback_provider.name}' สำเร็จเรียบร้อย!")
                    result = fb_result
                    result["fallback_used"] = True
                    result["primary_provider"] = provider.name
                    result["primary_error"] = orig_error
                    result["provider_used"] = f"{fallback_provider.name} ({model_info})"
                else:
                    fb_err = fb_result.get("error") if fb_result else "No fallback response"
                    print(f"❌ [AI Router FALLBACK] Fallback '{fallback_provider.name}' ขัดข้องเช่นกัน: {fb_err}")
                    result["fallback_error"] = fb_err
            except Exception as fe:
                print(f"❌ [AI Router FALLBACK] Exception จาก Fallback Provider: {fe}")
                result["fallback_error"] = str(fe)

        if not result.get("provider_used"):
            result["provider_used"] = provider.name

        # บรรจุข้อมูล Meta เสริม
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
