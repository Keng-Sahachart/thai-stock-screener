#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : gemini_api.py
LOCATION    : core/ai/providers/
OCCASION    : เรียกใช้เมื่อ config ตั้งค่า active_provider เป็น "gemini_api"
DESCRIPTION : Provider สำหรับส่งข้อมูลให้ Google Gemini API (Free Tier / Pro) โดยตรง
              ผ่าน Official REST API (ไม่ต้องลงแพ็กเกจเพิ่ม ใช้ standard library)
==============================================================================
"""

import os
import json
import urllib.request
import urllib.error
from core.ai.base_provider import BaseAIProvider

class GeminiApiProvider(BaseAIProvider):
    def __init__(self, config: dict = None):
        super().__init__(name="gemini_api", config=config)
        self.model = self.config.get("model", "gemini-2.0-flash")
        api_key_var = self.config.get("api_key_env", "GEMINI_API_KEY")
        self.api_key = os.getenv(api_key_var) or self.config.get("api_key", "")

    def evaluate(self, payload: dict) -> dict:
        if not self.api_key:
            err = "ไม่พบ GEMINI_API_KEY ใน .env หรือ bot_config.json โปรดระบุคีย์เพื่อใช้งาน"
            print(f"[GeminiApiProvider ERROR] {err}")
            return {"error": err, "top_pick": "NONE", "evaluations": []}

        prompt = self.build_prompt(payload)
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent?key={self.api_key}"

        request_body = {
            "contents": [
                {
                    "parts": [{"text": prompt}]
                }
            ],
            "generationConfig": {
                "response_mime_type": "application/json",
                "temperature": 0.2
            }
        }

        req = urllib.request.Request(
            url,
            data=json.dumps(request_body).encode("utf-8"),
            headers={"Content-Type": "application/json"}
        )

        try:
            with urllib.request.urlopen(req, timeout=45) as response:
                result = json.loads(response.read().decode("utf-8"))
                candidates = result.get("candidates", [])
                if candidates:
                    raw_text = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                    return self.clean_and_parse_json(raw_text)
                return {"error": "No candidates returned from Gemini API", "top_pick": "NONE", "evaluations": []}
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8")
            print(f"[GeminiApiProvider HTTP ERROR] {e.code}: {err_body}")
            return {"error": f"HTTP {e.code}: {err_body}", "top_pick": "NONE", "evaluations": []}
        except Exception as e:
            print(f"[GeminiApiProvider EXCEPTION] {e}")
            return {"error": str(e), "top_pick": "NONE", "evaluations": []}
