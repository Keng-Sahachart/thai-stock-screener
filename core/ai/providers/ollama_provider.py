#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : ollama_provider.py
LOCATION    : core/ai/providers/
OCCASION    : เรียกใช้เมื่อ config ตั้งค่า active_provider เป็น "local_ollama"
DESCRIPTION : Provider สำหรับส่งข้อมูลให้ Local AI Server ผ่าน Ollama REST API
              รองรับทั้ง Ollama ที่รันบน Pi 5 หรือรันบนเครื่อง PC (พอร์ต 11434)
==============================================================================
"""

import os
import json
import urllib.request
import urllib.error
from core.ai.base_provider import BaseAIProvider

class OllamaProvider(BaseAIProvider):
    def __init__(self, config: dict = None):
        super().__init__(name="local_ollama", config=config)
        self.endpoint = self.config.get("endpoint", "http://192.168.1.124:11434")
        self.model = self.config.get("model", "qwen2.5:1.5b")
        self.timeout_sec = self.config.get("timeout_sec", 120)

    def evaluate(self, payload: dict) -> dict:
        prompt = self.build_prompt(payload)
        url = f"{self.endpoint.rstrip('/')}/api/generate"

        request_body = {
            "model": self.model,
            "prompt": prompt,
            "format": "json",
            "stream": False,
            "options": {
                "temperature": 0.2
            }
        }

        req = urllib.request.Request(
            url,
            data=json.dumps(request_body).encode("utf-8"),
            headers={"Content-Type": "application/json"}
        )

        try:
            with urllib.request.urlopen(req, timeout=self.timeout_sec) as response:
                result = json.loads(response.read().decode("utf-8"))
                raw_text = result.get("response", "")
                return self.clean_and_parse_json(raw_text)
        except urllib.error.URLError as e:
            err = f"ไม่สามารถเชื่อมต่อไปยัง Ollama ที่ {self.endpoint}: {e.reason}"
            print(f"[OllamaProvider ERROR] {err}")
            return {"error": err, "top_pick": "NONE", "evaluations": []}
        except Exception as e:
            print(f"[OllamaProvider EXCEPTION] {e}")
            return {"error": str(e), "top_pick": "NONE", "evaluations": []}
