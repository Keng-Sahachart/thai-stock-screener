#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : agy_pi5.py
LOCATION    : core/ai/providers/
OCCASION    : เรียกใช้เมื่อ config ตั้งค่า active_provider เป็น "agy_pi5"
DESCRIPTION : Provider สำหรับส่งข้อมูลให้ Antigravity CLI (agy) บน Raspberry Pi 5
              เชื่อมต่อผ่าน Plink (SSH) และรันโหมด Headless CLI
==============================================================================
"""

import os
import sys
import json
import tempfile
import subprocess
from core.ai.base_provider import BaseAIProvider

class AgyPi5Provider(BaseAIProvider):
    def __init__(self, config: dict = None):
        super().__init__(name="agy_pi5", config=config)
        self.host = self.config.get("host", "192.168.1.124")
        self.user = self.config.get("user", "keng")
        self.password = os.getenv("pi_password") or os.getenv("posql_password") or "P@ssw0rd@19"
        self.plink_path = self.config.get("plink_path", "C:\\Program Files\\PuTTY\\plink.exe")
        self.agy_binary = self.config.get("agy_binary", "/home/keng/.local/bin/agy")
        self.timeout_sec = self.config.get("timeout_sec", 60)

    def evaluate(self, payload: dict) -> dict:
        prompt = self.build_prompt(payload)

        # สร้างไฟล์ Prompt ชั่วคราวเพื่อส่งผ่าน Stdin ของ Plink ป้องกัน Escape Character บิดเบือน
        temp_fd, temp_path = tempfile.mkstemp(suffix=".txt", prefix="agy_prompt_")
        try:
            with open(temp_fd, "w", encoding="utf-8") as f:
                f.write(prompt)

            cmd = [
                self.plink_path,
                "-batch",
                "-pw", self.password,
                f"{self.user}@{self.host}",
                f"{self.agy_binary} --output-format json"
            ]

            with open(temp_path, "r", encoding="utf-8") as stdin_f:
                proc = subprocess.run(
                    cmd,
                    stdin=stdin_f,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    timeout=self.timeout_sec
                )

            if proc.returncode != 0:
                err_msg = proc.stderr.strip()
                print(f"[AgyPi5Provider ERROR] Return code {proc.returncode}: {err_msg}")
                return {"error": err_msg, "top_pick": "NONE", "evaluations": []}

            # Parse ผลลัพธ์จาก CLI JSON wrapper
            agy_resp = json.loads(proc.stdout)
            raw_ai_text = agy_resp.get("response", "")
            return self.clean_and_parse_json(raw_ai_text)

        except Exception as e:
            print(f"[AgyPi5Provider EXCEPTION] {e}")
            return {"error": str(e), "top_pick": "NONE", "evaluations": []}
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)
