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
import time
import tempfile
import datetime
import subprocess
import urllib.request
import sqlite3
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

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

            # ตรวจสอบว่ากำลังรันอยู่บน Pi 5 (Linux) หรือรันจากเครื่อง Windows Dev
            is_running_locally_on_pi = (sys.platform != "win32") or os.path.exists(self.agy_binary)

            if is_running_locally_on_pi:
                # รันอยู่บน Pi 5 โดยตรง: เรียก agy ในเครื่องได้ทันที ไม่ต้องผ่าน Plink/SSH
                cmd = [self.agy_binary, "--output-format", "json"]
            else:
                # รันอยู่บน Windows Dev PC: สั่งงานข้ามเครื่องไปยัง Pi 5 ผ่าน Plink
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

            # บันทึก usage สถิติรอบวิเคราะห์ล่าสุด
            try:
                usage_info = {
                    "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "duration_seconds": agy_resp.get("duration_seconds", 0),
                    "usage": agy_resp.get("usage", {})
                }
                usage_path = ROOT_DIR / "logs" / "last_agy_usage.json"
                usage_path.parent.mkdir(parents=True, exist_ok=True)
                with open(usage_path, "w", encoding="utf-8") as uf:
                    json.dump(usage_info, uf, ensure_ascii=False, indent=2)
            except Exception as ue:
                print(f"[AgyPi5Provider WARN] ไม่สามารถบันทึก usage stat: {ue}")

            return self.clean_and_parse_json(raw_ai_text)

        except Exception as e:
            print(f"[AgyPi5Provider EXCEPTION] {e}")
            return {"error": str(e), "top_pick": "NONE", "evaluations": []}
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def get_quota_status(self) -> dict:
        """
        ตรวจสอบสถานะการเชื่อมต่อ เครดิต/โควตา บัญชีผู้ใช้ และสถิติการใช้งานของ agy บน Pi 5
        รองรับการเรียกใช้งานทั้งจาก Pi 5 โดยตรง (Production) และจาก Windows Dev PC (ผ่าน Plink)
        """
        is_running_locally_on_pi = (sys.platform != "win32") or os.path.exists(self.agy_binary)
        if is_running_locally_on_pi:
            return self._fetch_local_status()
        else:
            return self._fetch_remote_status()

    def _fetch_local_status(self) -> dict:
        """ประมวลผลดึงสถานะโดยตรงจาก Local File และ CloudCode API บน Pi 5"""
        status_info = {
            "provider": "agy_pi5",
            "status": "OFFLINE",
            "account": "kengnex@gmail.com",
            "tier": "Gemini Code Assist (Standard Tier)",
            "tier_desc": "Unlimited coding & analysis assistant with Gemini models",
            "model": "Gemini 3.8 Flash (High)",
            "cloudcode_connected": False,
            "latency_ms": 0,
            "token_valid": False,
            "token_expiry": None,
            "token_expiry_formatted": "-",
            "today_requests": 0,
            "total_requests": 0,
            "last_request_time": "-",
            "last_request_title": "-",
            "last_usage": None
        }

        # 1. ตรวจสอบ OAuth Token
        token_file = os.path.expanduser("~/.gemini/antigravity-cli/antigravity-oauth-token")
        access_token = None
        if os.path.exists(token_file):
            try:
                with open(token_file, "r", encoding="utf-8") as tf:
                    tdata = json.load(tf)
                tok_info = tdata.get("token", {})
                access_token = tok_info.get("access_token")
                expiry_str = tok_info.get("expiry")
                status_info["token_expiry"] = expiry_str
                status_info["token_expiry_formatted"] = self._format_thai_time(expiry_str)
                status_info["token_valid"] = bool(access_token)
            except Exception as e:
                status_info["token_error"] = str(e)

        # 2. ทดสอบเชื่อมต่อ Google CloudCode loadCodeAssist
        if access_token:
            try:
                t0 = time.time()
                req = urllib.request.Request(
                    "https://daily-cloudcode-pa.googleapis.com/v1internal:loadCodeAssist",
                    headers={
                        "Authorization": f"Bearer {access_token}",
                        "Content-Type": "application/json"
                    },
                    data=b"{}"
                )
                with urllib.request.urlopen(req, timeout=5) as resp:
                    cdata = json.loads(resp.read().decode())
                    t1 = time.time()
                    status_info["latency_ms"] = int((t1 - t0) * 1000)
                    status_info["cloudcode_connected"] = True
                    status_info["status"] = "ONLINE"
                    
                    allowed = cdata.get("allowedTiers", [])
                    if allowed:
                        status_info["tier"] = allowed[0].get("name", status_info["tier"])
                        status_info["tier_desc"] = allowed[0].get("description", status_info["tier_desc"])
            except Exception as ce:
                status_info["cloudcode_error"] = str(ce)
                status_info["status"] = "ERROR"

        # 3. ดึงสถิติคำสั่งสะสมจาก conversation_summaries.db
        db_path = os.path.expanduser("~/.gemini/antigravity-cli/conversation_summaries.db")
        if os.path.exists(db_path):
            try:
                conn = sqlite3.connect(db_path)
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM conversation_summaries")
                status_info["total_requests"] = cur.fetchone()[0]

                today_prefix = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
                cur.execute("SELECT COUNT(*) FROM conversation_summaries WHERE last_modified_time LIKE ?", (f"{today_prefix}%",))
                status_info["today_requests"] = cur.fetchone()[0]

                cur.execute("SELECT last_modified_time, title FROM conversation_summaries ORDER BY last_modified_time DESC LIMIT 1")
                row = cur.fetchone()
                if row:
                    status_info["last_request_time"] = self._format_thai_time(row[0])
                    status_info["last_request_title"] = row[1]
                conn.close()
            except Exception as de:
                status_info["db_error"] = str(de)

        # 4. ดึงสถิติ Token ของรอบล่าสุดจาก logs/last_agy_usage.json
        usage_path = ROOT_DIR / "logs" / "last_agy_usage.json"
        if usage_path.exists():
            try:
                with open(usage_path, "r", encoding="utf-8") as uf:
                    status_info["last_usage"] = json.load(uf)
            except Exception:
                pass

        return status_info

    def _fetch_remote_status(self) -> dict:
        """สั่งรันบน Pi 5 ผ่าน SSH (Plink) เพื่อดึงผลลัพธ์ JSON สำหรับกรณีเรียกบน Windows Dev"""
        try:
            remote_py = "/home/keng/thai-stock-screener/thai-stock-screener/.venv/bin/python"
            remote_repo = "/home/keng/thai-stock-screener/thai-stock-screener"
            cmd = [
                self.plink_path,
                "-batch",
                "-pw", self.password,
                f"{self.user}@{self.host}",
                f"cd {remote_repo} && {remote_py} -m core.ai.providers.agy_pi5 --status"
            ]
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=15
            )
            if proc.returncode == 0 and proc.stdout.strip():
                return json.loads(proc.stdout.strip())
            else:
                return {
                    "provider": "agy_pi5",
                    "status": "ERROR",
                    "error": proc.stderr.strip() or "No output from remote host"
                }
        except Exception as e:
            return {
                "provider": "agy_pi5",
                "status": "ERROR",
                "error": str(e)
            }

    @staticmethod
    def _format_thai_time(time_str: str) -> str:
        """แปลง ISO/UTC Timestamp ให้อ่านง่ายในโซนเวลาไทย (ICT)"""
        if not time_str:
            return "-"
        try:
            if "T" in time_str:
                dt = datetime.datetime.fromisoformat(time_str)
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            elif "+00:00" in time_str:
                clean = time_str.split("+")[0].strip()
                if "." in clean:
                    base, subsec = clean.split(".")
                    clean = f"{base}.{subsec[:6]}"
                dt = datetime.datetime.strptime(clean, "%Y-%m-%d %H:%M:%S.%f")
                dt += datetime.timedelta(hours=7)
                return dt.strftime("%Y-%m-%d %H:%M:%S (ICT)")
        except Exception:
            return time_str
        return time_str


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Agy Pi5 Provider CLI")
    parser.add_argument("--status", action="store_true", help="Print provider quota & connection status in JSON")
    args = parser.parse_args()

    provider = AgyPi5Provider()
    if args.status:
        print(json.dumps(provider.get_quota_status(), ensure_ascii=False))
    else:
        print("AgyPi5Provider initialized. Use --status to check provider quota.")
