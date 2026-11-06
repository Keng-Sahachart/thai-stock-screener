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

        # สร้างไฟล์ Prompt ชั่วคราวเพื่อส่งผ่าน Stdin ป้องกัน Escape Character บิดเบือน
        temp_fd, temp_path = tempfile.mkstemp(suffix=".txt", prefix="agy_prompt_")
        temp_out_fd, temp_out_path = tempfile.mkstemp(suffix=".json", prefix="agy_out_")
        temp_err_fd, temp_err_path = tempfile.mkstemp(suffix=".err", prefix="agy_err_")
        os.close(temp_out_fd)
        os.close(temp_err_fd)

        try:
            with open(temp_fd, "w", encoding="utf-8") as f:
                f.write(prompt)

            is_running_locally_on_pi = (sys.platform != "win32") or os.path.exists(self.agy_binary)

            if is_running_locally_on_pi:
                # รันอยู่บน Pi 5 โดยตรง: เรียก agy ในเครื่องพร้อมจับผลลัพธ์ผ่านไฟล์ชั่วคราว
                # เมื่อ agy เขียน JSON ผลลัพธ์เสร็จ จะสั่งปิด background language server ทันที
                cmd = [
                    self.agy_binary,
                    "--output-format", "json",
                    "--effort", "low",
                    "--dangerously-skip-permissions",
                    "--disable-slash-commands"
                ]

                with open(temp_path, "r", encoding="utf-8") as stdin_f:
                    with open(temp_out_path, "w", encoding="utf-8") as out_f, open(temp_err_path, "w", encoding="utf-8") as err_f:
                        proc = subprocess.Popen(
                            cmd,
                            stdin=stdin_f,
                            stdout=out_f,
                            stderr=err_f,
                            text=True,
                            encoding="utf-8"
                        )

                        agy_resp = None
                        poll_interval = 1.0
                        max_polls = int(self.timeout_sec / poll_interval)

                        for _ in range(max_polls):
                            time.sleep(poll_interval)
                            if os.path.exists(temp_out_path) and os.path.getsize(temp_out_path) > 300:
                                try:
                                    with open(temp_out_path, "r", encoding="utf-8") as rf:
                                        content = rf.read().strip()
                                        if content.endswith("}"):
                                            data = json.loads(content)
                                            if "status" in data and "response" in data:
                                                agy_resp = data
                                                proc.terminate()
                                                try:
                                                    proc.wait(timeout=2)
                                                except Exception:
                                                    proc.kill()
                                                break
                                except Exception:
                                    pass
                            if proc.poll() is not None:
                                break

                        if not agy_resp:
                            if os.path.exists(temp_out_path) and os.path.getsize(temp_out_path) > 0:
                                try:
                                    with open(temp_out_path, "r", encoding="utf-8") as rf:
                                        agy_resp = json.loads(rf.read().strip())
                                except Exception:
                                    pass

                        if not agy_resp:
                            proc.terminate()
                            try:
                                proc.wait(timeout=2)
                            except Exception:
                                proc.kill()

                            # อ่านข้อความ Error จาก stderr
                            err_msg = ""
                            if os.path.exists(temp_err_path):
                                try:
                                    with open(temp_err_path, "r", encoding="utf-8") as ef:
                                        err_msg = ef.read().strip()
                                except Exception:
                                    pass

                            if "quota" in err_msg.lower() or "429" in err_msg or "resource_exhausted" in err_msg.lower():
                                clean_err = "⚠️ โควตาการใช้งานของบัญชี Google Antigravity บน Pi 5 เต็มชั่วคราว (Individual quota reached)"
                                print(f"[AgyPi5Provider QUOTA_EXHAUSTED] {err_msg}")
                                return {"error": clean_err, "top_pick": "NONE", "evaluations": []}

                            return {"error": err_msg or f"AI evaluation timed out after {self.timeout_sec}s", "top_pick": "NONE", "evaluations": []}

            else:
                # รันอยู่บน Windows Dev PC: สั่งงานข้ามเครื่องไปยัง Pi 5 ผ่าน Plink
                cmd = [
                    self.plink_path,
                    "-batch",
                    "-pw", self.password,
                    f"{self.user}@{self.host}",
                    f"{self.agy_binary} --output-format json --effort low --dangerously-skip-permissions --disable-slash-commands"
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
            if not agy_resp:
                return {"error": "No response received from AI", "top_pick": "NONE", "evaluations": []}

            if agy_resp.get("status") == "ERROR" or agy_resp.get("error"):
                err_str = agy_resp.get("error") or "Unknown AI error"
                if "quota" in err_str.lower() or "429" in err_str or "individual quota" in err_str.lower():
                    err_str = f"⚠️ โควตาการใช้งาน Antigravity บน Pi 5 เต็มชั่วคราว ({err_str})"
                return {
                    "error": err_str,
                    "status": "ERROR",
                    "top_pick": "NONE",
                    "evaluations": []
                }

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
            for p in [temp_path, temp_out_path, temp_err_path]:
                if os.path.exists(p):
                    try:
                        os.remove(p)
                    except Exception:
                        pass

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

    def _resolve_account_email(self, access_token: str, id_token: str = None, token_data: dict = None) -> str:
        """ตรวจสอบและดึง Email จริงของบัญชี Google จาก Token"""
        if token_data and isinstance(token_data, dict):
            for k in ("email", "user_email", "account"):
                if token_data.get(k):
                    return token_data[k]
            tok = token_data.get("token", {})
            if isinstance(tok, dict):
                for k in ("email", "user_email", "account"):
                    if tok.get(k):
                        return tok[k]

        # ถอดรหัสจาก id_token (JWT)
        if id_token and isinstance(id_token, str):
            try:
                import base64
                parts = id_token.split(".")
                if len(parts) >= 2:
                    b64 = parts[1]
                    padded = b64 + "=" * ((4 - len(b64) % 4) % 4)
                    payload = json.loads(base64.urlsafe_b64decode(padded.encode()).decode("utf-8"))
                    if payload.get("email"):
                        return payload["email"]
            except Exception:
                pass

        # เรียก Google tokeninfo API ผ่าน access_token
        if access_token and isinstance(access_token, str):
            try:
                url = f"https://oauth2.googleapis.com/tokeninfo?access_token={access_token}"
                req = urllib.request.Request(url, headers={"User-Agent": "AntigravityBot/1.0"})
                with urllib.request.urlopen(req, timeout=3) as resp:
                    data = json.loads(resp.read().decode())
                    if data.get("email"):
                        return data["email"]
            except Exception:
                pass

            # เรียก Google userinfo API สำรอง
            try:
                url = "https://www.googleapis.com/oauth2/v3/userinfo"
                req = urllib.request.Request(url, headers={
                    "Authorization": f"Bearer {access_token}",
                    "User-Agent": "AntigravityBot/1.0"
                })
                with urllib.request.urlopen(req, timeout=3) as resp:
                    data = json.loads(resp.read().decode())
                    if data.get("email"):
                        return data["email"]
            except Exception:
                pass

        return None

    def _fetch_local_status(self) -> dict:
        """ประมวลผลดึงสถานะโดยตรงจาก Local File และ CloudCode API บน Pi 5"""
        status_info = {
            "provider": "agy_pi5",
            "status": "OFFLINE",
            "account": "กำลังตรวจสอบบัญชี...",
            "tier": "Gemini Code Assist (Standard Tier)",
            "tier_desc": "Unlimited coding & analysis assistant with Gemini models",
            "model": "Gemini 3.8 Flash (High)",
            "cloudcode_connected": False,
            "latency_ms": 0,
            "token_valid": False,
            "token_expiry": None,
            "token_expiry_formatted": "-",
            "today_requests": 0,
            "weekly_requests": 0,
            "account_weekly_requests": 0,
            "device_weekly_requests": 0,
            "total_requests": 0,
            "quota_exhausted": False,
            "quota_reset_in": None,
            "last_request_time": "-",
            "last_request_title": "-",
            "last_usage": None
        }

        # 1. ตรวจสอบ OAuth Token
        token_file = os.path.expanduser("~/.gemini/antigravity-cli/antigravity-oauth-token")
        access_token = None
        id_token = None
        tdata = {}
        if os.path.exists(token_file):
            try:
                with open(token_file, "r", encoding="utf-8") as tf:
                    tdata = json.load(tf)
                tok_info = tdata.get("token", {})
                access_token = tok_info.get("access_token") or tdata.get("access_token")
                id_token = tok_info.get("id_token") or tdata.get("id_token")
                expiry_str = tok_info.get("expiry") or tdata.get("expiry")
                status_info["token_expiry"] = expiry_str
                status_info["token_expiry_formatted"] = self._format_thai_time(expiry_str)
                status_info["token_valid"] = bool(access_token)
            except Exception as e:
                status_info["token_error"] = str(e)

        # 1.1 ดึง Account Email จริงจาก Token (Dynamic Resolution)
        resolved_email = self._resolve_account_email(access_token, id_token, tdata)
        if resolved_email:
            status_info["account"] = resolved_email
        else:
            status_info["account"] = "Google Account (Active)"

        # 1.2 ติดตามการสลับบัญชีเพื่อคำนวณโควตาแยกรายบัญชี
        tracker_path = ROOT_DIR / "logs" / "account_tracker.json"
        tracker_data = {}
        if tracker_path.exists():
            try:
                with open(tracker_path, "r", encoding="utf-8") as trf:
                    tracker_data = json.load(trf)
            except Exception:
                pass

        current_acc = status_info["account"]
        last_acc = tracker_data.get("active_account")
        account_just_switched = False

        if current_acc and current_acc != last_acc and current_acc != "กำลังตรวจสอบบัญชี...":
            account_just_switched = True
            tracker_data = {
                "active_account": current_acc,
                "switched_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            }
            try:
                tracker_path.parent.mkdir(parents=True, exist_ok=True)
                with open(tracker_path, "w", encoding="utf-8") as trw:
                    json.dump(tracker_data, trw, indent=2)
            except Exception:
                pass

        switched_at = tracker_data.get("switched_at")

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

                seven_days_ago = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
                cur.execute("SELECT COUNT(*) FROM conversation_summaries WHERE last_modified_time >= ?", (seven_days_ago,))
                device_weekly = cur.fetchone()[0]
                status_info["device_weekly_requests"] = device_weekly

                if switched_at:
                    cur.execute("SELECT COUNT(*) FROM conversation_summaries WHERE last_modified_time >= ?", (switched_at,))
                    acc_weekly = cur.fetchone()[0]
                    status_info["account_weekly_requests"] = acc_weekly
                else:
                    status_info["account_weekly_requests"] = device_weekly

                status_info["weekly_requests"] = status_info["account_weekly_requests"]

                cur.execute("SELECT last_modified_time, title FROM conversation_summaries ORDER BY last_modified_time DESC LIMIT 1")
                row = cur.fetchone()
                if row:
                    status_info["last_request_time"] = self._format_thai_time(row[0])
                    status_info["last_request_title"] = row[1]
                conn.close()
            except Exception as de:
                status_info["db_error"] = str(de)

        # 3.1 ตรวจสอบสถานะ RESOURCE_EXHAUSTED จาก cli.log ของ agy
        cli_log_path = os.path.expanduser("~/.gemini/antigravity-cli/cli.log")
        if os.path.exists(cli_log_path):
            try:
                import re
                with open(cli_log_path, "r", encoding="utf-8", errors="ignore") as lf:
                    lines = lf.readlines()[-60:]
                log_tail = "".join(lines)
                if "RESOURCE_EXHAUSTED" in log_tail or "quota reached" in log_tail.lower():
                    if not account_just_switched:
                        status_info["quota_exhausted"] = True
                        m = re.search(r"Resets in ([^\.\n\r]+)", log_tail, re.IGNORECASE)
                        if m:
                            status_info["quota_reset_in"] = m.group(1).strip()
            except Exception:
                pass

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
