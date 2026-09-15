#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
FILE NAME   : webdriver_utils.py
LOCATION    : core/
DESCRIPTION : รองรับการสร้าง Selenium Chrome WebDriver ข้ามแพลตฟอร์ม
              ทั้ง Windows, Linux x86_64 และ Linux ARM64 (Raspberry Pi / aarch64)
              โดยแก้ปัญหา SeleniumManager ไม่รองรับ linux/aarch64
==============================================================================
"""

import os
import sys
import shutil
import platform
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options


def find_chromedriver_path() -> str | None:
    """ค้นหาพาธของ chromedriver บนระบบ"""
    # 1. ลองค้นหาจาก PATH ในระบบ
    which_path = shutil.which("chromedriver")
    if which_path and os.path.isfile(which_path) and os.access(which_path, os.X_OK):
        return which_path

    # 2. รายการพาธมาตรฐานบน Linux / Raspberry Pi OS
    candidate_paths = [
        "/usr/bin/chromedriver",
        "/usr/lib/chromium-browser/chromedriver",
        "/usr/lib/chromium/chromedriver",
        "/usr/local/bin/chromedriver",
        "/snap/bin/chromium.chromedriver",
    ]
    for p in candidate_paths:
        if os.path.isfile(p) and os.access(p, os.X_OK):
            return p

    return None


def find_chromium_browser_path() -> str | None:
    """ค้นหาพาธของเบราว์เซอร์ Chromium / Chrome บนระบบ"""
    # 1. ค้นหาจาก PATH
    for name in ["chromium-browser", "chromium", "google-chrome", "google-chrome-stable"]:
        which_path = shutil.which(name)
        if which_path and os.path.isfile(which_path) and os.access(which_path, os.X_OK):
            return which_path

    # 2. รายการพาธมาตรฐานบน Linux / Raspberry Pi OS
    candidate_paths = [
        "/usr/bin/chromium-browser",
        "/usr/bin/chromium",
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/snap/bin/chromium",
    ]
    for p in candidate_paths:
        if os.path.isfile(p) and os.access(p, os.X_OK):
            return p

    return None


def get_chrome_driver(headless: bool = True, extra_options: list[str] | None = None) -> webdriver.Chrome:
    """
    สร้างและคืนค่า instance ของ Selenium Chrome WebDriver
    รองรับ Windows, Linux x86_64, และ Raspberry Pi OS (linux/aarch64)
    """
    system = platform.system().lower()
    machine = platform.machine().lower()
    is_arm_linux = (system == "linux") and ("arm" in machine or "aarch64" in machine)

    options = Options()
    if headless:
        options.add_argument("--headless=new")

    # Options เพื่อความเสถียรบน Server / Raspberry Pi
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-extensions")
    options.add_argument("--remote-allow-origins=*")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    if extra_options:
        for opt in extra_options:
            options.add_argument(opt)

    # บน Linux ตรวจหาพาธ Browser
    if system == "linux":
        browser_path = find_chromium_browser_path()
        if browser_path:
            options.binary_location = browser_path

    # ค้นหาพาธของ chromedriver เพื่อส่งให้ Service
    driver_path = find_chromedriver_path()

    if driver_path:
        service = Service(executable_path=driver_path)
        return webdriver.Chrome(service=service, options=options)

    # กรณีเป็น Raspberry Pi (ARM64) แต่หา chromedriver ไม่พบ
    if is_arm_linux:
        raise RuntimeError(
            "ไม่พบ 'chromedriver' บนระบบ Raspberry Pi (ARM64)!\n"
            "เนื่องจาก Selenium Manager ไม่รองรับ linux/aarch64 โดยตรง\n"
            "กรุณาติดตั้ง Chromium และ ChromeDriver ผ่าน apt บน Pi ด้วยคำสั่ง:\n"
            "  sudo apt update && sudo apt install -y chromium-browser chromium-chromedriver"
        )

    # กรณีระบบอื่นๆ (เช่น Windows หรือ x86_64) ปล่อยให้ Selenium Manager ทำงานอัตโนมัติ
    return webdriver.Chrome(options=options)
