'''
เอาไว้ merge ไฟล์ source code ทั้งหมดในโปรเจกต์เป็นไฟล์เดียว พร้อม metadata ของแต่ละไฟล์
'''
from datetime import datetime
import fnmatch
import os
from pathlib import Path

# ==========================================
# ส่วนตั้งค่า (Configuration)
# ==========================================

# 1. รายการไฟล์/โฟลเดอร์/Pattern ที่ต้องการรวม
INCLUDES = [
    # "src/*",           # โฟลเดอร์ src ทั้งหมด
    # "*.py",            # ไฟล์ .py ทุกไฟล์ใน root
    # "components/*.vue",
    # "utils/helpers.py",
]

# 2. รายการไฟล์/โฟลเดอร์/Pattern ที่ต้องการยกเว้น (Exclude)
EXCLUDES = [
    "__pycache__",
    ".git/*",
    "dist/*",
    "node_modules/*",
    
    "_DatabaseBackup/*",
    "_importCsvHistoryPrice/*",
    "_notUse_backup/*",
    "_testLab/*",
    ".vscode/*",
    "env/*",
    "update/__pycache__/*",

    "combined_source_All_Files.txt",  # กันไม่ให้รวมไฟล์ output ตัวเอง
    "merge_files.py",
    ".gitignore",
    "*.pyc",
    "*.log",
    ".env",
    "*.txt",
    "*.md",
    "*.code-workspace",
]

OUTPUT_FILE = "combined_source_All_Files.txt"
PROJECT_ROOT = "."  # โฟลเดอร์เริ่มต้นของโปรเจกต์


def matches_any_pattern(relative_path: str, patterns: list[str]) -> bool:
    """ตรวจสอบว่า path ตรงกับ pattern ใดๆ ในรายการหรือไม่"""
    # ปรับ path ให้ใช้เครื่องหมาย / เสมอเพื่อความเข้ากันได้ข้าม OS
    norm_path = relative_path.replace(os.sep, "/")
    file_name = Path(norm_path).name

    for pattern in patterns:
        clean_pattern = pattern.strip().replace(os.sep, "/")
        
        # จับคู่แบบตรงตัวกับ Path, จับคู่ด้วย fnmatch, หรือจับคู่ชื่อไฟล์เดี่ยวๆ
        if fnmatch.fnmatch(norm_path, clean_pattern) or \
           fnmatch.fnmatch(file_name, clean_pattern) or \
           norm_path.startswith(clean_pattern.rstrip("/*") + "/"):
            return True
            
    return False


def collect_target_files(root_dir: str) -> list[Path]:
    """สแกนและกรองไฟล์ตามเงื่อนไข INCLUDES และ EXCLUDES"""
    root = Path(root_dir).resolve()
    valid_files = []

    for path in root.rglob("*"):
        if not path.is_file():
            continue

        rel_path = str(path.relative_to(root))

        # ตรวจสอบว่าอยู่ใน Excludes หรือไม่
        if matches_any_pattern(rel_path, EXCLUDES):
            continue

        # 2. ถ้า INCLUDES ว่าง ให้ถือว่าเอาทุกไฟล์ (ยกเว้นตัวที่โดน Exclude)
        #    ถ้า INCLUDES มีค่า ต้องตรงเงื่อนไขเท่านั้น
        if not INCLUDES or matches_any_pattern(rel_path, INCLUDES):
            valid_files.append(path)

    return sorted(valid_files)


def merge_source_files(root_dir: str, output_path: str):
    root = Path(root_dir).resolve()
    files = collect_target_files(root_dir)

    with open(output_path, "w", encoding="utf-8") as outfile:
        for file_path in files:
            rel_path = file_path.relative_to(root)
            stat = file_path.stat()

            # คำนวณ Metadata
            size_kb = stat.st_size / 1024
            mtime = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
            
            # ดึงเวลาสร้าง (st_birthtime บน macOS/Windows, st_ctime บน Linux)
            ctime_val = getattr(stat, "st_birthtime", stat.st_ctime)
            ctime = datetime.fromtimestamp(ctime_val).strftime("%Y-%m-%d %H:%M:%S")

            # ส่วนหัวคั่นแต่ละไฟล์
            header = f"""
{"=" * 80}
FILE NAME    : {file_path.name}
FOLDER       : {rel_path.parent}
FULL PATH    : {rel_path}
SIZE         : {size_kb:.2f} KB
CREATED DATE : {ctime}
MODIFIED DATE: {mtime}
{"=" * 80}
"""
            outfile.write(header)

            # อ่านและเขียนเนื้อหาไฟล์ (ข้ามกรณีเป็น binary หรือ decode ไม่ผ่าน)
            try:
                content = file_path.read_text(encoding="utf-8")
                outfile.write(content)
            except UnicodeDecodeError:
                outfile.write(f"[Binary or Non-UTF8 content skipped: {file_path.name}]\n")

            outfile.write("\n\n")

    print(f"รวมไฟล์เรียบร้อยแล้ว: {len(files)} ไฟล์ -> {output_path}")


if __name__ == "__main__":
    merge_source_files(PROJECT_ROOT, OUTPUT_FILE)