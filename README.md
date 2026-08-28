# thai-stock-screener
analysis thai stock in set

how to install on pi
git clone https://github.com/Keng-Sahachart/thai-stock-screener.git

python -m venv .venv
pip install -r .\requirements.txt

crontab -e

25 12 * * * /home/keng/thai-stock-screener/thai-stock-screener/.venv/bin/python /home/keng/thai-stock-screener/thai-stock-screener/taskUpdate.py >> /home/keng/thai-stock-screener/taskUpdate.log 2>&1

กด Ctrl + O แล้วกด Enter เพื่อบันทึก
กด Ctrl + X เพื่อออกจากโปรแกรมแก้ไข

ดูเวลาเครื่อง
timedatectl 