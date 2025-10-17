import ezyquant as ez
from ezyquant.backtesting.account import SETAccount
from ezyquant.backtesting import Context

ssc = ez.SETSignalCreator(
   start_date="2022-01-01", # วันที่ต้องการเริ่มดึง data
   end_date="2022-01-04", # วันที่สิ้นสุดการดึง data
   index_list=['SET50'], # list index ที่ต้องการจะดึงข้อมูล ถ้าไม่ต้องการให้ใส่ list ว่าง
   symbol_list= ['NETBAY'] # list หุ้นที่ต้องการจะดึง ถ้าไม่ต้องการให้ใส่ list ว่าง
)

print(ssc.stock_list)
# df = ssc.get_stock_list()   