# from yahooquery import Screener
# s = Screener()
# data = s.get_screeners('most_actives', count=250)
# print(data)


import requests
r = requests.get("https://api.twelvedata.com/stocks?exchange=XBKK")
stocks = r.json()["data"]
print(stocks)