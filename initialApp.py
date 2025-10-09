import os
from dotenv import load_dotenv
load_dotenv()

args_Investor = {
                    "app_id": os.getenv("app_id"),
                    "app_secret": os.getenv("app_secret"),
                    "broker_id": "023",
                    "app_code": "ALGO_EQ",
#                     "broker_id":"SANDBOX",
#                     "app_code":"SANDBOX",
                    "is_auto_queue": False 
}

db_args = {
    'database': os.getenv("posql_db"),
    'user': os.getenv("posql_u"),
    'password': os.getenv("posql_p"),
    'host': os.getenv("posql_h"),
    'port': "5432"
}

# sqlSvr_args = {
#     'server': 'stockGoldenCross',
#     'database': 'stockBotTrade',
#     'username': 'sa',
#     'password': 'P@ssw0rd@Sql',
#     'driver': 'ODBC Driver 17 for SQL Server'
# }