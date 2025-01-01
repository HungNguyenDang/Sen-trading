# region import
import pyodbc
import pandas as pd
import MetaTrader5 as mt5
from sqlalchemy import create_engine

from helpers_database import *
# endregion

# region class Account, Pair, Server
class Account:
    def __init__(self, login, password, server):
        self.login = login
        self.password = password
        self.server = server

class Pair:
    def __init__(self, symbol, timeframe):
        self.symbol = symbol
        self.timeframe = timeframe

class Create_Server:
    def __init__(self, driver, server, username, password, db_name):
        self.driver = driver
        self.server = server
        self.username = username
        self.password = password
        self.db_name =db_name
# endregion

# region variables
# Define the connection parameters for SQL Server
server = 'HUNG-LAPTOP' 
username = 'trading'  
password = 'Dulieulon123'
driver = '{ODBC Driver 17 for SQL Server}'
driver_alchemy = 'ODBC+Driver+17+for+SQL+Server'
db_name = "audusd"
table_name = "audusd_h6"
pair_name = "AUDUSD.sml"
timeframe = mt5.TIMEFRAME_H6
unique_text = 'text_id'
from_day = pd.to_datetime("2000-01-01 00:00:00")
to_day = pd.to_datetime("2025-01-01 00:00:00")
chunk_capacity = 20000
# endregion

timeframe_list = {mt5.TIMEFRAME_M2, mt5.TIMEFRAME_M3,
             mt5.TIMEFRAME_M10,
             mt5.TIMEFRAME_M30, mt5.TIMEFRAME_H1, mt5.TIMEFRAME_H2,
             mt5.TIMEFRAME_H3, mt5.TIMEFRAME_H4, 
             mt5.TIMEFRAME_W1, mt5.TIMEFRAME_MN1}
table_name_list = {"audusd_m2", "audusd_m3",
             "audusd_m10",
             "audusd_m30", "audusd_h1", "audusd_h2",
             "audusd_h3", "audusd_h4", 
             "audusd_w1", "audusd_mn1"}

# for tf, tbn in zip(timeframe_list, table_name_list): 
#     print(f"Timeframe: {tf}, Table Name: {tbn}")      

Server = Create_Server(driver, server, username, password, db_name)
create_table(Server, table_name)

# take data from OANDA MT5, split into smaller chunks for processing
account = Account(6269482, 'jc6qhduy', 'OANDA-Demo-1')
pair = Pair(pair_name, timeframe)
date_ranges = generate_date_ranges(2000, 2025)
ohlc_df = get_data_MT5(link, account, pair, from_day, to_day)
print("number of rows from mt5: ", len(ohlc_df))
df_chunk = divide_dataframe(ohlc_df, chunk_capacity)

# add data to the table
engine = create_engine(f"mssql+pyodbc://{Server.username}:{Server.password}@{Server.server}/{Server.db_name}?driver={driver_alchemy}")
add_data(table_name, engine, df_chunk)
