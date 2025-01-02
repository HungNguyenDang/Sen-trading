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

# --------------------------------------------------------------------------------------#
# CHANGE THESE VARIABLES
prefix = "uk100"  # change when using manually and automatically
db_name = prefix
table_name = f"{prefix}_m15" # change when using manually
pair_name = "UK100"  # change when using manually
timeframe = mt5.TIMEFRAME_M15  # change when using manually

unique_text = 'text_id'
from_day = pd.to_datetime("2005-01-01 01:00:00") # not all instruments has data from 2000, check from broker
to_day = pd.to_datetime("2025-01-01 00:00:00")
chunk_capacity = 20000 # size of each time to insert data to sql
# endregion

timeframe_list = [mt5.TIMEFRAME_M1, mt5.TIMEFRAME_M2, mt5.TIMEFRAME_M3, mt5.TIMEFRAME_M4,
                  mt5.TIMEFRAME_M5, mt5.TIMEFRAME_M10, mt5.TIMEFRAME_M15,
                  mt5.TIMEFRAME_M30, mt5.TIMEFRAME_H1, mt5.TIMEFRAME_H2,
                  mt5.TIMEFRAME_H3, mt5.TIMEFRAME_H4, mt5.TIMEFRAME_H6, mt5.TIMEFRAME_D1, 
                  mt5.TIMEFRAME_W1, mt5.TIMEFRAME_MN1]

table_name_list = [f"{prefix}_m1", f"{prefix}_m2", f"{prefix}_m3", f"{prefix}_m4",
                   f"{prefix}_m5", f"{prefix}_m10", f"{prefix}_m15",
                   f"{prefix}_m30", f"{prefix}_h1", f"{prefix}_h2",
                   f"{prefix}_h3", f"{prefix}_h4", f"{prefix}_h6", f"{prefix}_d1", 
                   f"{prefix}_w1", f"{prefix}_mn1"]

Server = Create_Server(driver, server, username, password, db_name)
create_table(Server, table_name)

# The account for mt5
account = Account(6269482, 'jc6qhduy', 'OANDA-Demo-1')

# The engine for sql database
engine = create_engine(f"mssql+pyodbc://{Server.username}:{Server.password}@{Server.server}/{Server.db_name}?driver={driver_alchemy}")

# --------------------------------------------------------------------------------------#
manual = 1
# -- Manual side, take the pair name, database name, table name, timeframe and run it
if manual == 1:
    # date_ranges = generate_date_ranges(2000, 2025)
    pair = Pair(pair_name, timeframe)
    ohlc_df = get_data_MT5(link, account, pair, from_day, to_day)
    print("number of rows from mt5: ", len(ohlc_df))
    df_chunk = divide_dataframe(ohlc_df, chunk_capacity)

    # add data to the table

    add_data(table_name, engine, df_chunk)

# --------------------------------------------------------------------------------------#
automatic = 0
# -- Automatic side, process all timeframe for 1 pair
if automatic == 1:
    # Create a dictionary with the two lists
    data = {
        "Timeframe": timeframe_list,
        "Table Name": table_name_list
    }

    # Create DataFrame
    df_id = pd.DataFrame(data)

    for index, row in df_id.iterrows():
        # print(row['Timeframe'], ", ", row['Table Name'])
        pair = Pair(pair_name, row['Timeframe'])
        ohlc_df = get_data_MT5(link, account, pair, from_day, to_day)
        print("number of rows from mt5: ", len(ohlc_df))
        df_chunk = divide_dataframe(ohlc_df, chunk_capacity)

        # add data to the table
        add_data(row['Table Name'], engine, df_chunk)
