# region import
import pyodbc
import pandas as pd
import MetaTrader5 as mt5
import pymysql
from sqlalchemy import create_engine
# endregion

# region VARIABLES
# from_day = pd.to_datetime("2000-01-01 00:00:00")
# to_day = pd.to_datetime("2025-01-01 00:00:00")
link = "C:\\Program Files\\OANDA MetaTrader 5\\terminal64.exe"
# endregion

def get_data_MT5(link, account, pair, from_day, to_day):
    if not mt5.initialize(link):
        print("initialize() failed, error code =", mt5.last_error())
        quit()
    mt5.login(account.login, account.password, account.server)
    ohlc = mt5.copy_rates_range(pair.symbol, pair.timeframe, from_day, to_day)
    ohlc_df = pd.DataFrame(ohlc)
    ohlc_df['time'] = pd.to_datetime(ohlc_df['time'], unit='s')
    ohlc_df['text_id'] = ohlc_df['time'].dt.strftime('%Y-%m-%d-%H-%M')
    
    # drop column 'real_volume'
    ohlc_df=ohlc_df.drop(['real_volume'], axis = 1)

    # drop dupplicate of text_id
    ohlc_df = ohlc_df.drop_duplicates(subset=['text_id'], keep='first')

    ohlc_df = check_data_type(ohlc_df)
    return ohlc_df

def generate_date_ranges(start_year, end_year):
    date_ranges = []
    for year in range(start_year, end_year):
        from_day = pd.to_datetime(f"{year}-01-01 00:00:00")
        to_day = pd.to_datetime(f"{year + 1}-01-01 00:00:00")
        date_ranges.append((from_day, to_day))
    return date_ranges

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def divide_dataframe(df, n):
    # Use a list comprehension to create chunks of DataFrames
    df_chunks = [df.iloc[i:i + n] for i in range(0, len(df), n)]
    return df_chunks

def create_table(Server, table_name):
    try:
        connection = pyodbc.connect(
            f"DRIVER={Server.driver};SERVER={Server.server};UID={Server.username};PWD={Server.password};DATABASE={Server.db_name}"
        )
        print("Connection successful!")
    except pyodbc.Error as e:
        print(f"Error connecting to database: {e}")

    # Create a cursor object to interact with the SQL Server
    cursor = connection.cursor()

    # Check if the database exists
    cursor.execute(f"SELECT name FROM master.dbo.sysdatabases WHERE name = '{Server.db_name}'")
    database_exists = cursor.fetchone()

    if not database_exists:
        # Create the database if it doesn't exist
        create_database_query = f"CREATE DATABASE {Server.db_name}"
        cursor.execute(create_database_query)
        print(f"Database {Server.db_name} created successfully!")
    else:
        print(f"Database {Server.db_name} already exists.")

    # Check if the table exists and create it if it does not
    table_check_query = f"""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}')
        BEGIN
            CREATE TABLE {table_name} (
                id INT IDENTITY(1,1) PRIMARY KEY,
                time datetime,
                [open] FLOAT,
                [high] FLOAT,
                [low] FLOAT,
                [close] FLOAT,
                tick_volume BIGINT,
                spread INT,
                text_id VARCHAR(255)
            )
        END
    """
        
    # Execute the table creation query
    cursor.execute(table_check_query)
    print(f"Table {table_name} checked and created if it did not exist.")

    # Commit the transaction
    connection.commit()

    # Close the connection
    cursor.close()
    connection.close()

    print(f"Database '{Server.db_name}' and table '{table_name}' are connected.")

def add_data(table_name, engine, df_chunks):
    # Import DataFrame chunks into the SQL table using the 'append' option
    for i, chunk in enumerate(df_chunks):
        try:
            chunk.to_sql(table_name, con=engine, if_exists='append', index=False)
            print(f"Chunk {i+1} data imported successfully into {table_name}!")
        except Exception as e:
            print(f"Error importing data for chunk {i+1}: {e}")

    print("Data import completed successfully.")

def check_data_type (df):
    for col in df.columns:
        if df[col].dtype == 'uint64':
            df[col] = df[col].astype('int64')  # Convert to int64
        elif df[col].dtype == 'object':
            # Convert to datetime if applicable
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception as e:
                pass
    return df

    