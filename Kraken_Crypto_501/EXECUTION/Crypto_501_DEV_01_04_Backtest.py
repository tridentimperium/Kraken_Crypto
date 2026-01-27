import sys
import os
import pyodbc
import logging
import pandas as pd
import numpy as np
import json
import math
from datetime import datetime
from dotenv import load_dotenv

# ================================
# LOGGING
# ================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if logger.hasHandlers():
    logger.handlers.clear()
console_handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.info("--- STARTING BACKTEST ---")

# ================================
# PATHS
# ================================
EXECUTION_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_PATH = os.path.dirname(EXECUTION_DIR)
CONFIG_PATH = os.path.join(BASE_PATH, "CONFIG")

# ================================
# TABLE NAMES
# ================================
SOURCE_TABLE = "dbo.Crypto_501_DEV_01_02_Analysis_Results"
BACKTEST_TABLE = "dbo.Crypto_501_DEV_01_04_Analysis_Backtest"

# ================================
# LOAD CONFIG (batch or standalone) - FIXED
# ================================
vars_config = None
if len(sys.argv) > 1:
    # Batch mode: use JSON passed from batch runner
    try:
        json_config_string = sys.argv[1]
        vars_config = json.loads(json_config_string)
        logger.info("Loaded variables from command-line argument (Batch Mode).")
    except json.JSONDecodeError as e:
        logger.error(f"FATAL ERROR: Failed to decode JSON config from command line: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"FATAL ERROR: Unknown error loading variables from command line: {e}")
        sys.exit(1)
else:
    # Standalone mode: load from default file
    variables_file = os.path.join(CONFIG_PATH, "ZZ_VARIABLES", "Crypto_501_variables.json")
    if not os.path.exists(variables_file):
        logger.error(f"FATAL ERROR: Variables file not found: {variables_file}")
        sys.exit(1)
    try:
        with open(variables_file, 'r', encoding='utf-8') as f:
            vars_config = json.load(f)
        logger.info(f"Loaded variables from default file: {variables_file} (Standalone Mode).")
    except Exception as e:
        logger.error(f"FATAL ERROR: Failed to load variables from file '{variables_file}': {e}")
        sys.exit(1)

if vars_config is None:
    logger.error("FATAL ERROR: Configuration was not loaded. Exiting.")
    sys.exit(1)

# Extract IDs with defaults
ANALYSIS_RUN_ID = int(vars_config.get("AnalysisRunID", 1))
FETCH_RUN_ID = int(vars_config.get("FetchRunID", 1))

logger.info(f"Using AnalysisRunID = {ANALYSIS_RUN_ID}, FetchRunID = {FETCH_RUN_ID}")

# ================================
# LOAD BACKTEST PARAMETERS FROM Crypto_501_parameters.json
# (unchanged - this file is separate and always loaded)
# ================================
parameters_file = os.path.join(CONFIG_PATH, "ZZ_PARAMETERS", "Crypto_501_parameters.json")
if not os.path.exists(parameters_file):
    logger.error(f"Parameters file not found: {parameters_file}")
    sys.exit(1)

with open(parameters_file, 'r', encoding='utf-8') as f:
    params_config = json.load(f)

def _get_param(key, default, cast=lambda x: x):
    val = params_config.get(key, default)
    if isinstance(val, list):
        val = val[0]
    return cast(val)

INITIAL_STARTING_BALANCE = round(_get_param("StartingBalance", 10000.0, float), 2)
LEVERAGE = _get_param("Leverage", 1.0, float)

logger.info(f"Backtest parameters loaded: StartingBalance = {INITIAL_STARTING_BALANCE:.2f}, Leverage = {LEVERAGE}")

# ================================
# SQL CONNECTION
# ================================
sql_env_file = os.path.join(CONFIG_PATH, "SQLSERVER", "Crypto_501_sqlserver_local.env")
if not os.path.exists(sql_env_file):
    sql_env_file = os.path.join(CONFIG_PATH, "SQLSERVER", "Crypto_501_sqlserver_remote.env")
load_dotenv(sql_env_file, encoding='utf-8')
conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={os.getenv('SQL_SERVER')};"
    f"DATABASE={os.getenv('SQL_DATABASE')};"
    f"UID={os.getenv('SQL_USER')};"
    f"PWD={os.getenv('SQL_PASSWORD')};"
    f"TrustServerCertificate=yes;"
)
try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    logger.info("Connected to SQL Server")
except Exception as e:
    logger.error(f"SQL connection failed: {e}")
    sys.exit(1)

# ================================
# ENSURE BACKTEST TABLE
# ================================
create_backtest_sql = f'''
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_501_DEV_01_04_Analysis_Backtest')
BEGIN
    SELECT * INTO {BACKTEST_TABLE} FROM {SOURCE_TABLE} WHERE 1 = 0;
END

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_501_DEV_01_04_Analysis_Backtest' AND COLUMN_NAME = 'EntryExit')
    ALTER TABLE {BACKTEST_TABLE} ADD EntryExit FLOAT NULL;
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_501_DEV_01_04_Analysis_Backtest' AND COLUMN_NAME = 'LongShort')
    ALTER TABLE {BACKTEST_TABLE} ADD LongShort NVARCHAR(20) NULL;
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_501_DEV_01_04_Analysis_Backtest' AND COLUMN_NAME = 'StartingBalance')
    ALTER TABLE {BACKTEST_TABLE} ADD StartingBalance FLOAT NULL;
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_501_DEV_01_04_Analysis_Backtest' AND COLUMN_NAME = 'Leverage')
    ALTER TABLE {BACKTEST_TABLE} ADD Leverage FLOAT NULL;
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_501_DEV_01_04_Analysis_Backtest' AND COLUMN_NAME = 'Quantity')
    ALTER TABLE {BACKTEST_TABLE} ADD Quantity FLOAT NULL;
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_501_DEV_01_04_Analysis_Backtest' AND COLUMN_NAME = 'EntryPrice')
    ALTER TABLE {BACKTEST_TABLE} ADD EntryPrice FLOAT NULL;
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_501_DEV_01_04_Analysis_Backtest' AND COLUMN_NAME = 'EntryCost')
    ALTER TABLE {BACKTEST_TABLE} ADD EntryCost FLOAT NULL;
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_501_DEV_01_04_Analysis_Backtest' AND COLUMN_NAME = 'ExitPrice')
    ALTER TABLE {BACKTEST_TABLE} ADD ExitPrice FLOAT NULL;
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_501_DEV_01_04_Analysis_Backtest' AND COLUMN_NAME = 'ExitCost')
    ALTER TABLE {BACKTEST_TABLE} ADD ExitCost FLOAT NULL;
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_501_DEV_01_04_Analysis_Backtest' AND COLUMN_NAME = 'ProfitLoss')
    ALTER TABLE {BACKTEST_TABLE} ADD ProfitLoss FLOAT NULL;
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_501_DEV_01_04_Analysis_Backtest' AND COLUMN_NAME = 'EndingBalance')
    ALTER TABLE {BACKTEST_TABLE} ADD EndingBalance FLOAT NULL;
'''
cursor.execute(create_backtest_sql)
conn.commit()
logger.info(f"Backtest table ready: {BACKTEST_TABLE}")

# ================================
# LOAD DATA
# ================================
query = f"""
SELECT * 
FROM {SOURCE_TABLE} 
WHERE AnalysisRunID = ? AND FetchRunID = ? 
ORDER BY FetchRunID, AnalysisRunID, DateTime
"""
df = pd.read_sql(query, conn, params=[ANALYSIS_RUN_ID, FETCH_RUN_ID])
if df.empty:
    logger.warning("No data found.")
    conn.close()
    sys.exit(0)

df['DateTime'] = pd.to_datetime(df['DateTime'])
df = df.set_index('DateTime').sort_index()
logger.info(f"Loaded {len(df)} rows.")

# ================================
# BACKTEST LOGIC
# ================================
def backtest_group(group):
    df = group.sort_index().copy()

    # Initialize new columns
    for col in ['L_PTPrice', 'L_SLPrice', 'S_PTPrice', 'S_SLPrice',
                'EntryExit', 'LongShort', 'StartingBalance', 'Leverage', 'Quantity',
                'EntryPrice', 'EntryCost', 'ExitPrice', 'ExitCost', 'ProfitLoss', 'EndingBalance']:
        df[col] = np.nan

    df['InTrade'] = 0

    in_trade = False
    current_balance = INITIAL_STARTING_BALANCE
    entry_price = 0.0
    quantity = 0.0
    entry_cost = 0.0
    pt_price = np.nan
    sl_price = np.nan
    trade_direction = None

    prev_entry_exit = None
    prev_in_trade = 0

    for idx in df.index:
        close = df.loc[idx, 'Close']
        buy_sig = df.loc[idx, 'BuySignal']
        sell_sig = df.loc[idx, 'SellSignal']

        l_pt_pct = df.loc[idx, 'L_PTPercent']
        l_sl_pct = df.loc[idx, 'L_SLPercent']
        s_pt_pct = df.loc[idx, 'S_PTPercent']
        s_sl_pct = df.loc[idx, 'S_SLPercent']

        # Carry forward PT/SL prices while in trade
        if in_trade:
            df.loc[idx, 'InTrade'] = 1
            df.loc[idx, 'LongShort'] = 'Long' if trade_direction == 'long' else 'Short'
            df.loc[idx, 'StartingBalance'] = round(current_balance, 2)
            df.loc[idx, 'Leverage'] = LEVERAGE
            df.loc[idx, 'Quantity'] = round(quantity, 6)
            df.loc[idx, 'EntryPrice'] = round(entry_price, 2)
            df.loc[idx, 'EntryCost'] = round(entry_cost, 2)
            df.loc[idx, 'L_PTPrice'] = round(pt_price, 3) if trade_direction == 'long' else np.nan
            df.loc[idx, 'L_SLPrice'] = round(sl_price, 3) if trade_direction == 'long' else np.nan
            df.loc[idx, 'S_PTPrice'] = round(pt_price, 3) if trade_direction == 'short' else np.nan
            df.loc[idx, 'S_SLPrice'] = round(sl_price, 3) if trade_direction == 'short' else np.nan

            # Check exit conditions
            hit_pt = False
            hit_sl = False

            if trade_direction == 'long':
                hit_pt = close >= pt_price
                hit_sl = close <= sl_price
            elif trade_direction == 'short':
                hit_pt = close <= pt_price
                hit_sl = close >= sl_price

            if hit_pt or hit_sl:
                df.loc[idx, 'EntryExit'] = 2.0
                df.loc[idx, 'ExitPrice'] = round(close, 3)
                df.loc[idx, 'ExitCost'] = round(close * quantity, 2)
                df.loc[idx, 'ProfitLoss'] = round(df.loc[idx, 'ExitCost'] - entry_cost, 2)
                new_balance = current_balance + df.loc[idx, 'ProfitLoss']
                df.loc[idx, 'EndingBalance'] = round(new_balance, 2)
                current_balance = new_balance
                in_trade = False
                prev_entry_exit = 2.0
                prev_in_trade = 1
                continue  # do not enter new trade on the same candle

        # Entry logic
        can_enter = (
            (prev_entry_exit is None and prev_in_trade == 0) or
            (prev_entry_exit == 2.0 and prev_in_trade == 1)
        )

        if not in_trade and can_enter and (buy_sig == 1 or sell_sig == 1):
            in_trade = True
            entry_price = close
            quantity = (current_balance * LEVERAGE) / close
            entry_cost = entry_price * quantity

            if buy_sig == 1:
                trade_direction = 'long'
                pt_price = close * (1 + l_pt_pct / 100) if pd.notna(l_pt_pct) else np.nan
                sl_price = close * (1 - abs(l_sl_pct) / 100) if pd.notna(l_sl_pct) else np.nan
                df.loc[idx, 'L_PTPrice'] = round(pt_price, 3)
                df.loc[idx, 'L_SLPrice'] = round(sl_price, 3)
            else:
                trade_direction = 'short'
                pt_price = close * (1 - s_pt_pct / 100) if pd.notna(s_pt_pct) else np.nan
                sl_price = close * (1 + abs(s_sl_pct) / 100) if pd.notna(s_sl_pct) else np.nan
                df.loc[idx, 'S_PTPrice'] = round(pt_price, 3)
                df.loc[idx, 'S_SLPrice'] = round(sl_price, 3)

            df.loc[idx, 'InTrade'] = 1
            df.loc[idx, 'EntryExit'] = 1.0
            df.loc[idx, 'LongShort'] = 'Long' if trade_direction == 'long' else 'Short'
            df.loc[idx, 'StartingBalance'] = round(current_balance, 2)
            df.loc[idx, 'Leverage'] = LEVERAGE
            df.loc[idx, 'Quantity'] = round(quantity, 6)
            df.loc[idx, 'EntryPrice'] = round(entry_price, 3)
            df.loc[idx, 'EntryCost'] = round(entry_cost, 2)

        # Update tracking variables for next row
        prev_entry_exit = df.loc[idx, 'EntryExit'] if pd.notna(df.loc[idx, 'EntryExit']) else None
        prev_in_trade = int(df.loc[idx, 'InTrade'])

    return df

# Run backtest per symbol/timeframe group
grouped = df.groupby(['Symbol', 'Timeframe'])
df_backtest = pd.concat([backtest_group(g) for _, g in grouped])

# ================================
# INSERT INTO BACKTEST TABLE
# ================================
insert_sql = f"""
INSERT INTO {BACKTEST_TABLE}
(FetchRunID, AnalysisRunID, DateTime, Timeframe, Symbol, [Open], [High], [Low], [Close], Volume, N001,
 IsSwingHigh, IsSwingLow, SwingType, Slope, N002, Trend, N003, Entry, EntryCount, TargetDirection,
 L_PTPercent, L_SLPercent, L_PTPrice, L_SLPrice, S_PTPercent, S_SLPercent, S_PTPrice, S_SLPrice,
 N004, EntryExit,
 BuySignal, SellSignal, LongShort, InTrade, N005,
 StartingBalance, Leverage, Quantity, EntryPrice, EntryCost, ExitPrice, ExitCost, ProfitLoss, EndingBalance)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

rows = 0
try:
    for idx, row in df_backtest.iterrows():
        values = [
            FETCH_RUN_ID,
            ANALYSIS_RUN_ID,
            idx,
            row['Timeframe'],
            row['Symbol'],
            None if pd.isna(row['Open']) else float(row['Open']),
            None if pd.isna(row['High']) else float(row['High']),
            None if pd.isna(row['Low']) else float(row['Low']),
            None if pd.isna(row['Close']) else float(row['Close']),
            None if pd.isna(row['Volume']) else float(row['Volume']),
            None if pd.isna(row.get('N001')) else float(row['N001']),
            int(row.get('IsSwingHigh', 0)),
            int(row.get('IsSwingLow', 0)),
            row['SwingType'] if pd.notna(row.get('SwingType')) else None,
            None if pd.isna(row.get('Slope')) else float(row['Slope']),
            None if pd.isna(row.get('N002')) else float(row['N002']),
            row['Trend'] if pd.notna(row.get('Trend')) else 'Sideways',
            None if pd.isna(row.get('N003')) else float(row['N003']),
            row['Entry'] if pd.notna(row.get('Entry')) else None,
            int(row['EntryCount']) if pd.notna(row.get('EntryCount')) else None,
            row['TargetDirection'] if pd.notna(row.get('TargetDirection')) else None,
            None if pd.isna(row.get('L_PTPercent')) else float(row['L_PTPercent']),
            None if pd.isna(row.get('L_SLPercent')) else float(row['L_SLPercent']),
            None if pd.isna(row.get('L_PTPrice')) else float(row['L_PTPrice']),
            None if pd.isna(row.get('L_SLPrice')) else float(row['L_SLPrice']),
            None if pd.isna(row.get('S_PTPercent')) else float(row['S_PTPercent']),
            None if pd.isna(row.get('S_SLPercent')) else float(row['S_SLPercent']),
            None if pd.isna(row.get('S_PTPrice')) else float(row['S_PTPrice']),
            None if pd.isna(row.get('S_SLPrice')) else float(row['S_SLPrice']),
            None if pd.isna(row.get('N004')) else float(row['N004']),
            None if pd.isna(row.get('EntryExit')) else float(row['EntryExit']),
            int(row.get('BuySignal', 0)),
            int(row.get('SellSignal', 0)),
            row['LongShort'] if pd.notna(row.get('LongShort')) else None,
            int(row.get('InTrade', 0)),
            None if pd.isna(row.get('N005')) else float(row['N005']),
            None if pd.isna(row.get('StartingBalance')) else float(row['StartingBalance']),
            None if pd.isna(row.get('Leverage')) else float(row['Leverage']),
            None if pd.isna(row.get('Quantity')) else float(row['Quantity']),
            None if pd.isna(row.get('EntryPrice')) else float(row['EntryPrice']),
            None if pd.isna(row.get('EntryCost')) else float(row['EntryCost']),
            None if pd.isna(row.get('ExitPrice')) else float(row['ExitPrice']),
            None if pd.isna(row.get('ExitCost')) else float(row['ExitCost']),
            None if pd.isna(row.get('ProfitLoss')) else float(row['ProfitLoss']),
            None if pd.isna(row.get('EndingBalance')) else float(row['EndingBalance'])
        ]

        cursor.execute(insert_sql, values)
        rows += 1

    conn.commit()
    logger.info(f"Backtest complete: Inserted {rows} rows into {BACKTEST_TABLE}")
except Exception as e:
    logger.error(f"Insert failed: {e}")
    conn.rollback()
finally:
    conn.close()

logger.info("Backtest script finished successfully.")