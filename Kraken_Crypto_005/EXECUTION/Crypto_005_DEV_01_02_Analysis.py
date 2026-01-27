import sys
import os
import pyodbc
import logging
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
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
logger.info("--- STARTING ANALYSIS ---")

# ================================
# PATHS
# ================================
EXECUTION_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_PATH = os.path.dirname(EXECUTION_DIR)
CONFIG_PATH = os.path.join(BASE_PATH, "CONFIG")

# ================================
# FIXED TABLE NAMES
# ================================
FETCH_TABLE = "dbo.Crypto_005_DEV_01_01_Fetch_Data"
ANALYSIS_TABLE = "dbo.Crypto_005_DEV_01_02_Analysis_Results"

# ================================
# LOAD VARIABLES FROM JSON
# ================================
vars_config = None
if len(sys.argv) > 1:
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
    variables_file = os.path.join(CONFIG_PATH, "ZZ_VARIABLES", "Crypto_005_variables.json")
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

def _get_val(key, default, cast=lambda x: x):
    val = vars_config.get(key, default)
    if isinstance(val, list):
        val = val[0]
    return cast(val)

LOOKBACK = _get_val("SwingLookback", 7, int)
MIN_SWING_PCT = _get_val("MinSwingPct", 0.08, float)
ENABLE_MIN_SWING = _get_val("EnableMinSwingFilter", False, bool)
ENTRY = _get_val("Entry", 1, int)
ENTRY_COUNT = _get_val("EntryCount", 1, int)
TARGET_DIRECTION = _get_val("TargetDirection", 1, int)
L_PT_PERCENT = _get_val("L_ProfitTargetPercent", 2, float)
L_SL_PERCENT = _get_val("L_StopLossPercent", 2, float)
S_PT_PERCENT = _get_val("S_ProfitTargetPercent", L_PT_PERCENT, float)
S_SL_PERCENT = _get_val("S_StopLossPercent", L_SL_PERCENT, float)
TREND_LINE_RANGE = _get_val("TrendlineRange", 24, int)
LOG_LEVEL = _get_val("LogLevel", "INFO", str).upper()

FETCH_RUN_ID = _get_val("FetchRunID", 1, int)
ANALYSIS_RUN_ID = _get_val("AnalysisRunID", 1, int)

logging.getLogger().setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

logger.info(f"Config → LOOKBACK={LOOKBACK}, MIN_PCT={MIN_SWING_PCT}, ENABLE_FILTER={ENABLE_MIN_SWING}, "
            f"ENTRY={ENTRY}, ENTRY_COUNT={ENTRY_COUNT}, TARGET_DIRECTION={TARGET_DIRECTION}, "
            f"L_PT%={L_PT_PERCENT}, L_SL%={L_SL_PERCENT}, S_PT%={S_PT_PERCENT}, S_SL%={S_SL_PERCENT}, "
            f"TREND_LINE_RANGE={TREND_LINE_RANGE}")
logger.info(f"Using FetchRunID = {FETCH_RUN_ID}, AnalysisRunID = {ANALYSIS_RUN_ID}")

# ================================
# LOAD SQL CREDENTIALS
# ================================
sql_env_file = os.path.join(CONFIG_PATH, "SQLSERVER", "Crypto_005_sqlserver_local.env")
if not os.path.exists(sql_env_file):
    sql_env_file = os.path.join(CONFIG_PATH, "SQLSERVER", "Crypto_005_sqlserver_remote.env")
if not os.path.exists(sql_env_file):
    logger.error(f"SQL env file not found: {sql_env_file}")
    sys.exit(1)
load_dotenv(sql_env_file, encoding='utf-8')
logger.info(f"Loaded SQL env: {sql_env_file}")
required = ["SQL_SERVER", "SQL_DATABASE", "SQL_USER", "SQL_PASSWORD"]
missing = [k for k in required if not os.getenv(k)]
if missing:
    logger.error(f"Missing SQL env vars: {missing}")
    sys.exit(1)

# ================================
# SQL CONNECTION
# ================================
try:
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.getenv('SQL_SERVER')};"
        f"DATABASE={os.getenv('SQL_DATABASE')};"
        f"UID={os.getenv('SQL_USER')};"
        f"PWD={os.getenv('SQL_PASSWORD')};"
        f"TrustServerCertificate=yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    logger.info("Connected to SQL Server")
except Exception as e:
    logger.error(f"SQL connection failed: {e}")
    sys.exit(1)

# ================================
# ENSURE ANALYSIS TABLE
# ================================
create_analysis_table = f'''
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES
               WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_005_DEV_01_02_Analysis_Results')
BEGIN
    CREATE TABLE {ANALYSIS_TABLE} (
        FetchRunID INT NOT NULL,
        AnalysisRunID INT NOT NULL,
        DateTime DATETIME NOT NULL,
        Timeframe VARCHAR(10) NOT NULL,
        Symbol NVARCHAR(50) NOT NULL,
        [Open] FLOAT NULL,
        [High] FLOAT NULL,
        [Low] FLOAT NULL,
        [Close] FLOAT NULL,
        Volume FLOAT NULL,
        N001 FLOAT NULL,
        IsSwingHigh BIT NOT NULL DEFAULT 0,
        IsSwingLow BIT NOT NULL DEFAULT 0,
        SwingType NVARCHAR(10) NULL,
        Slope FLOAT NULL,
        N002 FLOAT NULL,
        Trend NVARCHAR(20) NULL,
        N003 FLOAT NULL,
        Entry NVARCHAR(10) NULL,
        EntryCount INT NULL,
        TargetDirection NVARCHAR(20) NULL,
        L_PTPercent DECIMAL(10,2) NULL,
        L_SLPercent DECIMAL(10,2) NULL,
        L_PTPrice FLOAT NULL,
        L_SLPrice FLOAT NULL,
        S_PTPercent DECIMAL(10,2) NULL,
        S_SLPercent DECIMAL(10,2) NULL,
        S_PTPrice FLOAT NULL,
        S_SLPrice FLOAT NULL,
        N004 FLOAT NULL,
        EntryExit FLOAT NULL,
        BuySignal BIT NOT NULL DEFAULT 0,
        SellSignal BIT NOT NULL DEFAULT 0,
        LongShort NVARCHAR(20) NULL,
        InTrade BIT NOT NULL DEFAULT 0,
        N005 FLOAT NULL,
        StartingBalance FLOAT NULL,
        Leverage FLOAT NULL,
        Quantity FLOAT NULL,
        EntryPrice FLOAT NULL,
        EntryCost FLOAT NULL,
        ExitPrice FLOAT NULL,
        ExitCost FLOAT NULL,
        ProfitLoss FLOAT NULL,
        EndingBalance FLOAT NULL,
        PRIMARY KEY (AnalysisRunID, DateTime, Symbol, Timeframe)
    );
END
ELSE
BEGIN
    IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS
               WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_005_DEV_01_02_Analysis_Results'
               AND COLUMN_NAME = 'PTPercent')
        EXEC sp_rename 'dbo.Crypto_005_DEV_01_02_Analysis_Results.PTPercent', 'L_PTPercent', 'COLUMN';

    IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS
               WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_005_DEV_01_02_Analysis_Results'
               AND COLUMN_NAME = 'SLPercent')
        EXEC sp_rename 'dbo.Crypto_005_DEV_01_02_Analysis_Results.SLPercent', 'L_SLPercent', 'COLUMN';

    IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS
               WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_005_DEV_01_02_Analysis_Results'
               AND COLUMN_NAME = 'PTPrice')
        EXEC sp_rename 'dbo.Crypto_005_DEV_01_02_Analysis_Results.PTPrice', 'L_PTPrice', 'COLUMN';

    IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS
               WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_005_DEV_01_02_Analysis_Results'
               AND COLUMN_NAME = 'SLPrice')
        EXEC sp_rename 'dbo.Crypto_005_DEV_01_02_Analysis_Results.SLPrice', 'L_SLPrice', 'COLUMN';

    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_005_DEV_01_02_Analysis_Results' AND COLUMN_NAME = 'L_PTPercent')
        ALTER TABLE {ANALYSIS_TABLE} ADD L_PTPercent DECIMAL(10,2) NULL;
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_005_DEV_01_02_Analysis_Results' AND COLUMN_NAME = 'L_SLPercent')
        ALTER TABLE {ANALYSIS_TABLE} ADD L_SLPercent DECIMAL(10,2) NULL;
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_005_DEV_01_02_Analysis_Results' AND COLUMN_NAME = 'L_PTPrice')
        ALTER TABLE {ANALYSIS_TABLE} ADD L_PTPrice FLOAT NULL;
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_005_DEV_01_02_Analysis_Results' AND COLUMN_NAME = 'L_SLPrice')
        ALTER TABLE {ANALYSIS_TABLE} ADD L_SLPrice FLOAT NULL;
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_005_DEV_01_02_Analysis_Results' AND COLUMN_NAME = 'S_PTPercent')
        ALTER TABLE {ANALYSIS_TABLE} ADD S_PTPercent DECIMAL(10,2) NULL;
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_005_DEV_01_02_Analysis_Results' AND COLUMN_NAME = 'S_SLPercent')
        ALTER TABLE {ANALYSIS_TABLE} ADD S_SLPercent DECIMAL(10,2) NULL;
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_005_DEV_01_02_Analysis_Results' AND COLUMN_NAME = 'S_PTPrice')
        ALTER TABLE {ANALYSIS_TABLE} ADD S_PTPrice FLOAT NULL;
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_005_DEV_01_02_Analysis_Results' AND COLUMN_NAME = 'S_SLPrice')
        ALTER TABLE {ANALYSIS_TABLE} ADD S_SLPrice FLOAT NULL;

    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_005_DEV_01_02_Analysis_Results' AND COLUMN_NAME = 'EntryExit')
        ALTER TABLE {ANALYSIS_TABLE} ADD EntryExit FLOAT NULL;
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_005_DEV_01_02_Analysis_Results' AND COLUMN_NAME = 'LongShort')
        ALTER TABLE {ANALYSIS_TABLE} ADD LongShort NVARCHAR(20) NULL;
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_005_DEV_01_02_Analysis_Results' AND COLUMN_NAME = 'N005')
        ALTER TABLE {ANALYSIS_TABLE} ADD N005 FLOAT NULL;
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_005_DEV_01_02_Analysis_Results' AND COLUMN_NAME = 'StartingBalance')
        ALTER TABLE {ANALYSIS_TABLE} ADD StartingBalance FLOAT NULL;
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_005_DEV_01_02_Analysis_Results' AND COLUMN_NAME = 'Leverage')
        ALTER TABLE {ANALYSIS_TABLE} ADD Leverage FLOAT NULL;
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_005_DEV_01_02_Analysis_Results' AND COLUMN_NAME = 'Quantity')
        ALTER TABLE {ANALYSIS_TABLE} ADD Quantity FLOAT NULL;
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_005_DEV_01_02_Analysis_Results' AND COLUMN_NAME = 'EntryPrice')
        ALTER TABLE {ANALYSIS_TABLE} ADD EntryPrice FLOAT NULL;
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_005_DEV_01_02_Analysis_Results' AND COLUMN_NAME = 'EntryCost')
        ALTER TABLE {ANALYSIS_TABLE} ADD EntryCost FLOAT NULL;
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_005_DEV_01_02_Analysis_Results' AND COLUMN_NAME = 'ExitPrice')
        ALTER TABLE {ANALYSIS_TABLE} ADD ExitPrice FLOAT NULL;
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_005_DEV_01_02_Analysis_Results' AND COLUMN_NAME = 'ExitCost')
        ALTER TABLE {ANALYSIS_TABLE} ADD ExitCost FLOAT NULL;
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_005_DEV_01_02_Analysis_Results' AND COLUMN_NAME = 'ProfitLoss')
        ALTER TABLE {ANALYSIS_TABLE} ADD ProfitLoss FLOAT NULL;
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_005_DEV_01_02_Analysis_Results' AND COLUMN_NAME = 'EndingBalance')
        ALTER TABLE {ANALYSIS_TABLE} ADD EndingBalance FLOAT NULL;
END
'''
try:
    cursor.execute(create_analysis_table)
    conn.commit()
    logger.info(f"Table ensured with all new columns: {ANALYSIS_TABLE}")
except Exception as e:
    logger.error(f"Table setup failed: {e}")
    conn.close()
    sys.exit(1)

# ================================
# LOAD DATA USING FetchRunID
# ================================
query = f"SELECT DateTime, Timeframe, Symbol, [Open], [High], [Low], [Close], Volume FROM {FETCH_TABLE} WHERE FetchRunID = ? ORDER BY DateTime"
df = pd.read_sql(query, conn, params=[FETCH_RUN_ID])
if df.empty:
    logger.warning("No data.")
    conn.close()
    sys.exit(0)

df['DateTime'] = pd.to_datetime(df['DateTime'])
df = df.set_index('DateTime').sort_index()
logger.info(f"Loaded {len(df)} rows.")

# ================================
# SWING DETECTION (Real-Time Version: Only Past Data)
# ================================
high = df['High']
low = df['Low']
is_swing_high = [False] * len(df)
is_swing_low = [False] * len(df)

last_swing_high_idx = None
last_swing_low_idx = None

for i in range(LOOKBACK, len(df)):
    if high.iloc[i] >= high.iloc[i - LOOKBACK:i].max():
        if last_swing_high_idx is None or (i - last_swing_high_idx) >= (LOOKBACK // 2):
            is_swing_high[i] = True
            last_swing_high_idx = i
    
    if low.iloc[i] <= low.iloc[i - LOOKBACK:i].min():
        if last_swing_low_idx is None or (i - last_swing_low_idx) >= (LOOKBACK // 2):
            is_swing_low[i] = True
            last_swing_low_idx = i

df['IsSwingHigh'] = is_swing_high
df['IsSwingLow'] = is_swing_low

# ================================
# LABEL HH/LL/LH/HL (with optional % filter)
# ================================
swing_highs = df[df['IsSwingHigh']].copy()
swing_lows = df[df['IsSwingLow']].copy()

prev_high = None
for idx in swing_highs.index:
    current = swing_highs.loc[idx, 'High']
    if prev_high is None:
        df.loc[idx, 'SwingType'] = None
    else:
        pct_change = (current - prev_high) / prev_high * 100
        if ENABLE_MIN_SWING and abs(pct_change) < MIN_SWING_PCT:
            df.loc[idx, 'SwingType'] = None
        else:
            df.loc[idx, 'SwingType'] = 'HH' if current > prev_high else 'LH'
    prev_high = current

prev_low = None
for idx in swing_lows.index:
    current = swing_lows.loc[idx, 'Low']
    if prev_low is None:
        df.loc[idx, 'SwingType'] = None
    else:
        pct_change = (prev_low - current) / prev_low * 100
        if ENABLE_MIN_SWING and abs(pct_change) < MIN_SWING_PCT:
            df.loc[idx, 'SwingType'] = None
        else:
            df.loc[idx, 'SwingType'] = 'LL' if current < prev_low else 'HL'
    prev_low = current

# ================================
# SLOPE CALCULATION
# ================================
df['Slope'] = np.nan
for i in range(len(df)):
    end_time = df.index[i]
    start_time = end_time - timedelta(hours=TREND_LINE_RANGE)
    past_data = df[(df.index >= start_time) & (df.index <= end_time)]
    if len(past_data) < 2:
        continue
    past_span = (past_data.index[-1] - past_data.index[0]).total_seconds() / 3600.0
    if past_span < TREND_LINE_RANGE:
        continue
    x = (past_data.index - past_data.index[0]).total_seconds() / 3600.0
    y = past_data['Close'].values
    slope, _ = np.polyfit(x, y, 1)
    df.iloc[i, df.columns.get_loc('Slope')] = round(slope, 8)

# ================================
# TREND DETECTION BASED ON SLOPE
# ================================
df['Trend'] = 'Sideways'
df.loc[df['Slope'] > 0, 'Trend'] = 'Upward'
df.loc[df['Slope'] < 0, 'Trend'] = 'Downward'

# ================================
# SET NEW COLUMNS FROM CONFIG
# ================================
entry_str = "Ordered" if ENTRY == 1 else "Mixed" if ENTRY == 2 else None
target_direction_str = "Long" if TARGET_DIRECTION == 1 else "Short" if TARGET_DIRECTION == 2 else "Both" if TARGET_DIRECTION == 3 else None
df['Entry'] = entry_str
df['EntryCount'] = ENTRY_COUNT
df['TargetDirection'] = target_direction_str

# Long side - from specific L_ keys in variables.json
df['L_PTPercent'] = round(L_PT_PERCENT, 2)
df['L_SLPercent'] = round(L_SL_PERCENT, 2)
df['L_PTPrice'] = np.nan
df['L_SLPrice'] = np.nan

# Short side
df['S_PTPercent'] = round(S_PT_PERCENT, 2)
df['S_SLPercent'] = round(S_SL_PERCENT, 2)
df['S_PTPrice'] = np.nan
df['S_SLPrice'] = np.nan

df['BuySignal'] = 0
df['SellSignal'] = 0
df['LongShort'] = None
df['InTrade'] = 0

# Add NULL columns
df['N001'] = np.nan
df['N002'] = np.nan
df['N003'] = np.nan
df['N004'] = np.nan
df['EntryExit'] = np.nan
df['N005'] = np.nan
df['StartingBalance'] = np.nan
df['Leverage'] = np.nan
df['Quantity'] = np.nan
df['EntryPrice'] = np.nan
df['EntryCost'] = np.nan
df['ExitPrice'] = np.nan
df['ExitCost'] = np.nan
df['ProfitLoss'] = np.nan
df['EndingBalance'] = np.nan

# ================================
# SIGNAL DETECTION (unchanged)
# ================================
swings = df[(df['IsSwingHigh'] | df['IsSwingLow']) & df['SwingType'].notna()].copy()
swings = swings.sort_index()

if entry_str is not None and len(swings) >= 3:
    swing_indices = swings.index.tolist()
    pattern_length = 2 * ENTRY_COUNT + 1
    min_required_swings = pattern_length

    last_buy_signal_idx = None
    last_sell_signal_idx = None

    for j in range(min_required_swings - 1, len(swing_indices)):
        pattern_end_idx = swing_indices[j]
        pattern_start_idx = swing_indices[j - pattern_length + 1]
        pattern = swings.loc[pattern_start_idx:pattern_end_idx]

        if len(pattern) != pattern_length:
            continue

        pattern_types = pattern['SwingType'].tolist()
        pattern_is_high = pattern['IsSwingHigh'].tolist()

        signal_placed = False

        if TARGET_DIRECTION in [1, 3] and last_buy_signal_idx != pattern_end_idx:
            up_types = {'HL', 'HH'}
            if all(t in up_types for t in pattern_types):
                is_match = True
                if entry_str == "Ordered":
                    expected_types = ['HL' if k % 2 == 0 else 'HH' for k in range(pattern_length)]
                    expected_is_high = [False if k % 2 == 0 else True for k in range(pattern_length)]
                    if pattern_types != expected_types or pattern_is_high != expected_is_high:
                        is_match = False
                if is_match:
                    confirm_type = pattern_types[-1]
                    if entry_str == "Ordered":
                        if confirm_type == 'HL':
                            df.loc[pattern_end_idx, 'BuySignal'] = 1
                            last_buy_signal_idx = pattern_end_idx
                            signal_placed = True
                    else:
                        df.loc[pattern_end_idx, 'BuySignal'] = 1
                        last_buy_signal_idx = pattern_end_idx
                        signal_placed = True

        if TARGET_DIRECTION in [2, 3] and last_sell_signal_idx != pattern_end_idx and not signal_placed:
            down_types = {'LH', 'LL'}
            if all(t in down_types for t in pattern_types):
                is_match = True
                if entry_str == "Ordered":
                    expected_types = ['LH' if k % 2 == 0 else 'LL' for k in range(pattern_length)]
                    expected_is_high = [True if k % 2 == 0 else False for k in range(pattern_length)]
                    if pattern_types != expected_types or pattern_is_high != expected_is_high:
                        is_match = False
                if is_match:
                    confirm_type = pattern_types[-1]
                    if entry_str == "Ordered":
                        if confirm_type == 'LH':
                            df.loc[pattern_end_idx, 'SellSignal'] = 1
                            last_sell_signal_idx = pattern_end_idx
                    else:
                        df.loc[pattern_end_idx, 'SellSignal'] = 1
                        last_sell_signal_idx = pattern_end_idx

# ================================
# INSERT - FetchRunID first
# ================================
insert_sql = f"""
INSERT INTO {ANALYSIS_TABLE}
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
    for idx, row in df.iterrows():
        cursor.execute(insert_sql,
            FETCH_RUN_ID, ANALYSIS_RUN_ID, idx,
            row['Timeframe'], row['Symbol'],
            None if pd.isna(row['Open']) else float(row['Open']),
            None if pd.isna(row['High']) else float(row['High']),
            None if pd.isna(row['Low']) else float(row['Low']),
            None if pd.isna(row['Close']) else float(row['Close']),
            None if pd.isna(row['Volume']) else float(row['Volume']),
            None if pd.isna(row.get('N001', np.nan)) else float(row['N001']),
            1 if row['IsSwingHigh'] else 0,
            1 if row['IsSwingLow'] else 0,
            row['SwingType'] if pd.notna(row['SwingType']) else None,
            None if pd.isna(row['Slope']) else float(row['Slope']),
            None if pd.isna(row.get('N002', np.nan)) else float(row['N002']),
            row['Trend'],
            None if pd.isna(row.get('N003', np.nan)) else float(row['N003']),
            row['Entry'] if pd.notna(row.get('Entry', np.nan)) else None,
            None if pd.isna(row.get('EntryCount', np.nan)) else int(row['EntryCount']),
            row['TargetDirection'] if pd.notna(row.get('TargetDirection', np.nan)) else None,
            None if pd.isna(row.get('L_PTPercent', np.nan)) else round(float(row['L_PTPercent']), 2),
            None if pd.isna(row.get('L_SLPercent', np.nan)) else round(float(row['L_SLPercent']), 2),
            None if pd.isna(row.get('L_PTPrice', np.nan)) else float(row['L_PTPrice']),
            None if pd.isna(row.get('L_SLPrice', np.nan)) else float(row['L_SLPrice']),
            None if pd.isna(row.get('S_PTPercent', np.nan)) else round(float(row['S_PTPercent']), 2),
            None if pd.isna(row.get('S_SLPercent', np.nan)) else round(float(row['S_SLPercent']), 2),
            None if pd.isna(row.get('S_PTPrice', np.nan)) else float(row['S_PTPrice']),
            None if pd.isna(row.get('S_SLPrice', np.nan)) else float(row['S_SLPrice']),
            None if pd.isna(row.get('N004', np.nan)) else float(row['N004']),
            None if pd.isna(row.get('EntryExit', np.nan)) else float(row['EntryExit']),
            1 if row['BuySignal'] else 0,
            1 if row['SellSignal'] else 0,
            row['LongShort'] if pd.notna(row.get('LongShort')) else None,
            1 if row['InTrade'] else 0,
            None if pd.isna(row.get('N005', np.nan)) else float(row['N005']),
            None if pd.isna(row.get('StartingBalance', np.nan)) else float(row['StartingBalance']),
            None if pd.isna(row.get('Leverage', np.nan)) else float(row['Leverage']),
            None if pd.isna(row.get('Quantity', np.nan)) else float(row['Quantity']),
            None if pd.isna(row.get('EntryPrice', np.nan)) else float(row['EntryPrice']),
            None if pd.isna(row.get('EntryCost', np.nan)) else float(row['EntryCost']),
            None if pd.isna(row.get('ExitPrice', np.nan)) else float(row['ExitPrice']),
            None if pd.isna(row.get('ExitCost', np.nan)) else float(row['ExitCost']),
            None if pd.isna(row.get('ProfitLoss', np.nan)) else float(row['ProfitLoss']),
            None if pd.isna(row.get('EndingBalance', np.nan)) else float(row['EndingBalance'])
        )
        rows += 1
    conn.commit()
    logger.info(f"Inserted {rows} rows. FetchRunID: {FETCH_RUN_ID}, AnalysisRunID: {ANALYSIS_RUN_ID}")
except Exception as e:
    logger.error(f"Insert failed: {e}")
    conn.rollback()
finally:
    conn.close()

logger.info("Done.")