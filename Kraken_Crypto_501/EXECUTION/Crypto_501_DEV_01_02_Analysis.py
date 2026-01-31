import sys
import os
import pyodbc
import logging
import pandas as pd
import numpy as np
import json
import time
import warnings
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# Suppress the pandas SQLAlchemy warning
warnings.filterwarnings('ignore', message='.*SQLAlchemy connectable.*')

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
logger.info("--- STARTING LIVE ANALYSIS ---")

# ================================
# PATHS
# ================================
EXECUTION_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_PATH = os.path.dirname(EXECUTION_DIR)
CONFIG_PATH = os.path.join(BASE_PATH, "CONFIG")

# ================================
# FIXED TABLE NAMES
# ================================
LIVE_DATA_TABLE = "dbo.Crypto_501_DEV_01_01_Live_Data_Kraken_1_min"
ANALYSIS_TABLE = "dbo.Crypto_501_DEV_01_02_Analysis_Results"

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

# Polling interval in seconds (how often to check for new data)
POLL_INTERVAL = _get_val("PollInterval", 10, int)

logging.getLogger().setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

logger.info(f"Config → LOOKBACK={LOOKBACK}, MIN_PCT={MIN_SWING_PCT}, ENABLE_FILTER={ENABLE_MIN_SWING}, "
            f"ENTRY={ENTRY}, ENTRY_COUNT={ENTRY_COUNT}, TARGET_DIRECTION={TARGET_DIRECTION}, "
            f"L_PT%={L_PT_PERCENT}, L_SL%={L_SL_PERCENT}, S_PT%={S_PT_PERCENT}, S_SL%={S_SL_PERCENT}, "
            f"TREND_LINE_RANGE={TREND_LINE_RANGE}")
logger.info(f"Poll interval: {POLL_INTERVAL} seconds")

# ================================
# LOAD SQL CREDENTIALS
# ================================
sql_env_file = os.path.join(CONFIG_PATH, "SQLSERVER", "Crypto_501_sqlserver_local.env")
if not os.path.exists(sql_env_file):
    sql_env_file = os.path.join(CONFIG_PATH, "SQLSERVER", "Crypto_501_sqlserver_remote.env")
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
def get_connection():
    """Create and return a new SQL connection"""
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
        return conn
    except Exception as e:
        logger.error(f"SQL connection failed: {e}")
        return None

conn = get_connection()
if conn is None:
    sys.exit(1)
cursor = conn.cursor()
logger.info("Connected to SQL Server")

# ================================
# ENSURE ANALYSIS TABLE
# ================================
create_analysis_table = f'''
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES
               WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_501_DEV_01_02_Analysis_Results')
BEGIN
    CREATE TABLE {ANALYSIS_TABLE} (
        DateTime_EST DATETIME NOT NULL,
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
        CONSTRAINT PK_Analysis_Results PRIMARY KEY CLUSTERED (DateTime DESC, Symbol)
    );
END
'''

# Create index on DateTime DESC if table already exists and index doesn't exist
create_index = f'''
IF NOT EXISTS (SELECT * FROM sys.indexes 
               WHERE name = 'IDX_Analysis_DateTime_DESC' 
               AND object_id = OBJECT_ID('{ANALYSIS_TABLE}'))
BEGIN
    CREATE NONCLUSTERED INDEX IDX_Analysis_DateTime_DESC 
    ON {ANALYSIS_TABLE} (DateTime DESC, Symbol)
    INCLUDE ([Close], BuySignal, SellSignal, Trend);
END
'''

try:
    cursor.execute(create_analysis_table)
    conn.commit()
    logger.info(f"Checked/created {ANALYSIS_TABLE}")
    
    # Try to create index (will skip if already exists)
    try:
        cursor.execute(create_index)
        conn.commit()
        logger.info("Checked/created DateTime DESC index")
    except Exception as idx_err:
        logger.warning(f"Index creation skipped or failed: {idx_err}")
        
except Exception as e:
    logger.error(f"Failed to create table: {e}")
    conn.close()
    sys.exit(1)

# ================================
# HELPER FUNCTIONS FOR ANALYSIS
# ================================
def calculate_swing_points(df, lookback):
    """Identify swing highs and lows"""
    df['IsSwingHigh'] = False
    df['IsSwingLow'] = False
    df['SwingType'] = None
    
    for i in range(lookback, len(df) - lookback):
        window_before = df['High'].iloc[i - lookback:i]
        window_after = df['High'].iloc[i + 1:i + lookback + 1]
        if df['High'].iloc[i] == max(window_before.max(), df['High'].iloc[i], window_after.max()):
            df.loc[df.index[i], 'IsSwingHigh'] = True
        
        window_before = df['Low'].iloc[i - lookback:i]
        window_after = df['Low'].iloc[i + 1:i + lookback + 1]
        if df['Low'].iloc[i] == min(window_before.min(), df['Low'].iloc[i], window_after.min()):
            df.loc[df.index[i], 'IsSwingLow'] = True
    
    return df

def assign_swing_types(df):
    """Assign swing types (HH, HL, LH, LL)"""
    swing_data = df[df['IsSwingHigh'] | df['IsSwingLow']].copy()
    
    for i in range(1, len(swing_data)):
        prev_idx = swing_data.index[i - 1]
        curr_idx = swing_data.index[i]
        
        if swing_data.loc[curr_idx, 'IsSwingHigh']:
            if swing_data.loc[prev_idx, 'IsSwingLow']:
                prev_low = df.loc[prev_idx, 'Low']
                if i >= 2:
                    prev_prev_idx = swing_data.index[i - 2]
                    prev_prev_low = df.loc[prev_prev_idx, 'Low']
                    if prev_low > prev_prev_low:
                        df.loc[curr_idx, 'SwingType'] = 'HH'
                    else:
                        df.loc[curr_idx, 'SwingType'] = 'LH'
        
        elif swing_data.loc[curr_idx, 'IsSwingLow']:
            if swing_data.loc[prev_idx, 'IsSwingHigh']:
                prev_high = df.loc[prev_idx, 'High']
                if i >= 2:
                    prev_prev_idx = swing_data.index[i - 2]
                    prev_prev_high = df.loc[prev_prev_idx, 'High']
                    if prev_high > prev_prev_high:
                        df.loc[curr_idx, 'SwingType'] = 'HL'
                    else:
                        df.loc[curr_idx, 'SwingType'] = 'LL'
    
    return df

def calculate_trendline_slope(df, trend_range):
    """Calculate trendline slope using time-based window"""
    df['Slope'] = np.nan
    
    for i in range(len(df)):
        end_time = df.index[i]
        start_time = end_time - timedelta(hours=trend_range)
        past_data = df[(df.index >= start_time) & (df.index <= end_time)]
        if len(past_data) < 2:
            continue
        past_span = (past_data.index[-1] - past_data.index[0]).total_seconds() / 3600.0
        if past_span < trend_range:
            continue
        x = (past_data.index - past_data.index[0]).total_seconds() / 3600.0
        y = past_data['Close'].values
        slope, _ = np.polyfit(x, y, 1)
        df.iloc[i, df.columns.get_loc('Slope')] = round(slope, 8)
    
    return df

def detect_signals(df, entry_str, entry_count, target_direction):
    """Detect buy and sell signals"""
    df['BuySignal'] = 0
    df['SellSignal'] = 0
    
    swings = df[(df['IsSwingHigh'] | df['IsSwingLow']) & df['SwingType'].notna()].copy()
    swings = swings.sort_index()
    
    if entry_str is not None and len(swings) >= 3:
        swing_indices = swings.index.tolist()
        pattern_length = 2 * entry_count + 1
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
            
            if target_direction in [1, 3] and last_buy_signal_idx != pattern_end_idx:
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
            
            if target_direction in [2, 3] and last_sell_signal_idx != pattern_end_idx and not signal_placed:
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
    
    return df

def process_new_data(df_new, entry_str, target_direction):
    """Process new data with full analysis logic"""
    if df_new.empty:
        return df_new
    
    # Calculate swing points
    df_new = calculate_swing_points(df_new, LOOKBACK)
    
    # Filter by minimum swing percentage if enabled
    if ENABLE_MIN_SWING:
        for idx in df_new[df_new['IsSwingHigh']].index:
            high_price = df_new.loc[idx, 'High']
            nearby_lows = df_new.loc[max(0, idx - LOOKBACK):idx, 'Low']
            if not nearby_lows.empty:
                min_low = nearby_lows.min()
                pct_change = abs((high_price - min_low) / min_low * 100) if min_low > 0 else 0
                if pct_change < MIN_SWING_PCT:
                    df_new.loc[idx, 'IsSwingHigh'] = False
        
        for idx in df_new[df_new['IsSwingLow']].index:
            low_price = df_new.loc[idx, 'Low']
            nearby_highs = df_new.loc[max(0, idx - LOOKBACK):idx, 'High']
            if not nearby_highs.empty:
                max_high = nearby_highs.max()
                pct_change = abs((max_high - low_price) / low_price * 100) if low_price > 0 else 0
                if pct_change < MIN_SWING_PCT:
                    df_new.loc[idx, 'IsSwingLow'] = False
    
    # Assign swing types
    df_new = assign_swing_types(df_new)
    
    # Calculate trendline slope
    df_new = calculate_trendline_slope(df_new, TREND_LINE_RANGE)
    
    # Assign trend
    df_new['Trend'] = 'Sideways'
    df_new.loc[df_new['Slope'] > 0, 'Trend'] = 'Upward'
    df_new.loc[df_new['Slope'] < 0, 'Trend'] = 'Downward'
    
    # Set config columns
    df_new['Entry'] = entry_str
    df_new['EntryCount'] = ENTRY_COUNT
    df_new['TargetDirection'] = target_direction
    df_new['L_PTPercent'] = round(L_PT_PERCENT, 2)
    df_new['L_SLPercent'] = round(L_SL_PERCENT, 2)
    df_new['L_PTPrice'] = np.nan
    df_new['L_SLPrice'] = np.nan
    df_new['S_PTPercent'] = round(S_PT_PERCENT, 2)
    df_new['S_SLPercent'] = round(S_SL_PERCENT, 2)
    df_new['S_PTPrice'] = np.nan
    df_new['S_SLPrice'] = np.nan
    df_new['LongShort'] = None
    df_new['InTrade'] = 0
    
    # Add NULL columns
    df_new['N001'] = np.nan
    df_new['N002'] = np.nan
    df_new['N003'] = np.nan
    df_new['N004'] = np.nan
    df_new['EntryExit'] = np.nan
    df_new['N005'] = np.nan
    df_new['StartingBalance'] = np.nan
    df_new['Leverage'] = np.nan
    df_new['Quantity'] = np.nan
    df_new['EntryPrice'] = np.nan
    df_new['EntryCost'] = np.nan
    df_new['ExitPrice'] = np.nan
    df_new['ExitCost'] = np.nan
    df_new['ProfitLoss'] = np.nan
    df_new['EndingBalance'] = np.nan
    
    # Detect signals
    df_new = detect_signals(df_new, entry_str, ENTRY_COUNT, TARGET_DIRECTION)
    
    return df_new

# ================================
# INSERT FUNCTION
# ================================
def insert_analysis_results(cursor, conn, df_results):
    """Insert analysis results into the database"""
    insert_sql = f"""
    INSERT INTO {ANALYSIS_TABLE}
    (DateTime_EST, DateTime, Timeframe, Symbol, [Open], [High], [Low], [Close], Volume, N001,
     IsSwingHigh, IsSwingLow, SwingType, Slope, N002, Trend, N003, Entry, EntryCount, TargetDirection,
     L_PTPercent, L_SLPercent, L_PTPrice, L_SLPrice, S_PTPercent, S_SLPercent, S_PTPrice, S_SLPrice,
     N004, EntryExit,
     BuySignal, SellSignal, LongShort, InTrade, N005,
     StartingBalance, Leverage, Quantity, EntryPrice, EntryCost, ExitPrice, ExitCost, ProfitLoss, EndingBalance)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    rows = 0
    try:
        for idx, row in df_results.iterrows():
            cursor.execute(insert_sql,
                row['DateTime_EST'],
                idx,
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
        logger.info(f"Inserted {rows} new rows")
        return rows
    except Exception as e:
        logger.error(f"Insert failed: {e}")
        conn.rollback()
        return 0

# ================================
# MAIN PROCESSING LOOP
# ================================
entry_str = "Ordered" if ENTRY == 1 else "Mixed" if ENTRY == 2 else None
target_direction_str = "Long" if TARGET_DIRECTION == 1 else "Short" if TARGET_DIRECTION == 2 else "Both" if TARGET_DIRECTION == 3 else None

# Track the last processed DateTime
last_processed_datetime = None

# Get the last processed datetime from the analysis table if it exists
try:
    query = f"""
    SELECT MAX(DateTime) as LastDateTime, MAX(DateTime_EST) as LastDateTime_EST
    FROM {ANALYSIS_TABLE}
    """
    cursor.execute(query)
    result = cursor.fetchone()
    if result and result[0]:
        last_processed_datetime = result[0]
        last_processed_datetime_est = result[1]
        logger.info(f"Resuming from last processed datetime: {last_processed_datetime_est} EST ({last_processed_datetime} UTC)")
except Exception as e:
    logger.warning(f"Could not retrieve last processed datetime: {e}")

logger.info("Starting live data processing loop. Press Ctrl+C to stop.")

try:
    while True:
        try:
            # Query for new data
            if last_processed_datetime:
                query = f"""
                SELECT DateTime, DateTime_EST, Timeframe, Symbol, [Open], [High], [Low], [Close], Volume
                FROM {LIVE_DATA_TABLE}
                WHERE DateTime > ?
                ORDER BY DateTime
                """
                df_new = pd.read_sql(query, conn, params=[last_processed_datetime])
            else:
                # First run - get all data or get recent data with a lookback window
                query = f"""
                SELECT DateTime, DateTime_EST, Timeframe, Symbol, [Open], [High], [Low], [Close], Volume
                FROM {LIVE_DATA_TABLE}
                ORDER BY DateTime
                """
                df_new = pd.read_sql(query, conn)
            
            if not df_new.empty:
                logger.info(f"Found {len(df_new)} new rows to process")
                
                # Set DateTime as index
                df_new.set_index('DateTime', inplace=True)
                
                # For proper analysis, we need historical context
                # Get a window of data including lookback period
                if last_processed_datetime:
                    # Get some historical data for context
                    historical_query = f"""
                    SELECT TOP {LOOKBACK * 2 + TREND_LINE_RANGE} DateTime, DateTime_EST, Timeframe, Symbol, [Open], [High], [Low], [Close], Volume
                    FROM {LIVE_DATA_TABLE}
                    WHERE DateTime <= ?
                    ORDER BY DateTime DESC
                    """
                    df_historical = pd.read_sql(historical_query, conn, params=[last_processed_datetime])
                    df_historical = df_historical.iloc[::-1]  # Reverse to chronological order
                    df_historical.set_index('DateTime', inplace=True)
                    
                    # Combine historical and new data
                    df_combined = pd.concat([df_historical, df_new])
                else:
                    df_combined = df_new
                
                # Process the combined data
                df_processed = process_new_data(df_combined, entry_str, target_direction_str)
                
                # Extract only the new rows for insertion
                if last_processed_datetime:
                    df_to_insert = df_processed[df_processed.index > last_processed_datetime]
                else:
                    df_to_insert = df_processed
                
                # Insert new results
                if not df_to_insert.empty:
                    rows_inserted = insert_analysis_results(cursor, conn, df_to_insert)
                    
                    if rows_inserted > 0:
                        # Update last processed datetime
                        last_processed_datetime = df_to_insert.index[-1]
                        last_processed_datetime_est = df_to_insert['DateTime_EST'].iloc[-1]
                        logger.info(f"Updated last processed datetime to: {last_processed_datetime_est} EST ({last_processed_datetime} UTC)")
                else:
                    logger.info("No new rows to insert after processing")
            else:
                logger.debug(f"No new data available. Waiting {POLL_INTERVAL} seconds...")
            
            # Wait before next poll
            time.sleep(POLL_INTERVAL)
            
        except KeyboardInterrupt:
            logger.info("Received interrupt signal. Shutting down gracefully...")
            break
        except Exception as e:
            logger.error(f"Error in processing loop: {e}")
            logger.info(f"Retrying in {POLL_INTERVAL} seconds...")
            time.sleep(POLL_INTERVAL)
            
            # Try to reconnect if connection was lost
            try:
                cursor.execute("SELECT 1")
            except:
                logger.warning("Database connection lost. Reconnecting...")
                conn.close()
                conn = get_connection()
                if conn:
                    cursor = conn.cursor()
                    logger.info("Reconnected to database")
                else:
                    logger.error("Failed to reconnect. Exiting.")
                    break

except KeyboardInterrupt:
    logger.info("Interrupted by user")
finally:
    if conn:
        conn.close()
        logger.info("Database connection closed")
    logger.info("Live analysis stopped.")