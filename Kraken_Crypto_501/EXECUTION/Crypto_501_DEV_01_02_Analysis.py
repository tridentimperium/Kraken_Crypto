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

# Suppress the pandas SQLAlchemy warning - we're using pyodbc which works fine
warnings.filterwarnings('ignore', message='.*SQLAlchemy connectable.*')
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')

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
POLL_INTERVAL = _get_val("PollInterval", 1, int)

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
    """Identify swing highs and lows - REAL-TIME VERSION (only looks backward)"""
    high = df['High']
    low = df['Low']
    
    # Initialize with existing values if they exist, otherwise False
    if 'IsSwingHigh' in df.columns:
        is_swing_high = df['IsSwingHigh'].tolist()
    else:
        is_swing_high = [False] * len(df)
    
    if 'IsSwingLow' in df.columns:
        is_swing_low = df['IsSwingLow'].tolist()
    else:
        is_swing_low = [False] * len(df)
    
    # Find the last swing indices from existing data
    last_swing_high_idx = None
    last_swing_low_idx = None
    
    for i in range(len(df)):
        if is_swing_high[i]:
            last_swing_high_idx = i
        if is_swing_low[i]:
            last_swing_low_idx = i
    
    # Only recalculate for rows that don't have swings yet or might be new
    for i in range(lookback, len(df)):
        # Only recalculate if this position doesn't already have a swing marked
        if not is_swing_high[i]:
            if high.iloc[i] >= high.iloc[i - lookback:i].max():
                if last_swing_high_idx is None or (i - last_swing_high_idx) >= (lookback // 2):
                    is_swing_high[i] = True
                    last_swing_high_idx = i
        
        if not is_swing_low[i]:
            if low.iloc[i] <= low.iloc[i - lookback:i].min():
                if last_swing_low_idx is None or (i - last_swing_low_idx) >= (lookback // 2):
                    is_swing_low[i] = True
                    last_swing_low_idx = i
    
    df['IsSwingHigh'] = is_swing_high
    df['IsSwingLow'] = is_swing_low
    
    # Preserve existing SwingType if it exists
    if 'SwingType' not in df.columns:
        df['SwingType'] = None
    
    return df

def assign_swing_types(df, enable_min_swing, min_swing_pct):
    """Assign swing types (HH, HL, LH, LL) with optional % filter"""
    swing_highs = df[df['IsSwingHigh']].copy()
    swing_lows = df[df['IsSwingLow']].copy()
    
    prev_high = None
    for idx in swing_highs.index:
        current = swing_highs.loc[idx, 'High']
        # Only recalculate if SwingType is not already set
        if pd.isna(df.loc[idx, 'SwingType']):
            if prev_high is None:
                df.loc[idx, 'SwingType'] = None
            else:
                pct_change = (current - prev_high) / prev_high * 100
                if enable_min_swing and abs(pct_change) < min_swing_pct:
                    df.loc[idx, 'SwingType'] = None
                else:
                    df.loc[idx, 'SwingType'] = 'HH' if current > prev_high else 'LH'
        prev_high = current
    
    prev_low = None
    for idx in swing_lows.index:
        current = swing_lows.loc[idx, 'Low']
        # Only recalculate if SwingType is not already set
        if pd.isna(df.loc[idx, 'SwingType']):
            if prev_low is None:
                df.loc[idx, 'SwingType'] = None
            else:
                pct_change = (prev_low - current) / prev_low * 100
                if enable_min_swing and abs(pct_change) < min_swing_pct:
                    df.loc[idx, 'SwingType'] = None
                else:
                    df.loc[idx, 'SwingType'] = 'LL' if current < prev_low else 'HL'
        prev_low = current
    
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
    
    # Assign swing types (includes MIN_SWING_PCT filter)
    df_new = assign_swing_types(df_new, ENABLE_MIN_SWING, MIN_SWING_PCT)
    
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
# UPSERT FUNCTION
# ================================
def insert_analysis_results(cursor, conn, df_results, last_processed_datetime):
    """Upsert analysis results - ONLY OHLCV for old rows, full update for latest row"""
    
    # Get existing data to preserve signals and check for changes
    existing_datetimes = [idx for idx in df_results.index]
    existing_data = {}
    
    if existing_datetimes:
        placeholders = ','.join(['?'] * len(existing_datetimes))
        check_query = f"""
        SELECT DateTime, [Open], [High], [Low], [Close], Volume,
               IsSwingHigh, IsSwingLow, SwingType, BuySignal, SellSignal
        FROM {ANALYSIS_TABLE}
        WHERE DateTime IN ({placeholders})
        """
        cursor.execute(check_query, existing_datetimes)
        for row in cursor.fetchall():
            existing_data[row[0]] = {
                'Open': row[1], 'High': row[2], 'Low': row[3], 'Close': row[4], 'Volume': row[5],
                'IsSwingHigh': row[6], 'IsSwingLow': row[7], 'SwingType': row[8],
                'BuySignal': row[9], 'SellSignal': row[10]
            }
    
    # SQL for OHLCV-only update (for old rows)
    update_ohlcv_sql = f"""
    UPDATE {ANALYSIS_TABLE}
    SET [Open] = ?, [High] = ?, [Low] = ?, [Close] = ?, Volume = ?
    WHERE DateTime = ? AND Symbol = ?
    """
    
    # SQL for full MERGE (latest row + new rows)
    merge_sql = f"""
    MERGE {ANALYSIS_TABLE} AS target
    USING (SELECT ? AS DateTime_EST, ? AS DateTime, ? AS Timeframe, ? AS Symbol, ? AS [Open], ? AS [High], 
                  ? AS [Low], ? AS [Close], ? AS Volume, ? AS N001, ? AS IsSwingHigh, ? AS IsSwingLow,
                  ? AS SwingType, ? AS Slope, ? AS N002, ? AS Trend, ? AS N003, ? AS Entry, ? AS EntryCount,
                  ? AS TargetDirection, ? AS L_PTPercent, ? AS L_SLPercent, ? AS L_PTPrice, ? AS L_SLPrice,
                  ? AS S_PTPercent, ? AS S_SLPercent, ? AS S_PTPrice, ? AS S_SLPrice, ? AS N004, ? AS EntryExit,
                  ? AS BuySignal, ? AS SellSignal, ? AS LongShort, ? AS InTrade, ? AS N005,
                  ? AS StartingBalance, ? AS Leverage, ? AS Quantity, ? AS EntryPrice, ? AS EntryCost,
                  ? AS ExitPrice, ? AS ExitCost, ? AS ProfitLoss, ? AS EndingBalance) AS source
    ON target.DateTime = source.DateTime AND target.Symbol = source.Symbol
    WHEN MATCHED THEN
        UPDATE SET DateTime_EST=source.DateTime_EST, Timeframe=source.Timeframe,
            [Open]=source.[Open], [High]=source.[High], [Low]=source.[Low], [Close]=source.[Close], Volume=source.Volume,
            N001=source.N001, IsSwingHigh=source.IsSwingHigh, IsSwingLow=source.IsSwingLow, SwingType=source.SwingType,
            Slope=source.Slope, N002=source.N002, Trend=source.Trend, N003=source.N003, Entry=source.Entry,
            EntryCount=source.EntryCount, TargetDirection=source.TargetDirection, L_PTPercent=source.L_PTPercent,
            L_SLPercent=source.L_SLPercent, L_PTPrice=source.L_PTPrice, L_SLPrice=source.L_SLPrice,
            S_PTPercent=source.S_PTPercent, S_SLPercent=source.S_SLPercent, S_PTPrice=source.S_PTPrice,
            S_SLPrice=source.S_SLPrice, N004=source.N004, EntryExit=source.EntryExit, BuySignal=source.BuySignal,
            SellSignal=source.SellSignal, LongShort=source.LongShort, InTrade=source.InTrade, N005=source.N005,
            StartingBalance=source.StartingBalance, Leverage=source.Leverage, Quantity=source.Quantity,
            EntryPrice=source.EntryPrice, EntryCost=source.EntryCost, ExitPrice=source.ExitPrice,
            ExitCost=source.ExitCost, ProfitLoss=source.ProfitLoss, EndingBalance=source.EndingBalance
    WHEN NOT MATCHED THEN
        INSERT (DateTime_EST, DateTime, Timeframe, Symbol, [Open], [High], [Low], [Close], Volume, N001,
                IsSwingHigh, IsSwingLow, SwingType, Slope, N002, Trend, N003, Entry, EntryCount, TargetDirection,
                L_PTPercent, L_SLPercent, L_PTPrice, L_SLPrice, S_PTPercent, S_SLPercent, S_PTPrice, S_SLPrice,
                N004, EntryExit, BuySignal, SellSignal, LongShort, InTrade, N005,
                StartingBalance, Leverage, Quantity, EntryPrice, EntryCost, ExitPrice, ExitCost, ProfitLoss, EndingBalance)
        VALUES (source.DateTime_EST, source.DateTime, source.Timeframe, source.Symbol, source.[Open], source.[High],
                source.[Low], source.[Close], source.Volume, source.N001, source.IsSwingHigh, source.IsSwingLow,
                source.SwingType, source.Slope, source.N002, source.Trend, source.N003, source.Entry, source.EntryCount,
                source.TargetDirection, source.L_PTPercent, source.L_SLPercent, source.L_PTPrice, source.L_SLPrice,
                source.S_PTPercent, source.S_SLPercent, source.S_PTPrice, source.S_SLPrice, source.N004, source.EntryExit,
                source.BuySignal, source.SellSignal, source.LongShort, source.InTrade, source.N005,
                source.StartingBalance, source.Leverage, source.Quantity, source.EntryPrice, source.EntryCost,
                source.ExitPrice, source.ExitCost, source.ProfitLoss, source.EndingBalance);
    """
    
    rows = 0
    new_rows = []
    updated_count = 0
    latest_row_idx = df_results.index[-1] if not df_results.empty else None
    
    try:
        for idx, row in df_results.iterrows():
            is_new = last_processed_datetime is None or idx > last_processed_datetime
            is_latest = (idx == latest_row_idx)
            
            # Check for OHLCV changes
            values_changed = False
            if idx in existing_data:
                old = existing_data[idx]
                if (old['Open'] != (None if pd.isna(row['Open']) else float(row['Open'])) or
                    old['High'] != (None if pd.isna(row['High']) else float(row['High'])) or
                    old['Low'] != (None if pd.isna(row['Low']) else float(row['Low'])) or
                    old['Close'] != (None if pd.isna(row['Close']) else float(row['Close'])) or
                    old['Volume'] != (None if pd.isna(row['Volume']) else float(row['Volume']))):
                    values_changed = True
            
            if is_new:
                # NEW ROW - full insert
                cursor.execute(merge_sql,
                    row['DateTime_EST'], idx, row['Timeframe'], row['Symbol'],
                    None if pd.isna(row['Open']) else float(row['Open']),
                    None if pd.isna(row['High']) else float(row['High']),
                    None if pd.isna(row['Low']) else float(row['Low']),
                    None if pd.isna(row['Close']) else float(row['Close']),
                    None if pd.isna(row['Volume']) else float(row['Volume']),
                    None if pd.isna(row.get('N001', np.nan)) else float(row['N001']),
                    1 if row['IsSwingHigh'] else 0, 1 if row['IsSwingLow'] else 0,
                    row['SwingType'] if pd.notna(row['SwingType']) else None,
                    None if pd.isna(row['Slope']) else float(row['Slope']),
                    None if pd.isna(row.get('N002', np.nan)) else float(row['N002']),
                    row['Trend'], None if pd.isna(row.get('N003', np.nan)) else float(row['N003']),
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
                    1 if row['BuySignal'] else 0, 1 if row['SellSignal'] else 0,
                    row['LongShort'] if pd.notna(row.get('LongShort')) else None,
                    1 if row['InTrade'] else 0, None if pd.isna(row.get('N005', np.nan)) else float(row['N005']),
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
                new_rows.append((idx, row['DateTime_EST']))
            elif is_latest:
                # LATEST ROW - full update with signal preservation
                old = existing_data.get(idx, {})
                swing_high = 1 if (old.get('IsSwingHigh') == 1 or row['IsSwingHigh']) else 0
                swing_low = 1 if (old.get('IsSwingLow') == 1 or row['IsSwingLow']) else 0
                buy_sig = 1 if (old.get('BuySignal') == 1 or row['BuySignal']) else 0
                sell_sig = 1 if (old.get('SellSignal') == 1 or row['SellSignal']) else 0
                # SwingType: once set, never changes
                swing_type = old.get('SwingType') if old.get('SwingType') is not None else (row['SwingType'] if pd.notna(row['SwingType']) else None)
                
                cursor.execute(merge_sql,
                    row['DateTime_EST'], idx, row['Timeframe'], row['Symbol'],
                    None if pd.isna(row['Open']) else float(row['Open']),
                    None if pd.isna(row['High']) else float(row['High']),
                    None if pd.isna(row['Low']) else float(row['Low']),
                    None if pd.isna(row['Close']) else float(row['Close']),
                    None if pd.isna(row['Volume']) else float(row['Volume']),
                    None if pd.isna(row.get('N001', np.nan)) else float(row['N001']),
                    swing_high, swing_low,  # PRESERVED
                    swing_type,  # PRESERVED - once set, never changes
                    None if pd.isna(row['Slope']) else float(row['Slope']),  # CAN CHANGE
                    None if pd.isna(row.get('N002', np.nan)) else float(row['N002']),
                    row['Trend'],  # CAN CHANGE
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
                    buy_sig, sell_sig,  # PRESERVED
                    row['LongShort'] if pd.notna(row.get('LongShort')) else None,
                    1 if row['InTrade'] else 0, None if pd.isna(row.get('N005', np.nan)) else float(row['N005']),
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
                if values_changed:
                    updated_count += 1
            else:
                # OLD ROW - OHLCV only
                cursor.execute(update_ohlcv_sql,
                    None if pd.isna(row['Open']) else float(row['Open']),
                    None if pd.isna(row['High']) else float(row['High']),
                    None if pd.isna(row['Low']) else float(row['Low']),
                    None if pd.isna(row['Close']) else float(row['Close']),
                    None if pd.isna(row['Volume']) else float(row['Volume']),
                    idx, row['Symbol']
                )
                rows += 1
                if values_changed:
                    updated_count += 1
        
        conn.commit()
        return rows, new_rows, updated_count
    except Exception as e:
        logger.error(f"Upsert failed: {e}")
        conn.rollback()
        return 0, [], 0

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
            # Query for new data from LIVE table AND historical analysis results
            if last_processed_datetime:
                # Get historical analysis results for context
                historical_query = f"""
                SELECT TOP {max(LOOKBACK * 3, TREND_LINE_RANGE + 10)}
                       DateTime, DateTime_EST, Timeframe, Symbol, [Open], [High], [Low], [Close], Volume,
                       IsSwingHigh, IsSwingLow, SwingType, Slope, Trend
                FROM {ANALYSIS_TABLE}
                WHERE DateTime < ?
                ORDER BY DateTime DESC
                """
                df_historical = pd.read_sql(historical_query, conn, params=[last_processed_datetime])
                df_historical = df_historical.iloc[::-1]  # Reverse to chronological order
                df_historical.set_index('DateTime', inplace=True)
                
                # Get new/updated data from live table (last 5 + any newer)
                live_query = f"""
                SELECT DateTime, DateTime_EST, Timeframe, Symbol, [Open], [High], [Low], [Close], Volume
                FROM {LIVE_DATA_TABLE}
                WHERE DateTime >= (
                    SELECT MIN(DateTime) FROM (
                        SELECT TOP 5 DateTime 
                        FROM {LIVE_DATA_TABLE}
                        WHERE DateTime <= ?
                        ORDER BY DateTime DESC
                    ) AS Last5
                )
                ORDER BY DateTime
                """
                df_live = pd.read_sql(live_query, conn, params=[last_processed_datetime])
                df_live.set_index('DateTime', inplace=True)
                
                # Combine: start with historical, then update/add from live
                df_new = df_historical.copy()
                
                for idx in df_live.index:
                    if idx in df_new.index:
                        # Update existing row with new OHLCV
                        df_new.loc[idx, ['DateTime_EST', 'Timeframe', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume']] = \
                            df_live.loc[idx, ['DateTime_EST', 'Timeframe', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume']]
                    else:
                        # New row - add it with just OHLCV data
                        new_row = df_live.loc[[idx]].copy()
                        # Add missing columns
                        new_row['IsSwingHigh'] = False
                        new_row['IsSwingLow'] = False
                        new_row['SwingType'] = None
                        new_row['Slope'] = np.nan
                        new_row['Trend'] = None
                        df_new = pd.concat([df_new, new_row])
                
                df_new = df_new.sort_index()
            else:
                # First run - get all data
                query = f"""
                SELECT DateTime, DateTime_EST, Timeframe, Symbol, [Open], [High], [Low], [Close], Volume
                FROM {LIVE_DATA_TABLE}
                ORDER BY DateTime
                """
                df_new = pd.read_sql(query, conn)
                df_new.set_index('DateTime', inplace=True)
            
            if not df_new.empty:
                
                # Process the data
                df_processed = process_new_data(df_new, entry_str, target_direction_str)
                
                # Extract rows for upsert
                # Include the last 5 processed rows (they may have been updated) and any newer rows
                if last_processed_datetime:
                    # Get the 5th row back from last_processed_datetime
                    all_datetimes = sorted(df_processed.index)
                    if last_processed_datetime in all_datetimes:
                        last_idx = all_datetimes.index(last_processed_datetime)
                        start_idx = max(0, last_idx - 4)  # Go back 4 rows (total of 5 including current)
                        cutoff_datetime = all_datetimes[start_idx]
                    else:
                        cutoff_datetime = all_datetimes[0]
                    df_to_insert = df_processed[df_processed.index >= cutoff_datetime]
                else:
                    df_to_insert = df_processed
                
                # Upsert results
                if not df_to_insert.empty:
                    rows_total, new_rows, updated_count = insert_analysis_results(cursor, conn, df_to_insert, last_processed_datetime)
                    
                    if rows_total > 0:
                        # Log new rows
                        for dt_utc, dt_est in new_rows:
                            logger.info(f"New Row: {dt_est} EST ({dt_utc} UTC)")
                        
                        # Log updates
                        if updated_count > 0:
                            logger.info(f"Row Updated...")
                        
                        # Update last processed datetime (only if we have new rows)
                        if new_rows:
                            last_processed_datetime = df_to_insert.index[-1]
                            last_processed_datetime_est = df_to_insert['DateTime_EST'].iloc[-1]
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