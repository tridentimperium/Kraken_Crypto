import sys
import os
import pyodbc
import logging
import pandas as pd
import numpy as np
import json
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
logger.info("--- STARTING RESULTS ANALYSIS GENERATION ---")

# ================================
# PATHS
# ================================
EXECUTION_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_PATH = os.path.dirname(EXECUTION_DIR)
CONFIG_PATH = os.path.join(BASE_PATH, "CONFIG")

# ================================
# TABLE NAMES
# ================================
SOURCE_TABLE = "dbo.Crypto_005_DEV_01_04_Analysis_Backtest"
TARGET_TABLE = "dbo.Crypto_005_DEV_01_07_Results_Analysis"

# ================================
# LOAD CONFIG (batch or standalone)
# ================================
vars_config = {}
if len(sys.argv) > 1:
    try:
        vars_config = json.loads(sys.argv[1])
        logger.info("Loaded config from batch (JSON argument)")
    except Exception as e:
        logger.error(f"Failed to parse JSON argument: {e}")
        sys.exit(1)
else:
    variables_file = os.path.join(CONFIG_PATH, "ZZ_VARIABLES", "Crypto_005_variables.json")
    if not os.path.exists(variables_file):
        logger.error(f"Variables file not found: {variables_file}")
        sys.exit(1)
    with open(variables_file, 'r', encoding='utf-8') as f:
        vars_config = json.load(f)
    logger.info("Loaded config from Crypto_005_variables.json (standalone)")

# Extract IDs with defaults
ANALYSIS_RUN_ID = int(vars_config.get("AnalysisRunID", 1))
FETCH_RUN_ID = int(vars_config.get("FetchRunID", 1))

logger.info(f"Using AnalysisRunID = {ANALYSIS_RUN_ID}, FetchRunID = {FETCH_RUN_ID}")

# ================================
# SQL CONNECTION
# ================================
sql_env_file = os.path.join(CONFIG_PATH, "SQLSERVER", "Crypto_005_sqlserver_local.env")
if not os.path.exists(sql_env_file):
    sql_env_file = os.path.join(CONFIG_PATH, "SQLSERVER", "Crypto_005_sqlserver_remote.env")

if not os.path.exists(sql_env_file):
    logger.error(f"SQL env file not found: {sql_env_file}")
    sys.exit(1)

load_dotenv(sql_env_file, encoding='utf-8')
logger.info(f"Loaded SQL env: {sql_env_file}")

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
# CREATE TARGET TABLE (updated columns)
# ================================
create_target_sql = f'''
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_005_DEV_01_07_Results_Analysis')
BEGIN
    CREATE TABLE {TARGET_TABLE} (
        FetchRunID INT NOT NULL,
        AnalysisRunID INT NOT NULL,
        Timeframe VARCHAR(10) NOT NULL,
        Symbol NVARCHAR(50) NOT NULL,
        N001 FLOAT NULL,
        Entry NVARCHAR(10) NULL,
        EntryCount INT NULL,
        TargetDirection NVARCHAR(20) NULL,
        N002 FLOAT NULL,
        L_PTPercent DECIMAL(10,2) NULL,
        L_SLPercent DECIMAL(10,2) NULL,
        S_PTPercent DECIMAL(10,2) NULL,
        S_SLPercent DECIMAL(10,2) NULL,
        N003 FLOAT NULL,
        Position VARCHAR(10) NOT NULL,
        PL_Type VARCHAR(10) NOT NULL,
        ProfitExecutionNumber INT NULL,
        LossExecutionNumber INT NULL,
        AverageProfit DECIMAL(10,2) NULL,
        AverageLoss DECIMAL(10,2) NULL,
        MaxProfit DECIMAL(10,2) NULL,
        MinProfit DECIMAL(10,2) NULL,
        MaxLoss DECIMAL(10,2) NULL,
        MinLoss DECIMAL(10,2) NULL,
        N004 FLOAT NULL,
        ProfitPercentage DECIMAL(5,2) NULL,
        LossPercentage DECIMAL(5,2) NULL,
        N005 FLOAT NULL,
        Profit DECIMAL(10,2) NULL,
        Loss DECIMAL(10,2) NULL,
        PositionPL DECIMAL(10,2) NULL,
        AnalysisPL DECIMAL(10,2) NULL,
        PRIMARY KEY (FetchRunID, AnalysisRunID, Position, PL_Type)
    );
END
'''
cursor.execute(create_target_sql)
conn.commit()
logger.info(f"Target table ensured: {TARGET_TABLE}")

# ================================
# LOAD BACKTEST DATA - Only rows with EntryExit = 2
# ================================
query = f"""
SELECT 
    FetchRunID, Timeframe, Symbol, Entry, EntryCount, TargetDirection,
    L_PTPercent, L_SLPercent, S_PTPercent, S_SLPercent,
    LongShort AS Position,
    ProfitLoss
FROM {SOURCE_TABLE}
WHERE AnalysisRunID = ? AND FetchRunID = ? AND EntryExit = 2
ORDER BY FetchRunID, AnalysisRunID, DateTime
"""
try:
    df = pd.read_sql(query, conn, params=[ANALYSIS_RUN_ID, FETCH_RUN_ID])
    logger.info(f"Loaded {len(df)} completed trades (EntryExit = 2).")
except Exception as e:
    logger.error(f"Failed to load data: {e}")
    conn.close()
    sys.exit(1)

if df.empty:
    logger.info("No completed trades found. Nothing to aggregate.")
    conn.close()
    sys.exit(0)

# Determine PL_Type based on actual profit/loss
df['PL_Type'] = np.where(df['ProfitLoss'] > 0, 'Profit', 'Loss')

logger.info(f"Data breakdown - Profit trades: {(df['PL_Type'] == 'Profit').sum()}, Loss trades: {(df['PL_Type'] == 'Loss').sum()}")

# ================================
# Calculate position totals for percentage calculation
# ================================
position_totals = df.groupby('Position').size().to_dict()
long_total = position_totals.get('Long', 0)
short_total = position_totals.get('Short', 0)

logger.info(f"Position totals - Long: {long_total}, Short: {short_total}")

# ================================
# CREATE ALL 4 COMBINATIONS (even if no data)
# ================================
combinations = [
    ('Long', 'Profit'),
    ('Long', 'Loss'),
    ('Short', 'Profit'),
    ('Short', 'Loss')
]

results = []

for position, pl_type in combinations:
    subset = df[(df['Position'] == position) & (df['PL_Type'] == pl_type)]
    
    total_exec = len(subset)
    
    if subset.empty:
        profit_exec = 0
        loss_exec = 0
        avg_profit = None
        avg_loss = None
        max_profit = None
        min_profit = None
        max_loss = None
        min_loss = None
        profit = 0.0
        loss = 0.0
    else:
        if pl_type == 'Profit':
            profit_exec = total_exec
            loss_exec = 0
            avg_profit = round(subset['ProfitLoss'].mean(), 2) if not subset['ProfitLoss'].isna().all() else None
            avg_loss = None
            max_profit = round(subset['ProfitLoss'].max(), 2) if not subset['ProfitLoss'].isna().all() else None
            min_profit = round(subset['ProfitLoss'].min(), 2) if not subset['ProfitLoss'].isna().all() else None
            max_loss = None
            min_loss = None
            profit = round(subset['ProfitLoss'].sum(), 2)
            loss = 0.0
        else:  # Loss
            profit_exec = 0
            loss_exec = total_exec
            avg_profit = None
            avg_loss = round(subset['ProfitLoss'].mean(), 2) if not subset['ProfitLoss'].isna().all() else None
            max_profit = None
            min_profit = None
            max_loss = round(subset['ProfitLoss'].max(), 2) if not subset['ProfitLoss'].isna().all() else None
            min_loss = round(subset['ProfitLoss'].min(), 2) if not subset['ProfitLoss'].isna().all() else None
            profit = 0.0
            loss = round(subset['ProfitLoss'].sum(), 2)

    # Common values from first row (or fallback)
    first_row = df.iloc[0] if not df.empty else pd.Series()

    # Calculate percentages based on position
    position_total = long_total if position == 'Long' else short_total
    
    if pl_type == 'Profit':
        profit_pct = round(profit_exec / position_total * 100, 2) if position_total > 0 else 0.0
        loss_pct = round((position_total - profit_exec) / position_total * 100, 2) if position_total > 0 else 0.0
    else:  # Loss
        profit_pct = round((position_total - loss_exec) / position_total * 100, 2) if position_total > 0 else 0.0
        loss_pct = round(loss_exec / position_total * 100, 2) if position_total > 0 else 0.0

    # Long-only or Short-only percentages
    l_pt = round(float(first_row['L_PTPercent']), 2) if pd.notna(first_row.get('L_PTPercent')) and position == 'Long' else None
    l_sl = round(float(first_row['L_SLPercent']), 2) if pd.notna(first_row.get('L_SLPercent')) and position == 'Long' else None
    s_pt = round(float(first_row['S_PTPercent']), 2) if pd.notna(first_row.get('S_PTPercent')) and position == 'Short' else None
    s_sl = round(float(first_row['S_SLPercent']), 2) if pd.notna(first_row.get('S_SLPercent')) and position == 'Short' else None

    results.append({
        'FetchRunID': FETCH_RUN_ID,
        'AnalysisRunID': ANALYSIS_RUN_ID,
        'Timeframe': first_row.get('Timeframe', 'Unknown'),
        'Symbol': first_row.get('Symbol', 'UNKNOWN'),
        'N001': None,
        'Entry': first_row.get('Entry'),
        'EntryCount': int(first_row['EntryCount']) if pd.notna(first_row.get('EntryCount')) else None,
        'TargetDirection': first_row.get('TargetDirection'),
        'N002': None,
        'L_PTPercent': l_pt,
        'L_SLPercent': l_sl,
        'S_PTPercent': s_pt,
        'S_SLPercent': s_sl,
        'N003': None,
        'Position': position,
        'PL_Type': pl_type,
        'ProfitExecutionNumber': profit_exec,
        'LossExecutionNumber': loss_exec,
        'AverageProfit': avg_profit,
        'AverageLoss': avg_loss,
        'MaxProfit': max_profit,
        'MinProfit': min_profit,
        'MaxLoss': max_loss,
        'MinLoss': min_loss,
        'N004': None,
        'ProfitPercentage': profit_pct,
        'LossPercentage': loss_pct,
        'N005': None,
        'Profit': profit,
        'Loss': loss,
        'PositionPL': profit + loss,
        'AnalysisPL': 0.0  # Will be calculated after all rows
    })

# Calculate AnalysisPL = sum of all PositionPL
analysis_pl = sum(r['PositionPL'] for r in results if r['PositionPL'] is not None)
for r in results:
    r['AnalysisPL'] = round(analysis_pl, 2)

df_result = pd.DataFrame(results)

logger.info(f"Generated {len(df_result)} aggregated result rows (should be 4).")

# ================================
# INSERT INTO TARGET TABLE (safe rounding + NaN handling)
# ================================
insert_sql = f"""
INSERT INTO {TARGET_TABLE}
(FetchRunID, AnalysisRunID, Timeframe, Symbol, N001, Entry, EntryCount, TargetDirection, N002,
 L_PTPercent, L_SLPercent, S_PTPercent, S_SLPercent, N003, Position, PL_Type,
 ProfitExecutionNumber, LossExecutionNumber,
 AverageProfit, AverageLoss, MaxProfit, MinProfit, MaxLoss, MinLoss,
 N004,
 ProfitPercentage, LossPercentage,
 N005,
 Profit, Loss, PositionPL, AnalysisPL)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

rows = 0
try:
    for _, row in df_result.iterrows():
        # Safe rounding function
        def safe_round(val, decimals=2):
            if pd.isna(val) or not np.isfinite(val):
                return None
            try:
                return round(float(val), decimals)
            except:
                return None

        values = [
            int(row['FetchRunID']) if pd.notna(row['FetchRunID']) else None,
            int(row['AnalysisRunID']) if pd.notna(row['AnalysisRunID']) else None,
            row['Timeframe'],
            row['Symbol'],
            None,  # N001
            row['Entry'],
            int(row['EntryCount']) if pd.notna(row['EntryCount']) else None,
            row['TargetDirection'],
            None,  # N002
            safe_round(row['L_PTPercent']),
            safe_round(row['L_SLPercent']),
            safe_round(row['S_PTPercent']),
            safe_round(row['S_SLPercent']),
            None,  # N003
            row['Position'],
            row['PL_Type'],
            int(row['ProfitExecutionNumber']) if pd.notna(row['ProfitExecutionNumber']) else None,
            int(row['LossExecutionNumber']) if pd.notna(row['LossExecutionNumber']) else None,
            safe_round(row['AverageProfit']),
            safe_round(row['AverageLoss']),
            safe_round(row['MaxProfit']),
            safe_round(row['MinProfit']),
            safe_round(row['MaxLoss']),
            safe_round(row['MinLoss']),
            None,  # N004
            safe_round(row['ProfitPercentage']),
            safe_round(row['LossPercentage']),
            None,  # N005
            safe_round(row['Profit']),
            safe_round(row['Loss']),
            safe_round(row['PositionPL']),
            safe_round(row['AnalysisPL'])
        ]

        cursor.execute(insert_sql, values)
        rows += 1

    conn.commit()
    logger.info(f"Successfully inserted {rows} aggregated result rows into {TARGET_TABLE}")
except Exception as e:
    logger.error(f"Insert failed: {e}")
    conn.rollback()
finally:
    conn.close()

logger.info("Results Analysis generation finished successfully.")