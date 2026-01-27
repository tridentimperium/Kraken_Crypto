import sys
import os
import pyodbc
import logging
import pandas as pd
import json
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
logger.info("--- STARTING PORTFOLIO SUMMARY GENERATION ---")

# ================================
# PATHS
# ================================
EXECUTION_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_PATH = os.path.dirname(EXECUTION_DIR)
CONFIG_PATH = os.path.join(BASE_PATH, "CONFIG")

# ================================
# TABLE NAMES
# ================================
SOURCE_TABLE = "dbo.Crypto_001_DEV_01_07_Results_Analysis"
TARGET_TABLE = "dbo.Crypto_001_DEV_01_09_Portfolio_Summary"

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
    variables_file = os.path.join(CONFIG_PATH, "ZZ_VARIABLES", "Crypto_001_variables.json")
    if not os.path.exists(variables_file):
        logger.error(f"Variables file not found: {variables_file}")
        sys.exit(1)
    with open(variables_file, 'r', encoding='utf-8') as f:
        vars_config = json.load(f)
    logger.info("Loaded config from Crypto_001_variables.json (standalone)")

# Extract IDs with defaults
ANALYSIS_RUN_ID = int(vars_config.get("AnalysisRunID", 1))
FETCH_RUN_ID = int(vars_config.get("FetchRunID", 1))

logger.info(f"Using AnalysisRunID = {ANALYSIS_RUN_ID}, FetchRunID = {FETCH_RUN_ID}")

# ================================
# LOAD PARAMETERS FROM Crypto_001_parameters.json
# ================================
parameters_file = os.path.join(CONFIG_PATH, "ZZ_PARAMETERS", "Crypto_001_parameters.json")
if not os.path.exists(parameters_file):
    logger.error(f"Parameters file not found: {parameters_file}")
    sys.exit(1)

try:
    with open(parameters_file, 'r', encoding='utf-8') as f:
        params = json.load(f)
    logger.info(f"Loaded parameters from {parameters_file}")
except Exception as e:
    logger.error(f"Failed to load parameters: {e}")
    sys.exit(1)

SYMBOL = params.get("Symbol_ID", "").strip().upper()
if not SYMBOL:
    logger.error("Symbol_ID missing in parameters.json")
    sys.exit(1)

STARTING_BALANCE = round(float(params.get("StartingBalance", 10000.0)), 2)
logger.info(f"Using Symbol = {SYMBOL}, StartingBalance = {STARTING_BALANCE:.2f}")

# ================================
# SQL CONNECTION
# ================================
sql_env_file = os.path.join(CONFIG_PATH, "SQLSERVER", "Crypto_001_sqlserver_local.env")
if not os.path.exists(sql_env_file):
    sql_env_file = os.path.join(CONFIG_PATH, "SQLSERVER", "Crypto_001_sqlserver_remote.env")

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
# CREATE TARGET TABLE
# ================================
create_target_sql = f'''
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_001_DEV_01_09_Portfolio_Summary')
BEGIN
    CREATE TABLE {TARGET_TABLE} (
        FetchRunID INT NOT NULL,
        AnalysisRunID INT NOT NULL,
        Symbol NVARCHAR(50) NOT NULL,
        N001 FLOAT NULL,
        TradeNumber INT NULL,
        N002 FLOAT NULL,
        StartingBalance DECIMAL(18,2) NULL,
        EndingBalance DECIMAL(18,2) NULL,
        PercentageChange DECIMAL(5,2) NULL,
        N003 FLOAT NULL,
        Position VARCHAR(10) NOT NULL,
        N004 FLOAT NULL,
        Profit DECIMAL(10,2) NULL,
        Loss DECIMAL(10,2) NULL,
        PositionPL DECIMAL(10,2) NULL,
        PositionEndingBalance DECIMAL(18,2) NULL,
        PositionPercentageChange DECIMAL(5,2) NULL,
        N005 FLOAT NULL,
        ProfitExecutionNumber INT NULL,
        LossExecutionNumber INT NULL,
        N006 FLOAT NULL,
        ProfitPercentage DECIMAL(5,2) NULL,
        LossPercentage DECIMAL(5,2) NULL,
        PRIMARY KEY (FetchRunID, AnalysisRunID, Position)
    );
END
'''
cursor.execute(create_target_sql)
conn.commit()
logger.info(f"Target table ensured: {TARGET_TABLE}")

# ================================
# LOAD AGGREGATED RESULTS
# ================================
query = f"""
SELECT 
    Position,
    ProfitExecutionNumber,
    LossExecutionNumber,
    Profit,
    Loss,
    PositionPL,
    ProfitPercentage,
    LossPercentage
FROM {SOURCE_TABLE}
WHERE FetchRunID = ? AND AnalysisRunID = ?
"""
try:
    df_source = pd.read_sql(query, conn, params=[FETCH_RUN_ID, ANALYSIS_RUN_ID])
    logger.info(f"Loaded {len(df_source)} rows from {SOURCE_TABLE}.")
except Exception as e:
    logger.error(f"Failed to load analysis results: {e}")
    conn.close()
    sys.exit(1)

# Always ensure we have one row per position (Long & Short)
positions = ['Long', 'Short']
df = pd.DataFrame({'Position': positions})

if not df_source.empty:
    # Aggregate in case there are multiple rows per position (though normally shouldn't be)
    df_agg = df_source.groupby('Position', as_index=False).agg({
        'ProfitExecutionNumber': 'sum',
        'LossExecutionNumber':   'sum',
        'Profit':                'sum',
        'Loss':                  'sum',
        'PositionPL':            'sum',
        'ProfitPercentage':      'max',
        'LossPercentage':        'max'
    })
    df = df.merge(df_agg, on='Position', how='left').fillna(0)
    logger.info("Aggregated data per position:")
    logger.info(df.to_string(index=False))
else:
    logger.warning("No data found in Results_Analysis → using zero values for both positions")
    df['ProfitExecutionNumber'] = 0
    df['LossExecutionNumber']   = 0
    df['Profit']                = 0.0
    df['Loss']                  = 0.0
    df['PositionPL']            = 0.0
    df['ProfitPercentage']      = 0.0
    df['LossPercentage']        = 0.0

# Compute portfolio-level totals
total_pl = df['PositionPL'].sum()
ending_balance = round(STARTING_BALANCE + total_pl, 2)
total_percentage_change = round(((ending_balance - STARTING_BALANCE) / STARTING_BALANCE * 100), 2) if STARTING_BALANCE != 0 else 0.0

total_trades = int(df['ProfitExecutionNumber'].sum() + df['LossExecutionNumber'].sum())

# ================================
# UPSERT USING MERGE
# ================================
merge_sql = f"""
MERGE INTO {TARGET_TABLE} AS target
USING (VALUES 
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
) AS source (
    FetchRunID, AnalysisRunID, Symbol, N001, TradeNumber, N002,
    StartingBalance, EndingBalance, PercentageChange, N003,
    Position, N004, Profit, Loss, PositionPL,
    PositionEndingBalance, PositionPercentageChange, N005,
    ProfitExecutionNumber, LossExecutionNumber, N006,
    ProfitPercentage, LossPercentage
)
ON target.FetchRunID = source.FetchRunID 
   AND target.AnalysisRunID = source.AnalysisRunID 
   AND target.Position = source.Position

WHEN MATCHED THEN
    UPDATE SET
        Symbol                      = source.Symbol,
        N001                        = source.N001,
        TradeNumber                 = source.TradeNumber,
        N002                        = source.N002,
        StartingBalance             = source.StartingBalance,
        EndingBalance               = source.EndingBalance,
        PercentageChange            = source.PercentageChange,
        N003                        = source.N003,
        N004                        = source.N004,
        Profit                      = source.Profit,
        Loss                        = source.Loss,
        PositionPL                  = source.PositionPL,
        PositionEndingBalance       = source.PositionEndingBalance,
        PositionPercentageChange    = source.PositionPercentageChange,
        N005                        = source.N005,
        ProfitExecutionNumber       = source.ProfitExecutionNumber,
        LossExecutionNumber         = source.LossExecutionNumber,
        N006                        = source.N006,
        ProfitPercentage            = source.ProfitPercentage,
        LossPercentage              = source.LossPercentage

WHEN NOT MATCHED THEN
    INSERT (
        FetchRunID, AnalysisRunID, Symbol, N001, TradeNumber, N002,
        StartingBalance, EndingBalance, PercentageChange, N003,
        Position, N004, Profit, Loss, PositionPL,
        PositionEndingBalance, PositionPercentageChange, N005,
        ProfitExecutionNumber, LossExecutionNumber, N006,
        ProfitPercentage, LossPercentage
    )
    VALUES (
        source.FetchRunID, source.AnalysisRunID, source.Symbol, source.N001, source.TradeNumber, source.N002,
        source.StartingBalance, source.EndingBalance, source.PercentageChange, source.N003,
        source.Position, source.N004, source.Profit, source.Loss, source.PositionPL,
        source.PositionEndingBalance, source.PositionPercentageChange, source.N005,
        source.ProfitExecutionNumber, source.LossExecutionNumber, source.N006,
        source.ProfitPercentage, source.LossPercentage
    );
"""

for _, row in df.iterrows():
    position = row['Position']
    
    position_pl = round(float(row['PositionPL']), 2)
    position_ending = round(STARTING_BALANCE + position_pl, 2)
    position_pct_change = round(((position_ending - STARTING_BALANCE) / STARTING_BALANCE * 100), 2) if STARTING_BALANCE != 0 else 0.0

    values = (
        int(FETCH_RUN_ID),
        int(ANALYSIS_RUN_ID),
        SYMBOL,
        None,
        int(total_trades),
        None,
        float(STARTING_BALANCE),
        float(ending_balance),
        float(total_percentage_change),
        None,
        position,
        None,
        float(row['Profit']),
        float(row['Loss']),
        float(position_pl),
        float(position_ending),
        float(position_pct_change),
        None,
        int(row['ProfitExecutionNumber']),
        int(row['LossExecutionNumber']),
        None,
        float(row['ProfitPercentage']),
        float(row['LossPercentage'])
    )

    try:
        cursor.execute(merge_sql, values)
    except Exception as e:
        logger.error(f"MERGE failed for position {position}: {e}")
        conn.rollback()
        conn.close()
        sys.exit(1)

conn.commit()
logger.info(f"Successfully upserted portfolio summary rows (Long & Short) into {TARGET_TABLE}")

conn.close()
logger.info("Portfolio Summary generation finished successfully.")