import sys
import os
import pyodbc
import logging
import pandas as pd
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
logger.info("--- STARTING PORTFOLIO BALANCE GENERATION ---")

# ================================
# PATHS
# ================================
EXECUTION_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_PATH = os.path.dirname(EXECUTION_DIR)
CONFIG_PATH = os.path.join(BASE_PATH, "CONFIG")

# ================================
# TABLE NAMES
# ================================
SOURCE_TABLE = "dbo.Crypto_008_DEV_01_06_Entry_Exit_Order"
TARGET_TABLE = "dbo.Crypto_008_DEV_01_08_Portfolio_Balance"

# ================================
# LOAD CONFIG
# ================================
vars_config = None
if len(sys.argv) > 1:
    try:
        json_config_string = sys.argv[1]
        vars_config = json.loads(json_config_string)
        logger.info("Loaded variables from command-line argument (Batch Mode).")
    except Exception as e:
        logger.error(f"FATAL ERROR loading config: {e}")
        sys.exit(1)
else:
    variables_file = os.path.join(CONFIG_PATH, "ZZ_VARIABLES", "Crypto_008_variables.json")
    if not os.path.exists(variables_file):
        logger.error(f"Variables file not found: {variables_file}")
        sys.exit(1)
    with open(variables_file, 'r', encoding='utf-8') as f:
        vars_config = json.load(f)
    logger.info("Loaded variables from file (Standalone Mode).")

FETCH_RUN_ID = int(vars_config.get("FetchRunID", 1))
ANALYSIS_RUN_ID = int(vars_config.get("AnalysisRunID", 1))
logger.info(f"Using FetchRunID = {FETCH_RUN_ID}, AnalysisRunID = {ANALYSIS_RUN_ID}")

# ================================
# SQL CONNECTION
# ================================
sql_env_file = os.path.join(CONFIG_PATH, "SQLSERVER", "Crypto_008_sqlserver_local.env")
if not os.path.exists(sql_env_file):
    sql_env_file = os.path.join(CONFIG_PATH, "SQLSERVER", "Crypto_008_sqlserver_remote.env")
load_dotenv(sql_env_file, encoding='utf-8')

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

# ================================
# CREATE / ALTER TARGET TABLE → DECIMAL(18,2)
# ================================
create_or_alter_sql = f'''
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_008_DEV_01_08_Portfolio_Balance')
BEGIN
    CREATE TABLE {TARGET_TABLE} (
        FetchRunID       INT           NOT NULL,
        AnalysisRunID    INT           NOT NULL,
        Symbol           NVARCHAR(50)  NOT NULL,
        N001             FLOAT         NULL,
        ExecutionDate    DATE          NOT NULL,
        TradeNumber      INT           NULL,
        N002             FLOAT         NULL,
        StartingBalance  DECIMAL(18,2) NULL,
        EndingBalance    DECIMAL(18,2) NULL,
        PercentageChange DECIMAL(18,2) NULL,
        PRIMARY KEY (FetchRunID, AnalysisRunID, Symbol, ExecutionDate)
    );
END
ELSE
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'Crypto_008_DEV_01_08_Portfolio_Balance' 
          AND COLUMN_NAME = 'StartingBalance' 
          AND DATA_TYPE = 'decimal'
    )
    BEGIN
        ALTER TABLE {TARGET_TABLE}
        ALTER COLUMN StartingBalance DECIMAL(18,2) NULL;
    END

    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'Crypto_008_DEV_01_08_Portfolio_Balance' 
          AND COLUMN_NAME = 'EndingBalance' 
          AND DATA_TYPE = 'decimal'
    )
    BEGIN
        ALTER TABLE {TARGET_TABLE}
        ALTER COLUMN EndingBalance DECIMAL(18,2) NULL;
    END

    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'Crypto_008_DEV_01_08_Portfolio_Balance' 
          AND COLUMN_NAME = 'PercentageChange' 
          AND DATA_TYPE = 'decimal'
    )
    BEGIN
        ALTER TABLE {TARGET_TABLE}
        ALTER COLUMN PercentageChange DECIMAL(18,2) NULL;
    END
END
'''
cursor.execute(create_or_alter_sql)
conn.commit()
logger.info(f"Target table ensured with DECIMAL(18,2): {TARGET_TABLE}")

# ================================
# LOAD DATA
# ================================
query = f"""
SELECT FetchRunID, AnalysisRunID, Symbol, DateTime, EntryExit, StartingBalance, EndingBalance
FROM {SOURCE_TABLE}
WHERE EntryExit IN (1.0, 2.0)
  AND FetchRunID = ?
  AND AnalysisRunID = ?
ORDER BY FetchRunID, AnalysisRunID, DateTime
"""
df_orders = pd.read_sql(query, conn, params=[FETCH_RUN_ID, ANALYSIS_RUN_ID])

if df_orders.empty:
    logger.info("No entry/exit orders found. Exiting.")
    conn.close()
    sys.exit(0)

df_orders['DateTime'] = pd.to_datetime(df_orders['DateTime'])
df_orders['ExecutionDate'] = df_orders['DateTime'].dt.date

# ================================
# GENERATE DAILY BALANCES
# ================================
all_rows = []

for symbol in df_orders['Symbol'].unique():
    df_sym = df_orders[df_orders['Symbol'] == symbol].sort_values('DateTime')
    if df_sym.empty:
        continue

    start_date = df_sym['ExecutionDate'].min()
    end_date   = df_sym['ExecutionDate'].max()
    all_dates  = pd.date_range(start=start_date, end=end_date, freq='D').date

    initial = round(float(df_sym.iloc[0]['StartingBalance']), 2)

    exits = df_sym[df_sym['EntryExit'] == 2.0]
    if exits.empty:
        df_daily = pd.DataFrame({
            'ExecutionDate': all_dates,
            'trade_number': 0,
            'balance': initial
        })
        df_daily['starting_balance'] = initial
        df_daily['ending_balance']   = initial
    else:
        grouped = exits.groupby('ExecutionDate').agg(
            trade_number=('EntryExit', 'count'),
            ending_balance=('EndingBalance', 'last')
        ).reset_index()

        df_all = pd.DataFrame({'ExecutionDate': all_dates})
        df_daily = df_all.merge(grouped, on='ExecutionDate', how='left')

        df_daily['ending_balance'] = df_daily['ending_balance'].ffill().fillna(initial)
        df_daily['starting_balance'] = df_daily['ending_balance'].shift(1).fillna(initial)

    # Round balances
    df_daily['starting_balance'] = df_daily['starting_balance'].round(2)
    df_daily['ending_balance']   = df_daily['ending_balance'].round(2)

    # Percentage change × 100 and round to 2 decimals
    df_daily['pct'] = (
        ((df_daily['ending_balance'] - df_daily['starting_balance']) /
         df_daily['starting_balance'].replace(0, pd.NA)) * 100
    ).fillna(0).round(2)

    df_daily['FetchRunID']       = FETCH_RUN_ID
    df_daily['AnalysisRunID']    = ANALYSIS_RUN_ID
    df_daily['Symbol']           = symbol
    df_daily['N001']             = None
    df_daily['TradeNumber']      = df_daily['trade_number'].astype('Int64')
    df_daily['N002']             = None
    df_daily['StartingBalance']  = df_daily['starting_balance']
    df_daily['EndingBalance']    = df_daily['ending_balance']
    df_daily['PercentageChange'] = df_daily['pct']

    all_rows.extend(df_daily[[
        'FetchRunID','AnalysisRunID','Symbol','N001','ExecutionDate',
        'TradeNumber','N002','StartingBalance','EndingBalance','PercentageChange'
    ]].to_dict('records'))

# ================================
# BULK INSERT
# ================================
insert_sql = f"""
INSERT INTO {TARGET_TABLE} (
    FetchRunID, AnalysisRunID, Symbol, N001, ExecutionDate,
    TradeNumber, N002, StartingBalance, EndingBalance, PercentageChange
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

if all_rows:
    data = [
        (
            r['FetchRunID'], r['AnalysisRunID'], r['Symbol'], r['N001'], r['ExecutionDate'],
            r['TradeNumber'], r['N002'],
            r['StartingBalance'], r['EndingBalance'], r['PercentageChange']
        )
        for r in all_rows
    ]

    chunk_size = 20000
    inserted = 0
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i+chunk_size]
        cursor.executemany(insert_sql, chunk)
        inserted += len(chunk)
        logger.info(f"Inserted {len(chunk):,} rows")

    conn.commit()
    logger.info(f"Total inserted: {inserted:,} rows")
else:
    logger.info("No rows generated")

conn.close()
logger.info("Finished.")