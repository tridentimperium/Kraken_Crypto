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
SOURCE_TABLE = "dbo.Crypto_001_DEV_01_06_Entry_Exit_Order"
TARGET_TABLE = "dbo.Crypto_001_DEV_01_08_Portfolio_Balance"

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
FETCH_RUN_ID = int(vars_config.get("FetchRunID", 1))
ANALYSIS_RUN_ID = int(vars_config.get("AnalysisRunID", 1))

logger.info(f"Using FetchRunID = {FETCH_RUN_ID}, AnalysisRunID = {ANALYSIS_RUN_ID}")

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
# CREATE TARGET TABLE (FetchRunID first)
# ================================
create_target_sql = f'''
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_001_DEV_01_08_Portfolio_Balance')
BEGIN
    CREATE TABLE {TARGET_TABLE} (
        FetchRunID INT NOT NULL,
        AnalysisRunID INT NOT NULL,
        Symbol NVARCHAR(50) NOT NULL,
        N001 FLOAT NULL,
        ExecutionDate DATE NOT NULL,
        TradeNumber INT NULL,
        N002 FLOAT NULL,
        StartingBalance FLOAT NULL,
        EndingBalance FLOAT NULL,
        PercentageChange FLOAT NULL,
        PRIMARY KEY (FetchRunID, AnalysisRunID, Symbol, ExecutionDate)
    );
END
'''
cursor.execute(create_target_sql)
conn.commit()
logger.info(f"Target table ensured: {TARGET_TABLE}")

# ================================
# LOAD ENTRY/EXIT ORDERS (filtered and ordered)
# ================================
query = f"""
SELECT 
    FetchRunID, AnalysisRunID, Symbol, DateTime, EntryExit, StartingBalance, EndingBalance
FROM {SOURCE_TABLE}
WHERE EntryExit IN (1.0, 2.0)
  AND FetchRunID = ?
  AND AnalysisRunID = ?
ORDER BY FetchRunID, AnalysisRunID, DateTime
"""
try:
    df_orders = pd.read_sql(query, conn, params=[FETCH_RUN_ID, ANALYSIS_RUN_ID])
    logger.info(f"Loaded {len(df_orders)} entry/exit rows.")
except Exception as e:
    logger.error(f"Failed to load data: {e}")
    conn.close()
    sys.exit(1)

if df_orders.empty:
    logger.info("No entry/exit orders found. Nothing to process.")
    conn.close()
    sys.exit(0)

df_orders['DateTime'] = pd.to_datetime(df_orders['DateTime'])
df_orders['ExecutionDate'] = df_orders['DateTime'].dt.date

# ================================
# GENERATE DAILY PORTFOLIO BALANCE
# ================================
all_daily_rows = []

for symbol in df_orders['Symbol'].unique():
    df_sym = df_orders[df_orders['Symbol'] == symbol].sort_values('DateTime').copy()
    
    start_date = df_sym['ExecutionDate'].min()
    end_date = df_sym['ExecutionDate'].max()
    all_dates = pd.date_range(start=start_date, end=end_date, freq='D').date
    
    current_balance = df_sym.iloc[0]['StartingBalance']

    for day in all_dates:
        day_trades = df_sym[df_sym['ExecutionDate'] == day]
        
        starting_balance = current_balance
        ending_balance = current_balance
        trade_number = None
        
        if not day_trades.empty:
            exit_rows = day_trades[day_trades['EntryExit'] == 2.0]
            if not exit_rows.empty:
                trade_number = len(exit_rows)
                ending_balance = exit_rows.iloc[-1]['EndingBalance']
                current_balance = ending_balance
        
        pct_change = round(((ending_balance - starting_balance) / starting_balance * 100), 2) if starting_balance != 0 else 0.0
        
        all_daily_rows.append({
            'FetchRunID': FETCH_RUN_ID,
            'AnalysisRunID': ANALYSIS_RUN_ID,
            'Symbol': symbol,
            'N001': None,
            'ExecutionDate': day,
            'TradeNumber': trade_number,
            'N002': None,
            'StartingBalance': round(starting_balance, 2),
            'EndingBalance': round(ending_balance, 2),
            'PercentageChange': pct_change
        })

df_daily = pd.DataFrame(all_daily_rows)
logger.info(f"Generated {len(df_daily)} daily portfolio balance rows.")

# ================================
# INSERT INTO TARGET TABLE (FetchRunID first)
# ================================
insert_sql = f"""
INSERT INTO {TARGET_TABLE}
(FetchRunID, AnalysisRunID, Symbol, N001, ExecutionDate, TradeNumber, N002, StartingBalance, EndingBalance, PercentageChange)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

rows = 0
try:
    for _, row in df_daily.iterrows():
        cursor.execute(insert_sql,
            row['FetchRunID'],
            row['AnalysisRunID'],
            row['Symbol'],
            row['N001'],
            row['ExecutionDate'],
            row['TradeNumber'],
            row['N002'],
            row['StartingBalance'],
            row['EndingBalance'],
            row['PercentageChange']
        )
        rows += 1
    
    conn.commit()
    logger.info(f"Successfully inserted {rows} daily portfolio balance rows into {TARGET_TABLE}")
except Exception as e:
    logger.error(f"Insert failed: {e}")
    conn.rollback()
finally:
    conn.close()

logger.info("Portfolio Balance generation finished successfully.")