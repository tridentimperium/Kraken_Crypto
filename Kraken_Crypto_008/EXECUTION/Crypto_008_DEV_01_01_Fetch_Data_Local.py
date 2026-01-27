import sys
import os
import pyodbc
import logging
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv

# ================================
# LOGGING SETUP
# ================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ================================
# PATHS & CONFIG
# ================================
execution_dir = os.path.dirname(os.path.abspath(__file__))
base_path = os.path.dirname(execution_dir)
config_path = os.path.join(base_path, "CONFIG")

TARGET_TABLE = "dbo.Crypto_008_DEV_01_01_Fetch_Data"

params_file = os.path.join(config_path, "ZZ_PARAMETERS", "Crypto_008_parameters.json")
if not os.path.exists(params_file):
    logger.error(f"Parameters file not found: {params_file}")
    sys.exit(1)

try:
    with open(params_file, 'r', encoding='utf-8') as f:
        params = json.load(f)
    logger.info(f"Loaded parameters from {params_file}")
except Exception as e:
    logger.error(f"Failed to load parameters: {e}")
    sys.exit(1)

# ================================
# EXTRACT & VALIDATE PARAMETERS
# ================================
sql_mode = str(params.get("SQL_Connection_Mode", "2"))
symbol_id = params.get("Symbol_ID")
timeframe = params.get("Timeframe")
start_date_str = params.get("StartDate")
end_date_str = params.get("EndDate")

# Symbol_ID
if not symbol_id or not isinstance(symbol_id, str):
    logger.error("Symbol_ID is required and must be a string.")
    sys.exit(1)
symbol_id = symbol_id.strip().upper()
logger.info(f"Symbol_ID: {symbol_id}")

# Timeframe
timeframe_map = {"1": "1MIN", "2": "5MIN", "3": "15MIN", "4": "1HRS", "5": "1DAY"}
timeframe_label = timeframe_map.get(timeframe)
if not timeframe_label:
    logger.error("Invalid Timeframe. Use 1–5.")
    sys.exit(1)
logger.info(f"Timeframe: {timeframe_label}")

# Dates
def parse_date(s):
    if not s or not isinstance(s, str):
        return None
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d")
    except ValueError as e:
        logger.error(f"Invalid date format '{s}': {e}")
        return None

start_date = parse_date(start_date_str)
end_date = parse_date(end_date_str)
if not start_date or not end_date:
    logger.error("Valid StartDate and EndDate required in YYYY-MM-DD format.")
    sys.exit(1)
if start_date > end_date:
    logger.error("StartDate must be <= EndDate.")
    sys.exit(1)

# End date inclusive: add one day for upper bound
end_date_exclusive = end_date + timedelta(days=1)

logger.info(f"Copying data ONLY for date range: {start_date.date()} to {end_date.date()} (inclusive)")

# Source table selection
if symbol_id == "KRAKEN_SPOT_ETH_USD":
    SOURCE_TABLE = "dbo.Crypto_888_DEV_01_01_Fetch_Data_ETH"
elif symbol_id == "KRAKEN_SPOT_BTC_USD":
    SOURCE_TABLE = "dbo.Crypto_999_DEV_01_01_Fetch_Data_BTC"
else:
    logger.error(f"Unsupported Symbol_ID: {symbol_id}")
    sys.exit(1)

logger.info(f"Source table: {SOURCE_TABLE}")
logger.info(f"Target table: {TARGET_TABLE}")

# SQL mode check
load_sql = sql_mode in ["1", "2"]
if not load_sql:
    logger.error("SQL mode must be enabled (1 or 2).")
    sys.exit(1)

# ================================
# LOAD SQL .ENV
# ================================
sql_env_file = os.path.join(
    config_path, "SQLSERVER",
    "Crypto_008_sqlserver_local.env" if sql_mode == "1" else "Crypto_008_sqlserver_remote.env"
)
if os.path.exists(sql_env_file):
    load_dotenv(sql_env_file, override=True)
    logger.info(f"Loaded SQL env: {sql_env_file}")
else:
    logger.error(f"SQL env file not found: {sql_env_file}")
    sys.exit(1)

# ================================
# SQL CONNECTION
# ================================
required = ["SQL_SERVER", "SQL_DATABASE", "SQL_USER", "SQL_PASSWORD"]
missing = [k for k in required if not os.getenv(k)]
if missing:
    logger.error(f"Missing SQL env vars: {missing}")
    sys.exit(1)

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
# ENSURE TARGET TABLE EXISTS
# ================================
create_table_sql = f'''
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
               WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_008_DEV_01_01_Fetch_Data')
BEGIN
    CREATE TABLE {TARGET_TABLE} (
        FetchRunID INT NOT NULL,
        DateTime DATETIME NOT NULL,
        Timeframe VARCHAR(10) NOT NULL,
        Symbol NVARCHAR(50) NOT NULL,
        [Open] FLOAT NULL,
        [High] FLOAT NULL,
        [Low] FLOAT NULL,
        [Close] FLOAT NULL,
        Volume FLOAT NULL,
        VWAP FLOAT NULL,
        BarCount INT NULL,
        BidPrice FLOAT NULL,
        AskPrice FLOAT NULL,
        BidSize INT NULL,
        AskSize INT NULL,
        ImpliedVolatility FLOAT NULL,
        HistoricalVolatility FLOAT NULL,
        CONSTRAINT PK_Crypto_008_Fetch_Data PRIMARY KEY (FetchRunID, DateTime, Symbol, Timeframe)
    );
END
'''
try:
    cursor.execute(create_table_sql)
    conn.commit()
    logger.info(f"Target table ensured: {TARGET_TABLE}")
except Exception as e:
    logger.error(f"Table setup failed: {e}")
    conn.close()
    sys.exit(1)

# ================================
# GET NEXT FetchRunID
# ================================
try:
    cursor.execute(f"SELECT ISNULL(MAX(FetchRunID), 0) + 1 FROM {TARGET_TABLE}")
    fetch_run_id = cursor.fetchone()[0]
    logger.info(f"Using FetchRunID: {fetch_run_id}")
except Exception as e:
    logger.error(f"Failed to get FetchRunID: {e}")
    fetch_run_id = 1
    logger.info(f"Defaulting to FetchRunID: {fetch_run_id}")

# ================================
# COPY DATA ONLY WITHIN THE DATE RANGE
# ================================
copy_sql = f'''
INSERT INTO {TARGET_TABLE}
    (FetchRunID, DateTime, Timeframe, Symbol, [Open], [High], [Low], [Close],
     Volume, VWAP, BarCount, BidPrice, AskPrice, BidSize, AskSize,
     ImpliedVolatility, HistoricalVolatility)
SELECT 
    ? AS FetchRunID,
    s.DateTime,
    s.Timeframe,
    s.Symbol,
    s.[Open], s.[High], s.[Low], s.[Close],
    s.Volume, s.VWAP, s.BarCount, s.BidPrice, s.AskPrice, s.BidSize, s.AskSize,
    s.ImpliedVolatility, s.HistoricalVolatility
FROM {SOURCE_TABLE} s
WHERE s.Symbol = ?
  AND s.Timeframe = ?
  AND s.DateTime >= ?
  AND s.DateTime < ?
  AND NOT EXISTS (
      SELECT 1 FROM {TARGET_TABLE} t
      WHERE t.FetchRunID = ?
        AND t.DateTime = s.DateTime
        AND t.Symbol = s.Symbol
        AND t.Timeframe = s.Timeframe
  )
'''

try:
    cursor.execute(
        copy_sql,
        fetch_run_id,           # FetchRunID
        symbol_id,              # Symbol
        timeframe_label,        # Timeframe
        start_date,             # >= StartDate
        end_date_exclusive,     # < EndDate + 1
        fetch_run_id            # NOT EXISTS FetchRunID
    )
    rows_inserted = cursor.rowcount
    conn.commit()
    logger.info(f"Successfully copied {rows_inserted} rows into {TARGET_TABLE} for date range {start_date.date()} to {end_date.date()}.")
except Exception as e:
    logger.error(f"Data copy failed: {e}")
    conn.rollback()
finally:
    conn.close()

logger.info("Script completed successfully.")