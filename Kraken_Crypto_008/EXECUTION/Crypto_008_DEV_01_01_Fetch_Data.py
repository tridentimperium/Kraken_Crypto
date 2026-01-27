import sys
import os
import time
import pyodbc
import logging
import json
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

# ================================
# LOGGING SETUP
# ================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
    encoding='utf-8'
)
logger = logging.getLogger(__name__)

# ================================
# PATHS & CONFIG
# ================================
execution_dir = os.path.dirname(os.path.abspath(__file__))  # EXECUTION/
base_path = os.path.dirname(execution_dir)                  # KRAKEN_CRYPTO/
config_path = os.path.join(base_path, "CONFIG")

# Fixed table name
TABLE_NAME = "dbo.Crypto_008_DEV_01_01_Fetch_Data"

# Parameters file
params_file = os.path.join(config_path, "ZZ_PARAMETERS", "Crypto_008_parameters.json")

if not os.path.exists(params_file):
    logger.error(f"Parameters file not found: {params_file}")
    sys.exit(1)

# Load parameters
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
api_mode = str(params.get("CLOUDAPI_Mode", "1"))
symbol_id = params.get("Symbol_ID")
timeframe = params.get("Timeframe")
start_date_str = params.get("StartDate")
end_date_str = params.get("EndDate")

# Symbol ID
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

logger.info(f"Date range: {start_date.date()} to {end_date.date()}")

# Modes
load_sql = sql_mode in ["1", "2"]
load_api = api_mode == "1"

if not load_sql:
    logger.warning("SQL disabled.")
if not load_api:
    logger.warning("API disabled.")

# ================================
# CLEAR & LOAD .ENV FILES
# ================================
for key in list(os.environ.keys()):
    if key.startswith(("SQL_", "CLOUDAPI_")):
        os.environ.pop(key, None)

conn = None
cursor = None
api_key = None

# SQL .env
if load_sql:
    sql_env_file = os.path.join(
        config_path, "SQLSERVER",
        "Crypto_008_sqlserver_local.env" if sql_mode == "1" else "Crypto_008_sqlserver_remote.env"
    )
    if os.path.exists(sql_env_file):
        load_dotenv(sql_env_file, encoding='utf-8')
        logger.info(f"Loaded SQL env: {sql_env_file}")
    else:
        logger.error(f"SQL env file not found: {sql_env_file}")
        load_sql = False

# CloudAPI .env
if load_api:
    api_env_file = os.path.join(config_path, "CLOUDAPI", "Crypto_008_cloudapi.env")
    if os.path.exists(api_env_file):
        load_dotenv(api_env_file, encoding='utf-8')
        logger.info(f"Loaded API env: {api_env_file}")
        api_key = os.getenv("CLOUDAPI_API_KEY")
        if not api_key:
            logger.error("CLOUDAPI_API_KEY missing.")
            load_api = False
    else:
        logger.error(f"API env file not found: {api_env_file}")
        load_api = False

# ================================
# SQL CONNECTION
# ================================
if load_sql:
    required = ["SQL_SERVER", "SQL_DATABASE", "SQL_USER", "SQL_PASSWORD"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        logger.error(f"Missing SQL env vars: {missing}")
        load_sql = False
    else:
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
            logger.info(f"Connected to SQL: {os.getenv('SQL_SERVER')}/{os.getenv('SQL_DATABASE')}")
        except Exception as e:
            logger.error(f"SQL connection failed: {e}")
            load_sql = False

if not load_sql or not conn or not cursor:
    logger.error("SQL connection required. Exiting.")
    sys.exit(1)

# ================================
# ENSURE TABLE EXISTS (FIXED SQL)
# ================================
create_table_sql = f'''
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
               WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_008_DEV_01_01_Fetch_Data')
BEGIN
    CREATE TABLE {TABLE_NAME} (
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
    PRINT 'Table {TABLE_NAME} created.';
END
ELSE
BEGIN
    -- Add FetchRunID if missing
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
                   WHERE TABLE_SCHEMA = 'dbo' 
                     AND TABLE_NAME = 'Crypto_008_DEV_01_01_Fetch_Data' 
                     AND COLUMN_NAME = 'FetchRunID')
    BEGIN
        ALTER TABLE {TABLE_NAME} ADD FetchRunID INT NOT NULL DEFAULT 1;
        PRINT 'Added FetchRunID column.';
    END

    -- Ensure PRIMARY KEY includes FetchRunID
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu ON tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
        WHERE tc.TABLE_SCHEMA = 'dbo'
          AND tc.TABLE_NAME = 'Crypto_008_DEV_01_01_Fetch_Data'
          AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
          AND ccu.COLUMN_NAME = 'FetchRunID'
    )
    BEGIN
        DECLARE @pkName NVARCHAR(128);
        SELECT @pkName = CONSTRAINT_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
        WHERE TABLE_SCHEMA = 'dbo'
          AND TABLE_NAME = 'Crypto_008_DEV_01_01_Fetch_Data'
          AND CONSTRAINT_TYPE = 'PRIMARY KEY';

        IF @pkName IS NOT NULL
            EXEC('ALTER TABLE {TABLE_NAME} DROP CONSTRAINT ' + @pkName);

        ALTER TABLE {TABLE_NAME}
        ADD CONSTRAINT PK_Crypto_008_Fetch_Data 
        PRIMARY KEY (FetchRunID, DateTime, Symbol, Timeframe);
        PRINT 'Updated PRIMARY KEY to include FetchRunID.';
    END
END
'''

try:
    cursor.execute(create_table_sql)
    conn.commit()
    logger.info(f"Table ensured: {TABLE_NAME}")
except Exception as e:
    logger.error(f"Table setup failed: {e}")
    conn.close()
    sys.exit(1)

# ================================
# GET NEXT FetchRunID
# ================================
try:
    cursor.execute(f"SELECT ISNULL(MAX(FetchRunID), 0) + 1 FROM {TABLE_NAME}")
    fetch_run_id = cursor.fetchone()[0]
    logger.info(f"Using FetchRunID: {fetch_run_id}")
except Exception as e:
    logger.warning(f"Failed to get FetchRunID: {e}. Using 1.")
    fetch_run_id = 1

# ================================
# FETCH DATA FROM COINAPI
# ================================
start_time = time.time()

if load_api and api_key:
    current = start_date
    while current <= end_date:
        time_start = current.strftime("%Y-%m-%dT00:00:00")
        time_end = (current + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")
        logger.info(f"Fetching {symbol_id} {timeframe_label} for {current.date()}...")

        url = f"https://rest.coinapi.io/v1/ohlcv/{symbol_id}/history"
        params_api = {
            "period_id": timeframe_label,
            "time_start": time_start,
            "time_end": time_end,
            "limit": 100000
        }
        headers = {"X-CoinAPI-Key": api_key}

        try:
            response = requests.get(url, headers=headers, params=params_api, timeout=30)
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Received {len(data)} records.")

                for item in data:
                    raw_dt = item['time_period_start']
                    sql_dt = raw_dt.replace('Z', '').split('.')[0]

                    cursor.execute(f'''
                        IF EXISTS (SELECT 1 FROM {TABLE_NAME}
                                   WHERE FetchRunID = ? AND [DateTime] = ? AND Symbol = ? AND Timeframe = ?)
                        BEGIN
                            UPDATE {TABLE_NAME} SET
                                [Open] = ?, [High] = ?, [Low] = ?, [Close] = ?, Volume = ?,
                                VWAP = ?, BarCount = ?, BidPrice = ?, AskPrice = ?, BidSize = ?,
                                AskSize = ?, ImpliedVolatility = ?, HistoricalVolatility = ?
                            WHERE FetchRunID = ? AND [DateTime] = ? AND Symbol = ? AND Timeframe = ?
                        END
                        ELSE
                        BEGIN
                            INSERT INTO {TABLE_NAME}
                            (FetchRunID, [DateTime], [Timeframe], [Symbol], [Open], [High], [Low], [Close],
                             Volume, VWAP, BarCount, BidPrice, AskPrice, BidSize, AskSize,
                             ImpliedVolatility, HistoricalVolatility)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        END
                    ''',
                        # UPDATE
                        fetch_run_id, sql_dt, symbol_id, timeframe_label,
                        item.get('price_open'), item.get('price_high'), item.get('price_low'), item.get('price_close'),
                        item.get('volume_traded'), None, item.get('trades_count'),
                        None, None, None, None, None, None,
                        # WHERE
                        fetch_run_id, sql_dt, symbol_id, timeframe_label,
                        # INSERT
                        fetch_run_id, sql_dt, timeframe_label, symbol_id,
                        item.get('price_open'), item.get('price_high'), item.get('price_low'), item.get('price_close'),
                        item.get('volume_traded'), None, item.get('trades_count'),
                        None, None, None, None, None, None
                    )
                conn.commit()
            else:
                logger.error(f"API error {response.status_code}: {response.text}")
        except Exception as e:
            logger.error(f"Request failed for {current.date()}: {e}")

        current += timedelta(days=1)
else:
    logger.warning("API fetch skipped.")

# ================================
# CLEANUP
# ================================
if conn:
    conn.close()

elapsed = time.time() - start_time
logger.info(f"Script completed in {elapsed:.2f} seconds.")