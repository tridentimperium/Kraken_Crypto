import sys
import os
import time
import pyodbc
import logging
import json
import websocket
from datetime import datetime, timezone
import pytz
import threading
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
execution_dir = os.path.dirname(os.path.abspath(__file__))  # ...\Kraken_Crypto_501\EXECUTION
base_path = os.path.dirname(execution_dir)                   # ...\Kraken_Crypto_501
config_path = os.path.join(base_path, "CONFIG")

# Fixed table name
TABLE_NAME = "dbo.Crypto_501_DEV_01_01_Live_Data_Kraken_1_min"

# Parameters file
params_file = os.path.join(config_path, "ZZ_PARAMETERS", "Crypto_501_parameters.json")

logger.info(f"Script running from: {execution_dir}")
logger.info(f"Base path: {base_path}")
logger.info(f"Config path: {config_path}")
logger.info(f"Looking for parameters file: {params_file}")

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

# Extract params
symbol_id_raw = params.get("Symbol_ID")
timeframe = params.get("Timeframe", "1")
keep_hours = int(params.get("Live_Data_HRs_Kraken", 24))  # Default to 24 hours if missing

if not symbol_id_raw or not isinstance(symbol_id_raw, str):
    logger.error("Symbol_ID is required and must be a string.")
    sys.exit(1)

symbol_kraken = symbol_id_raw.replace("KRAKEN_SPOT_", "").replace("_", "/")
logger.info(f"Using Kraken symbol: {symbol_kraken}")

if timeframe != "1":
    logger.error("This live script only supports Timeframe = '1' (1MIN).")
    sys.exit(1)

timeframe_label = "1MIN"
logger.info(f"Timeframe: {timeframe_label}")
logger.info(f"Keeping data for {keep_hours} hours (deleting older)")

# ================================
# LOAD SQL .env
# ================================
for key in list(os.environ.keys()):
    if key.startswith("SQL_"):
        os.environ.pop(key, None)

sql_mode = str(params.get("SQL_Connection_Mode", "2"))
load_sql = sql_mode in ["1", "2"]

sql_env_file = None
if load_sql:
    if sql_mode == "1":
        sql_env_file = os.path.join(config_path, "SQLSERVER", "Crypto_501_sqlserver_local.env")
    else:
        sql_env_file = os.path.join(config_path, "SQLSERVER", "Crypto_501_sqlserver_remote.env")

    logger.info(f"Looking for SQL env file: {sql_env_file}")
    if os.path.exists(sql_env_file):
        load_dotenv(sql_env_file, encoding='utf-8')
        logger.info(f"Loaded SQL env: {sql_env_file}")
    else:
        logger.error(f"SQL env file not found: {sql_env_file}")
        load_sql = False

# ================================
# SQL CONNECTION
# ================================
conn = None
cursor = None
if load_sql:
    required = ["SQL_SERVER", "SQL_DATABASE", "SQL_USER", "SQL_PASSWORD"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        logger.error(f"Missing SQL env vars: {missing}")
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
# ENSURE TABLE EXISTS (with DateTime_EST, clustered index DESC for newest first)
# ================================
create_table_sql = f'''
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
               WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_501_DEV_01_01_Live_Data_Kraken_1_min')
BEGIN
    CREATE TABLE {TABLE_NAME} (
        DateTime_EST DATETIME NULL,          -- Eastern Time (EST/EDT)
        DateTime     DATETIME NOT NULL,      -- UTC from Kraken
        Timeframe    VARCHAR(10) NOT NULL,
        Symbol       NVARCHAR(50) NOT NULL,
        [Open]       FLOAT NULL,
        [High]       FLOAT NULL,
        [Low]        FLOAT NULL,
        [Close]      FLOAT NULL,
        Volume       FLOAT NULL,
        VWAP         FLOAT NULL,
        BarCount     INT NULL,
        BidPrice     FLOAT NULL,
        AskPrice     FLOAT NULL,
        BidSize      INT NULL,
        AskSize      INT NULL,
        ImpliedVolatility FLOAT NULL,
        HistoricalVolatility  FLOAT NULL,
        CONSTRAINT PK_Crypto_501_DEV_01_01_Live_Data_Kraken_1_min 
            PRIMARY KEY CLUSTERED (DateTime DESC, Symbol ASC, Timeframe ASC)
    );
    PRINT 'Table {TABLE_NAME} created with DateTime_EST and clustered index DESC.';
END
ELSE
BEGIN
    -- Add DateTime_EST if missing
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
                   WHERE TABLE_SCHEMA = 'dbo' 
                     AND TABLE_NAME = 'Crypto_501_DEV_01_01_Live_Data_Kraken_1_min' 
                     AND COLUMN_NAME = 'DateTime_EST')
    BEGIN
        ALTER TABLE {TABLE_NAME} ADD DateTime_EST DATETIME NULL;
        PRINT 'Added DateTime_EST column.';
    END
END
'''

try:
    cursor.execute(create_table_sql)
    conn.commit()
    logger.info(f"Table ensured / verified: {TABLE_NAME}")
except Exception as e:
    logger.error(f"Table setup failed: {e}")
    conn.close()
    sys.exit(1)

# ================================
# CLEANUP FUNCTION (delete old data)
# ================================
def clean_old_data():
    if keep_hours <= 0:
        logger.info("Live_Data_HRs_Kraken <= 0 – skipping cleanup")
        return

    try:
        cursor.execute(f'''
            DELETE FROM {TABLE_NAME}
            WHERE DateTime < DATEADD(HOUR, -{keep_hours}, GETUTCDATE())
        ''')
        deleted_count = cursor.rowcount
        conn.commit()
        logger.info(f"Cleaned {deleted_count} old rows (keeping last {keep_hours} hours)")
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")

# ================================
# CLEANUP THREAD (runs every 60 seconds)
# ================================
def cleanup_thread():
    while True:
        clean_old_data()
        time.sleep(60)  # Clean every minute

# ================================
# UPSERT FUNCTION – using MERGE
# ================================
def upsert_candle(dt_utc: datetime, open_p: float, high_p: float, low_p: float, close_p: float, vol: float):
    # Convert UTC to Eastern Time
    utc_aware = dt_utc.replace(tzinfo=timezone.utc)
    est_tz = pytz.timezone('America/New_York')
    dt_est = utc_aware.astimezone(est_tz)

    try:
        cursor.execute(f'''
            MERGE INTO {TABLE_NAME} AS target
            USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source 
                (DateTime_EST, DateTime, Timeframe, Symbol, [Open], [High], [Low], [Close], Volume)
            ON target.DateTime = source.DateTime 
               AND target.Symbol = source.Symbol 
               AND target.Timeframe = source.Timeframe
            WHEN MATCHED THEN
                UPDATE SET 
                    DateTime_EST = source.DateTime_EST,
                    [Open]       = source.[Open],
                    [High]       = source.[High],
                    [Low]        = source.[Low],
                    [Close]      = source.[Close],
                    Volume       = source.Volume
            WHEN NOT MATCHED THEN
                INSERT (DateTime_EST, DateTime, Timeframe, Symbol, [Open], [High], [Low], [Close], Volume)
                VALUES (source.DateTime_EST, source.DateTime, source.Timeframe, source.Symbol, 
                        source.[Open], source.[High], source.[Low], source.[Close], source.Volume);
        ''',
            dt_est, dt_utc, timeframe_label, symbol_kraken,
            open_p, high_p, low_p, close_p, vol
        )
        conn.commit()
        logger.info(
            f"Upserted {symbol_kraken} {timeframe_label} @ {dt_utc} UTC / {dt_est} EST | "
            f"O={open_p:.2f} | H={high_p:.2f} | L={low_p:.2f} | C={close_p:.2f} | V={vol:.4f}"
        )
    except Exception as e:
        logger.error(f"DB upsert failed: {e}")

# ================================
# WEBSOCKET HANDLERS
# ================================
def on_message(ws, message):
    try:
        msg = json.loads(message)
        if msg.get("channel") == "ohlc" and msg.get("type") == "update":
            for candle in msg.get("data", []):
                if candle.get("symbol") == symbol_kraken:
                    ts_value = candle["timestamp"]

                    if isinstance(ts_value, (int, float)):
                        ts_float = float(ts_value)
                    else:
                        ts_clean = ts_value.rstrip('Z')
                        dt = datetime.fromisoformat(ts_clean)
                        ts_float = dt.timestamp()

                    minute_start_unix = int(ts_float // 60) * 60
                    dt_utc = datetime.fromtimestamp(minute_start_unix)

                    open_p  = float(candle.get("open", 0))
                    high_p  = float(candle.get("high", 0))
                    low_p   = float(candle.get("low", 0))
                    close_p = float(candle.get("close", 0))
                    vol     = float(candle.get("volume", 0))

                    upsert_candle(dt_utc, open_p, high_p, low_p, close_p, vol)
    except json.JSONDecodeError:
        pass
    except Exception as e:
        logger.error(f"Message processing error: {e}")

def on_error(ws, error):
    logger.error(f"WS error: {error}")

def on_close(ws, close_status_code, close_msg):
    logger.warning(f"WS closed | code={close_status_code} msg={close_msg}")

def on_open(ws):
    logger.info("WebSocket connected - subscribing...")
    sub = {
        "method": "subscribe",
        "params": {
            "channel": "ohlc",
            "symbol": [symbol_kraken],
            "interval": 1
        }
    }
    ws.send(json.dumps(sub))

# ================================
# MAIN - RUN FOREVER WITH RECONNECT
# ================================
if __name__ == "__main__":
    # Start cleanup thread
    threading.Thread(target=cleanup_thread, daemon=True).start()

    ws_url = "wss://ws.kraken.com/v2"

    while True:
        try:
            ws = websocket.WebSocketApp(
                ws_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            logger.info(f"Starting WebSocket connection to {ws_url} for {symbol_kraken} 1m OHLC...")
            ws.run_forever(
                ping_interval=25,
                ping_timeout=10
            )
        except Exception as e:
            logger.error(f"Main loop error: {e}")
            time.sleep(10)

    if conn:
        conn.close()