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
execution_dir = os.path.dirname(os.path.abspath(__file__))
base_path = os.path.dirname(execution_dir)
config_path = os.path.join(base_path, "CONFIG")
TABLE_NAME = "dbo.Crypto_501_DEV_01_01_Live_Data_Coinbase_5_min"
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

symbol_id_raw = params.get("Symbol_ID")
if not symbol_id_raw or not isinstance(symbol_id_raw, str):
    logger.error("Symbol_ID is required and must be a string.")
    sys.exit(1)

symbol_coinbase = symbol_id_raw.replace("KRAKEN_SPOT_", "").replace("_", "-")
logger.info(f"Using Coinbase symbol: {symbol_coinbase}")

keep_hours = int(params.get("Live_Data_HRs_Coinbase", 24))
logger.info(f"Keeping data for {keep_hours} hours")

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
# ENSURE TABLE EXISTS
# ================================
create_table_sql = f'''
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES
               WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_501_DEV_01_01_Live_Data_Coinbase_5_min')
BEGIN
    CREATE TABLE {TABLE_NAME} (
        DateTime_EST DATETIME NULL,
        DateTime DATETIME NOT NULL,
        Timeframe VARCHAR(10) NOT NULL DEFAULT '5MIN',
        Symbol NVARCHAR(50) NOT NULL,
        [Open] FLOAT NULL,
        [High] FLOAT NULL,
        [Low] FLOAT NULL,
        [Close] FLOAT NULL,
        Volume FLOAT NULL,
        CONSTRAINT PK_Crypto_501_DEV_01_01_Live_Data_Coinbase_5_min
            PRIMARY KEY CLUSTERED (DateTime DESC, Symbol ASC)
    );
    PRINT 'Table {TABLE_NAME} created.';
END
ELSE
BEGIN
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                   WHERE TABLE_SCHEMA = 'dbo'
                     AND TABLE_NAME = 'Crypto_501_DEV_01_01_Live_Data_Coinbase_5_min'
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
# CLEANUP FUNCTION
# ================================
def clean_old_data():
    if keep_hours <= 0:
        logger.info("Live_Data_HRs_Coinbase <= 0 – skipping cleanup")
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

def cleanup_thread():
    while True:
        clean_old_data()
        time.sleep(60)

threading.Thread(target=cleanup_thread, daemon=True).start()

# ================================
# UPSERT FUNCTION – now updates every incoming candle message
# ================================
def upsert_5min_candle(dt_utc: datetime, open_p: float, high_p: float, low_p: float, close_p: float, vol: float):
    dt_est = dt_utc.replace(tzinfo=timezone.utc).astimezone(pytz.timezone('America/New_York'))
    try:
        cursor.execute(f'''
            MERGE INTO {TABLE_NAME} AS target
            USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source
                (DateTime_EST, DateTime, Timeframe, Symbol, [Open], [High], [Low], [Close], Volume)
            ON target.DateTime = source.DateTime
               AND target.Symbol = source.Symbol
            WHEN MATCHED THEN
                UPDATE SET
                    DateTime_EST = source.DateTime_EST,
                    [Open] = source.[Open],
                    [High] = source.[High],
                    [Low] = source.[Low],
                    [Close] = source.[Close],
                    Volume = source.Volume
            WHEN NOT MATCHED THEN
                INSERT (DateTime_EST, DateTime, Timeframe, Symbol, [Open], [High], [Low], [Close], Volume)
                VALUES (source.DateTime_EST, source.DateTime, source.Timeframe, source.Symbol,
                        source.[Open], source.[High], source.[Low], source.[Close], source.Volume);
        ''',
            dt_est, dt_utc, '5MIN', symbol_coinbase,
            open_p, high_p, low_p, close_p, vol
        )
        conn.commit()
        logger.info(
            f"Upserted {symbol_coinbase} 5MIN @ {dt_utc} UTC / {dt_est} EST | "
            f"O={open_p:.8f} | H={high_p:.8f} | L={low_p:.8f} | C={close_p:.8f} | V={vol:.4f}"
        )
    except Exception as e:
        logger.error(f"DB upsert failed: {e}")

# ================================
# WEBSOCKET HANDLERS
# ================================
def on_message(ws, message):
    try:
        msg = json.loads(message)
        if msg.get("channel") != "candles":
            return

        for event in msg.get("events", []):
            for candle in event.get("candles", []):
                start_ts = int(candle["start"])
                dt_utc = datetime.fromtimestamp(start_ts, tz=timezone.utc)

                # Upsert EVERY update (running candle) - this keeps it fresh
                upsert_5min_candle(dt_utc,
                                   float(candle["open"]),
                                   float(candle["high"]),
                                   float(candle["low"]),
                                   float(candle["close"]),
                                   float(candle["volume"]))

    except json.JSONDecodeError:
        pass
    except Exception as e:
        logger.error(f"Message processing error: {e}")

def on_error(ws, error):
    logger.error(f"WS error: {error}")

def on_close(ws, close_status_code, close_msg):
    logger.warning(f"WS closed | code={close_status_code} msg={close_msg}")

def on_open(ws):
    logger.info("WebSocket connected - subscribing to candles...")
    sub = {
        "type": "subscribe",
        "product_ids": [symbol_coinbase],
        "channel": "candles"
    }
    ws.send(json.dumps(sub))

# ================================
# MAIN - RUN FOREVER WITH RECONNECT
# ================================
if __name__ == "__main__":
    ws_url = "wss://advanced-trade-ws.coinbase.com"
    while True:
        try:
            ws = websocket.WebSocketApp(
                ws_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            logger.info(f"Starting WebSocket for 5-min candles on {symbol_coinbase}...")
            ws.run_forever(
                ping_interval=25,
                ping_timeout=10
            )
        except Exception as e:
            logger.error(f"Main loop error: {e}")
            time.sleep(10)

    if conn:
        conn.close()