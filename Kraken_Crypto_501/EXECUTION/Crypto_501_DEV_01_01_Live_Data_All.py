import sys
import os
import time
import pyodbc
import json
from datetime import datetime, timezone, timedelta
import pytz
from dotenv import load_dotenv

# ================================
# PATHS & CONFIG
# ================================
execution_dir = os.path.dirname(os.path.abspath(__file__))
base_path = os.path.dirname(execution_dir)
config_path = os.path.join(base_path, "CONFIG")
params_file = os.path.join(config_path, "ZZ_PARAMETERS", "Crypto_501_parameters.json")
UNIFIED_TABLE = "dbo.Crypto_501_DEV_01_01_Live_Data_All"

if not os.path.exists(params_file):
    print("Parameters file not found")
    sys.exit(1)

with open(params_file, 'r', encoding='utf-8') as f:
    params = json.load(f)

symbol_id_raw = params.get("Symbol_ID")
if not symbol_id_raw or not isinstance(symbol_id_raw, str):
    print("Symbol_ID required")
    sys.exit(1)

# Symbol conversions
# If symbol is like "BTC/USD", convert appropriately for each exchange
symbol_kraken   = symbol_id_raw  # Kraken uses BTC/USD format
symbol_coinbase = symbol_id_raw.replace("/", "-")  # Coinbase uses BTC-USD format

# ================================
# SQL CONNECTION
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
    
    if os.path.exists(sql_env_file):
        load_dotenv(sql_env_file, encoding='utf-8')

conn = None
cursor = None
try:
    required = ["SQL_SERVER", "SQL_DATABASE", "SQL_USER", "SQL_PASSWORD"]
    if all(os.getenv(k) for k in required):
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
except Exception as e:
    print(f"SQL connection failed: {e}")
    sys.exit(1)

if not cursor:
    print("SQL connection required")
    sys.exit(1)

# ================================
# CREATE TABLE IF NOT EXISTS
# ================================
create_unified_sql = f"""
IF NOT EXISTS (
    SELECT * FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{UNIFIED_TABLE.split('.')[-1]}'
)
BEGIN
    CREATE TABLE {UNIFIED_TABLE} (
        DateTime_EST      DATETIME NULL,
        DateTime          DATETIME NOT NULL,
        N001              NVARCHAR(50) NULL,
        K_Timeframe       VARCHAR(10) NULL,
        K_Symbol          NVARCHAR(50) NULL,
        K_Open            FLOAT NULL,
        K_High            FLOAT NULL,
        K_Low             FLOAT NULL,
        K_Close           FLOAT NULL,
        K_Volume          FLOAT NULL,
        N002              NVARCHAR(50) NULL,
        C_Timeframe       VARCHAR(10) NULL,
        C_Symbol          NVARCHAR(50) NULL,
        C_Open            FLOAT NULL,
        C_High            FLOAT NULL,
        C_Low             FLOAT NULL,
        C_Close           FLOAT NULL,
        C_Volume          FLOAT NULL,
        N003              NVARCHAR(50) NULL,
        C5_Timeframe      VARCHAR(10) NULL,
        C5_Symbol         NVARCHAR(50) NULL,
        C5_Open           FLOAT NULL,
        C5_High           FLOAT NULL,
        C5_Low            FLOAT NULL,
        C5_Close          FLOAT NULL,
        C5_Volume         FLOAT NULL,
        CONSTRAINT PK_{UNIFIED_TABLE.split('.')[-1].replace('-','_')} 
            PRIMARY KEY CLUSTERED (DateTime DESC)
    );
END
"""
cursor.execute(create_unified_sql)
conn.commit()

# Track last printed timestamp to avoid repeats
last_printed_dt = None

# ================================
# MAIN LOOP – full loading + print only NEW latest row
# ================================
LOOKBACK_MINUTES = 30  # re-checks and re-upserts last 30 minutes every loop (covers ~30 rows)

while True:
    try:
        now_utc = datetime.now(timezone.utc)
        start_dt = now_utc - timedelta(minutes=LOOKBACK_MINUTES + 10)

        # Get all recent timestamps (full re-processing every loop)
        cursor.execute(f"""
            SELECT DISTINCT DateTime
            FROM (
                SELECT DateTime FROM dbo.Crypto_501_DEV_01_01_Live_Data_Kraken_1_min
                WHERE DateTime >= ?
                UNION
                SELECT DateTime FROM dbo.Crypto_501_DEV_01_01_Live_Data_Coinbase_1_min
                WHERE DateTime >= ?
            ) ts
            ORDER BY DateTime DESC
        """, start_dt, start_dt)

        minute_rows = cursor.fetchall()

        newest_dt = None

        for row in minute_rows:
            dt = row[0]

            if newest_dt is None or dt > newest_dt:
                newest_dt = dt

            # Kraken
            cursor.execute("""
                SELECT Timeframe, Symbol, [Open], [High], [Low], [Close], Volume
                FROM dbo.Crypto_501_DEV_01_01_Live_Data_Kraken_1_min
                WHERE DateTime = ? AND Symbol = ?
            """, dt, symbol_kraken)
            k = cursor.fetchone()

            # Coinbase 1-min
            cursor.execute("""
                SELECT Timeframe, Symbol, [Open], [High], [Low], [Close], Volume
                FROM dbo.Crypto_501_DEV_01_01_Live_Data_Coinbase_1_min
                WHERE DateTime = ? AND Symbol = ?
            """, dt, symbol_coinbase)
            c = cursor.fetchone()

            # Coinbase 5-min
            cursor.execute("""
                SELECT TOP 1 Timeframe, Symbol, [Open], [High], [Low], [Close], Volume
                FROM dbo.Crypto_501_DEV_01_01_Live_Data_Coinbase_5_min
                WHERE DateTime <= ? 
                  AND DATEADD(MINUTE, 5, DateTime) > ?
                ORDER BY DateTime DESC
            """, dt, dt)
            c5 = cursor.fetchone()

            # Timezone conversion
            if dt.tzinfo is None:
                dt_utc_aware = pytz.utc.localize(dt)
            else:
                dt_utc_aware = dt
            dt_est = dt_utc_aware.astimezone(pytz.timezone('America/New_York'))

            # Build vals
            vals = (
                dt_est, dt, None,
                k[0] if k else None, k[1] if k else None,
                k[2] if k else None, k[3] if k else None,
                k[4] if k else None, k[5] if k else None,
                k[6] if k else None,
                None,
                c[0] if c else None, c[1] if c else None,
                c[2] if c else None, c[3] if c else None,
                c[4] if c else None, c[5] if c else None,
                c[6] if c else None,
                None,
                c5[0] if c5 else None, c5[1] if c5 else None,
                c5[2] if c5 else None, c5[3] if c5 else None,
                c5[4] if c5 else None, c5[5] if c5 else None,
                c5[6] if c5 else None
            )

            # MERGE – forces overwrite with latest source values
            cursor.execute(f"""
                MERGE INTO {UNIFIED_TABLE} AS t
                USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS s
                (
                    DateTime_EST, DateTime, N001,
                    K_Timeframe, K_Symbol, K_Open, K_High, K_Low, K_Close, K_Volume,
                    N002,
                    C_Timeframe, C_Symbol, C_Open, C_High, C_Low, C_Close, C_Volume,
                    N003,
                    C5_Timeframe, C5_Symbol, C5_Open, C5_High, C5_Low, C5_Close, C5_Volume
                )
                ON t.DateTime = s.DateTime
                WHEN MATCHED THEN
                    UPDATE SET
                        DateTime_EST  = s.DateTime_EST,
                        N001          = s.N001,
                        K_Timeframe   = s.K_Timeframe,
                        K_Symbol      = s.K_Symbol,
                        K_Open        = s.K_Open,
                        K_High        = s.K_High,
                        K_Low         = s.K_Low,
                        K_Close       = s.K_Close,
                        K_Volume      = s.K_Volume,
                        N002          = s.N002,
                        C_Timeframe   = s.C_Timeframe,
                        C_Symbol      = s.C_Symbol,
                        C_Open        = s.C_Open,
                        C_High        = s.C_High,
                        C_Low         = s.C_Low,
                        C_Close       = s.C_Close,
                        C_Volume      = s.C_Volume,
                        N003          = s.N003,
                        C5_Timeframe  = s.C5_Timeframe,
                        C5_Symbol     = s.C5_Symbol,
                        C5_Open       = s.C5_Open,
                        C5_High       = s.C5_High,
                        C5_Low        = s.C5_Low,
                        C5_Close      = s.C5_Close,
                        C5_Volume     = s.C5_Volume
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (
                        DateTime_EST, DateTime, N001,
                        K_Timeframe, K_Symbol, K_Open, K_High, K_Low, K_Close, K_Volume,
                        N002,
                        C_Timeframe, C_Symbol, C_Open, C_High, C_Low, C_Close, C_Volume,
                        N003,
                        C5_Timeframe, C5_Symbol, C5_Open, C5_High, C5_Low, C5_Close, C5_Volume
                    )
                    VALUES (
                        s.DateTime_EST, s.DateTime, s.N001,
                        s.K_Timeframe, s.K_Symbol, s.K_Open, s.K_High, s.K_Low, s.K_Close, s.K_Volume,
                        s.N002,
                        s.C_Timeframe, s.C_Symbol, s.C_Open, s.C_High, s.C_Low, s.C_Close, s.C_Volume,
                        s.N003,
                        s.C5_Timeframe, s.C5_Symbol, s.C5_Open, s.C5_High, s.C5_Low, s.C5_Close, s.C5_Volume
                    );
            """, vals)
            conn.commit()

        # Print only if new latest timestamp
        if newest_dt and (last_printed_dt is None or newest_dt > last_printed_dt):
            cursor.execute("""
                SELECT Timeframe, Symbol
                FROM dbo.Crypto_501_DEV_01_01_Live_Data_Kraken_1_min
                WHERE DateTime = ? AND Symbol = ?
            """, newest_dt, symbol_kraken)
            k_disp = cursor.fetchone()

            if k_disp and k_disp[0] and k_disp[1]:
                tf = k_disp[0]
                sym = k_disp[1]
            else:
                cursor.execute("""
                    SELECT Timeframe, Symbol
                    FROM dbo.Crypto_501_DEV_01_01_Live_Data_Coinbase_1_min
                    WHERE DateTime = ? AND Symbol = ?
                """, newest_dt, symbol_coinbase)
                c_disp = cursor.fetchone()
                tf = c_disp[0] if c_disp else "1MIN_AGG"
                sym = c_disp[1] if c_disp else symbol_coinbase

            newest_est = newest_dt.astimezone(pytz.timezone('America/New_York')) if newest_dt.tzinfo else pytz.utc.localize(newest_dt).astimezone(pytz.timezone('America/New_York'))

            print(f"{sym} {tf} @ {newest_dt.isoformat()} UTC / {newest_est.isoformat()} EST")
            last_printed_dt = newest_dt

        time.sleep(1)

    except Exception as e:
        print(f"Error: {e}")
        time.sleep(5)

if conn:
    conn.close()