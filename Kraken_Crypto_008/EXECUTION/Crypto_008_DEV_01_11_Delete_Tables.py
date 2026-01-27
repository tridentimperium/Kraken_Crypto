# =============================================================================
# SCRIPT: delete_crypto_002_tables.py
#
# PURPOSE: Permanently drops the following tables (no confirmation prompt):
#   dbo.Crypto_002_DEV_01_02_Analysis_Results
#   dbo.Crypto_002_DEV_01_04_Analysis_Backtest
#
# WARNING: THERE IS NO UNDO. Run only if you are 100% sure.
# Tables are only dropped if they exist.
# =============================================================================

import os
import sys
import pyodbc
import logging
from dotenv import load_dotenv

# ================================
# LOGGING
# ================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ================================
# TABLES TO DROP
# ================================
TABLES_TO_DROP = [
    "dbo.Crypto_002_DEV_01_02_Analysis_Results",
    "dbo.Crypto_002_DEV_01_04_Analysis_Backtest"
]

# ================================
# SQL CONNECTION
# ================================
CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "CONFIG")
SQL_ENV_DIR = os.path.join(CONFIG_PATH, "SQLSERVER")

sql_env_file = os.path.join(SQL_ENV_DIR, "Crypto_002_sqlserver_local.env")
if not os.path.exists(sql_env_file):
    sql_env_file = os.path.join(SQL_ENV_DIR, "Crypto_002_sqlserver_remote.env")

if not os.path.exists(sql_env_file):
    logger.error(f"SQL env file not found: {sql_env_file}")
    sys.exit(1)

load_dotenv(sql_env_file, encoding='utf-8')

required = ["SQL_SERVER", "SQL_DATABASE", "SQL_USER", "SQL_PASSWORD"]
missing = [k for k in required if not os.getenv(k)]
if missing:
    logger.error(f"Missing SQL env vars: {missing}")
    sys.exit(1)

conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={os.getenv('SQL_SERVER')};"
    f"DATABASE={os.getenv('SQL_DATABASE')};"
    f"UID={os.getenv('SQL_USER')};"
    f"PWD={os.getenv('SQL_PASSWORD')};"
    f"TrustServerCertificate=yes;"
)

try:
    conn = pyodbc.connect(conn_str)
    conn.autocommit = True  # Required for DROP TABLE
    cursor = conn.cursor()
    logger.info("Connected to SQL Server")
except Exception as e:
    logger.error(f"SQL connection failed: {e}")
    sys.exit(1)

# ================================
# DROP TABLES (silent, no confirmation)
# ================================
logger.warning("Dropping tables... (no confirmation requested)")

for table in TABLES_TO_DROP:
    try:
        cursor.execute(f"""
            IF EXISTS (SELECT * FROM sys.tables 
                       WHERE name = '{table.split('.')[1]}' 
                       AND SCHEMA_NAME(schema_id) = 'dbo')
            BEGIN
                DROP TABLE {table};
                PRINT 'Dropped: {table}';
            END
            ELSE
                PRINT 'Not found: {table}';
        """)
        logger.info(f"Processed: {table}")
    except Exception as e:
        logger.error(f"Failed to drop {table}: {e}")

conn.commit()
conn.close()

logger.info("Script finished. Tables dropped if they existed.")
print("\nOperation complete - tables have been dropped (if they existed).\n")