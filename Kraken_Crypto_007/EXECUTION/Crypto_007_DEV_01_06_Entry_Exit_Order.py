import sys
import os
import pyodbc
import logging
import pandas as pd
import numpy as np
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
logger.info("--- STARTING ENTRY/EXIT ORDER EXTRACTION ---")

# ================================
# PATHS
# ================================
EXECUTION_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_PATH = os.path.dirname(EXECUTION_DIR)
CONFIG_PATH = os.path.join(BASE_PATH, "CONFIG")

# ================================
# TABLE NAMES
# ================================
SOURCE_TABLE = "dbo.Crypto_007_DEV_01_04_Analysis_Backtest"
DEST_TABLE = "dbo.Crypto_007_DEV_01_06_Entry_Exit_Order"

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
    variables_file = os.path.join(CONFIG_PATH, "ZZ_VARIABLES", "Crypto_007_variables.json")
    if not os.path.exists(variables_file):
        logger.error(f"Variables file not found: {variables_file}")
        sys.exit(1)
    with open(variables_file, 'r', encoding='utf-8') as f:
        vars_config = json.load(f)
    logger.info("Loaded config from Crypto_007_variables.json (standalone)")

# Extract IDs with defaults
ANALYSIS_RUN_ID = int(vars_config.get("AnalysisRunID", 1))
FETCH_RUN_ID = int(vars_config.get("FetchRunID", 1))

logger.info(f"Using AnalysisRunID = {ANALYSIS_RUN_ID}, FetchRunID = {FETCH_RUN_ID}")

# ================================
# LOAD SQL CREDENTIALS
# ================================
sql_env_file = os.path.join(CONFIG_PATH, "SQLSERVER", "Crypto_007_sqlserver_local.env")
if not os.path.exists(sql_env_file):
    sql_env_file = os.path.join(CONFIG_PATH, "SQLSERVER", "Crypto_007_sqlserver_remote.env")
if not os.path.exists(sql_env_file):
    logger.error(f"SQL env file not found: {sql_env_file}")
    sys.exit(1)
load_dotenv(sql_env_file, encoding='utf-8')
logger.info(f"Loaded SQL env: {sql_env_file}")

# ================================
# SQL CONNECTION
# ================================
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
# CREATE DESTINATION TABLE (same structure as source)
# ================================
create_dest_sql = f'''
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Crypto_007_DEV_01_06_Entry_Exit_Order')
BEGIN
    SELECT * INTO {DEST_TABLE} FROM {SOURCE_TABLE} WHERE 1 = 0;
    PRINT 'Created {DEST_TABLE} with same structure as {SOURCE_TABLE}';
END
'''
try:
    cursor.execute(create_dest_sql)
    conn.commit()
    logger.info(f"Destination table {DEST_TABLE} ensured.")
except Exception as e:
    logger.error(f"Failed to create destination table: {e}")
    conn.close()
    sys.exit(1)

# ================================
# LOAD ONLY ENTRY AND EXIT ROWS (filtered and ordered)
# ================================
query = f"""
SELECT *
FROM {SOURCE_TABLE}
WHERE (EntryExit = 1.0 OR EntryExit = 2.0)
  AND AnalysisRunID = ?
  AND FetchRunID = ?
ORDER BY FetchRunID, AnalysisRunID, DateTime
"""
try:
    df = pd.read_sql(query, conn, params=[ANALYSIS_RUN_ID, FETCH_RUN_ID])
    logger.info(f"Loaded {len(df)} entry/exit rows from {SOURCE_TABLE}.")
except Exception as e:
    logger.error(f"Failed to load data: {e}")
    conn.close()
    sys.exit(1)

if df.empty:
    logger.info("No entry/exit rows found. Nothing to insert.")
    conn.close()
    sys.exit(0)

# Ensure DateTime is proper for insert
df['DateTime'] = pd.to_datetime(df['DateTime'])

# ================================
# GET ALL COLUMN NAMES IN ORDER
# ================================
columns = list(df.columns)
column_list = ', '.join([f'[{col}]' for col in columns])
placeholders = ', '.join(['?' for _ in columns])
insert_sql = f"""
INSERT INTO {DEST_TABLE} ({column_list})
VALUES ({placeholders})
"""

# ================================
# INSERT ROWS (safe type conversion)
# ================================
rows_inserted = 0
try:
    for _, row in df.iterrows():
        values = []
        for col in columns:
            val = row[col]
            if pd.isna(val):
                values.append(None)
            elif isinstance(val, (np.integer, np.int64)):
                values.append(int(val))
            elif isinstance(val, (np.floating, np.float64)):
                values.append(float(val))
            else:
                values.append(val)
        cursor.execute(insert_sql, values)
        rows_inserted += 1
    conn.commit()
    logger.info(f"Successfully inserted {rows_inserted} entry/exit orders into {DEST_TABLE}")
except Exception as e:
    logger.error(f"Insert failed: {e}")
    conn.rollback()
finally:
    conn.close()

logger.info("Entry/Exit Order extraction script finished.")