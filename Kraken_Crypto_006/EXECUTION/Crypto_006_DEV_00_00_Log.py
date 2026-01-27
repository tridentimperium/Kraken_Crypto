import os
import sys
import json
import itertools
import pyodbc
import logging
from dotenv import load_dotenv

# --- CONFIGURATION ---
VARIABLES_FILE_SRC = "Crypto_006_variables.json"
LOG_TABLE = "dbo.Crypto_006_DEV_01_00_Log"

CREATE_LOG_TABLE_SQL = f"""
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{LOG_TABLE.split('.')[1]}')
BEGIN
    CREATE TABLE {LOG_TABLE} (
        FetchRunID INT NOT NULL,
        AnalysisRunID INT IDENTITY(1,1) NOT NULL,  -- Explicitly starts from 1
        Status NVARCHAR(50) NOT NULL DEFAULT 'PENDING',
        N001 FLOAT NULL,
        SwingLookback INT NULL,
        N002 FLOAT NULL,
        EnableMinSwingFilter BIT NULL,
        MinSwingPct FLOAT NULL,
        N003 FLOAT NULL,
        TrendlineRange INT NULL,
        N004 FLOAT NULL,
        Entry INT NULL,
        EntryCount INT NULL,
        N005 FLOAT NULL,
        TargetDirection INT NULL,
        N006 FLOAT NULL,
        L_ProfitTargetPercent FLOAT NULL,
        L_StopLossPercent FLOAT NULL,
        S_ProfitTargetPercent FLOAT NULL,
        S_StopLossPercent FLOAT NULL,
        N007 FLOAT NULL,
        LogMessage NVARCHAR(MAX) NULL,
        PRIMARY KEY (FetchRunID, AnalysisRunID)
    );
END
"""

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
    encoding='utf-8'
)
logger = logging.getLogger(__name__)

EXECUTION_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_PATH = os.path.dirname(EXECUTION_DIR)
CONFIG_PATH_FULL = os.path.join(BASE_PATH, "CONFIG", "ZZ_VARIABLES")
SQL_ENV_DIR = os.path.join(BASE_PATH, "CONFIG", "SQLSERVER")

def setup_sql_connection():
    sql_env_file = os.path.join(SQL_ENV_DIR, "Crypto_006_sqlserver_local.env")
    if not os.path.exists(sql_env_file):
        sql_env_file = os.path.join(SQL_ENV_DIR, "Crypto_006_sqlserver_remote.env")
    if not os.path.exists(sql_env_file):
        logger.error(f"SQL env file not found: {sql_env_file}")
        return None
    
    load_dotenv(sql_env_file, encoding='utf-8')
    required = ["SQL_SERVER", "SQL_DATABASE", "SQL_USER", "SQL_PASSWORD"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        logger.error(f"Missing SQL env vars: {missing}")
        return None
    
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
        logger.info("Connected to SQL Server")
        return conn
    except Exception as e:
        logger.error(f"SQL connection failed: {e}")
        return None

def ensure_log_table_exists(conn):
    try:
        cursor = conn.cursor()
        cursor.execute(CREATE_LOG_TABLE_SQL)
        conn.commit()
        logger.info(f"Log table ensured: {LOG_TABLE}")
    except Exception as e:
        logger.error(f"Failed to create log table: {e}")
        raise

def insert_config(cursor, conn, fetch_run_id, config):
    insert_sql = f"""
    INSERT INTO {LOG_TABLE} (
        FetchRunID,
        Status,
        N001,
        SwingLookback, N002,
        EnableMinSwingFilter, MinSwingPct, N003,
        TrendlineRange, N004,
        Entry, EntryCount, N005,
        TargetDirection, N006,
        L_ProfitTargetPercent, L_StopLossPercent,
        S_ProfitTargetPercent, S_StopLossPercent,
        N007,
        LogMessage
    )
    VALUES (
        ?, 'PENDING', NULL,
        ?, NULL,
        ?, ?, NULL,
        ?, NULL,
        ?, ?, NULL,
        ?, NULL,
        ?, ?,
        ?, ?,
        NULL,
        NULL
    )
    """
    params = (
        fetch_run_id,
        config.get('SwingLookback'),
        config.get('EnableMinSwingFilter'),
        config.get('MinSwingPct'),
        config.get('TrendlineRange'),
        config.get('Entry'),
        config.get('EntryCount'),
        config.get('TargetDirection'),
        config.get('L_ProfitTargetPercent'),
        config.get('L_StopLossPercent'),
        config.get('S_ProfitTargetPercent'),
        config.get('S_StopLossPercent')
    )
    try:
        cursor.execute(insert_sql, params)
        conn.commit()
    except Exception as e:
        logger.error(f"Failed to insert config: {e}")
        raise

def is_config_exists(cursor, fetch_run_id, config):
    check_sql = f"""
    SELECT TOP 1 1 FROM {LOG_TABLE}
    WHERE FetchRunID = ?
      AND SwingLookback = ?
      AND EnableMinSwingFilter = ?
      AND MinSwingPct = ?
      AND TrendlineRange = ?
      AND Entry = ?
      AND EntryCount = ?
      AND TargetDirection = ?
      AND L_ProfitTargetPercent = ?
      AND L_StopLossPercent = ?
      AND S_ProfitTargetPercent = ?
      AND S_StopLossPercent = ?
    """
    params = (
        fetch_run_id,
        config.get('SwingLookback'),
        config.get('EnableMinSwingFilter'),
        config.get('MinSwingPct'),
        config.get('TrendlineRange'),
        config.get('Entry'),
        config.get('EntryCount'),
        config.get('TargetDirection'),
        config.get('L_ProfitTargetPercent'),
        config.get('L_StopLossPercent'),
        config.get('S_ProfitTargetPercent'),
        config.get('S_StopLossPercent')
    )
    try:
        cursor.execute(check_sql, params)
        return cursor.fetchone() is not None
    except Exception as e:
        logger.error(f"Error checking config: {e}")
        return False

def populate_log():
    variables_file_path_src = os.path.join(CONFIG_PATH_FULL, VARIABLES_FILE_SRC)
    if not os.path.exists(variables_file_path_src):
        logger.error(f"Variables file not found: {variables_file_path_src}")
        sys.exit(1)

    with open(variables_file_path_src, 'r', encoding='utf-8') as f:
        vars_config = json.load(f)

    grid_params = {}
    constant_params = {}
    for key, value in vars_config.items():
        if isinstance(value, list) and not key.endswith('_N'):
            grid_params[key] = value
        elif not isinstance(value, list) and not key.endswith('_N'):
            constant_params[key] = value

    param_names = list(grid_params.keys())
    param_values = list(grid_params.values())

    total_combos = len(list(itertools.product(*param_values)))
    logger.info(f"Total configurations: {total_combos}")

    combinations = itertools.product(*param_values)

    conn = setup_sql_connection()
    if not conn:
        sys.exit(1)

    try:
        ensure_log_table_exists(conn)
    except Exception:
        conn.close()
        sys.exit(1)

    cursor = conn.cursor()

    fetch_run_id = 1

    insert_count = 0
    for combo in combinations:
        config = constant_params.copy()
        for i, name in enumerate(param_names):
            config[name] = combo[i]

        # Ensure L_ and S_ keys exist (fallback if missing in grid)
        config.setdefault('L_ProfitTargetPercent', config.get('ProfitTargetPercent'))
        config.setdefault('L_StopLossPercent', config.get('StopLossPercent'))

        if not is_config_exists(cursor, fetch_run_id, config):
            insert_config(cursor, conn, fetch_run_id, config)
            insert_count += 1

    conn.close()
    logger.info(f"Inserted {insert_count} new configurations.")

if __name__ == "__main__":
    populate_log()