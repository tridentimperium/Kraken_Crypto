import os
import sys
import json
import logging
import pyodbc
import subprocess
import time
from dotenv import load_dotenv

# ================================
# LOGGING
# ================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
    encoding='utf-8'
)
logger = logging.getLogger(__name__)

# ================================
# PATHS
# ================================
EXECUTION_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_PATH = os.path.dirname(EXECUTION_DIR)
CONFIG_PATH = os.path.join(BASE_PATH, "CONFIG")
SQL_ENV_DIR = os.path.join(BASE_PATH, "CONFIG", "SQLSERVER")

# ================================
# TABLE NAMES
# ================================
LOG_TABLE = "dbo.Crypto_001_DEV_01_00_Log"
LOG_SCRIPT = "Crypto_001_DEV_00_00_Log.py"

# ================================
# LIST OF SCRIPTS TO RUN IN ORDER
# ================================
SCRIPT_LIST = [
    "Crypto_001_DEV_01_02_Analysis.py",
    "Crypto_001_DEV_01_04_Backtest.py",
    "Crypto_001_DEV_01_06_Entry_Exit_Order.py",
    "Crypto_001_DEV_01_07_Results_Analysis.py",
    "Crypto_001_DEV_01_08_Portfolio_Balance.py",
    "Crypto_001_DEV_01_09_Portfolio_Summary.py",
]

# ================================
# SQL CONNECTION SETUP
# ================================
def setup_sql_connection():
    sql_env_file = os.path.join(SQL_ENV_DIR, "Crypto_001_sqlserver_local.env")
    if not os.path.exists(sql_env_file):
        sql_env_file = os.path.join(SQL_ENV_DIR, "Crypto_001_sqlserver_remote.env")
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

def log_table_exists(cursor):
    try:
        cursor.execute(
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_NAME = 'Crypto_001_DEV_01_00_Log' AND TABLE_SCHEMA = 'dbo'"
        )
        return cursor.fetchone() is not None
    except:
        return False

def run_log_script():
    log_script_path = os.path.join(EXECUTION_DIR, LOG_SCRIPT)
    if not os.path.exists(log_script_path):
        logger.error(f"Log script not found: {log_script_path}")
        sys.exit(1)

    logger.info("Running log script to create/populate log table...")
    try:
        child_env = os.environ.copy()
        child_env['PYTHONIOENCODING'] = 'utf-8'
        result = subprocess.run(
            [sys.executable, log_script_path],
            capture_output=True,
            text=True,
            check=True,
            env=child_env
        )
        logger.info("Log script completed successfully.")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        logger.error("Log script failed:")
        logger.error(e.stderr)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Failed to run log script: {e}")
        sys.exit(1)

def update_log_status(cursor, analysis_run_id, status, message=None):
    update_sql = f"""
        UPDATE {LOG_TABLE}
        SET Status = ?, LogMessage = ?
        WHERE AnalysisRunID = ?
    """
    try:
        cursor.execute(update_sql, (status, message or "", analysis_run_id))
        cursor.connection.commit()
    except Exception as e:
        logger.error(f"Failed to update log status for AnalysisRunID {analysis_run_id}: {e}")

# ================================
# MAIN BATCH RUNNER
# ================================
def run_batch():
    conn = setup_sql_connection()
    if not conn:
        logger.warning("Cannot connect to SQL. Running log script anyway...")
        run_log_script()
        logger.info("Batch cannot continue without SQL connection.")
        return

    cursor = conn.cursor()

    # Ensure log table exists
    if not log_table_exists(cursor):
        conn.close()
        run_log_script()
        conn = setup_sql_connection()
        if not conn:
            sys.exit(1)
        cursor = conn.cursor()

    # Count total pending configurations
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {LOG_TABLE} WHERE Status = 'PENDING'")
        total_pending = cursor.fetchone()[0]
    except Exception as e:
        logger.error(f"Failed to count pending runs: {e}")
        conn.close()
        sys.exit(1)

    if total_pending == 0:
        logger.info("No pending configurations found. All done!")
        conn.close()
        return

    logger.info(f"Found {total_pending} pending configurations. Starting batch execution...")

    # Fetch pending runs with all relevant parameters
    query = f"""
    SELECT 
        AnalysisRunID,
        SwingLookback, EnableMinSwingFilter, MinSwingPct,
        TrendlineRange,
        Entry, EntryCount,
        TargetDirection,
        L_ProfitTargetPercent, L_StopLossPercent,
        S_ProfitTargetPercent, S_StopLossPercent
    FROM {LOG_TABLE}
    WHERE Status = 'PENDING'
    ORDER BY AnalysisRunID
    """
    try:
        cursor.execute(query)
        pending_runs = cursor.fetchall()
    except Exception as e:
        logger.error(f"Failed to fetch pending configurations: {e}")
        conn.close()
        sys.exit(1)

    for idx, row in enumerate(pending_runs, 1):
        (analysis_run_id,
         swing_lookback, enable_min_swing, min_swing_pct,
         trendline_range,
         entry, entry_count,
         target_direction,
         l_pt_percent, l_sl_percent,
         s_pt_percent, s_sl_percent) = row

        logger.info(f"--- Processing configuration {idx}/{total_pending} (AnalysisRunID: {analysis_run_id}) ---")

        update_log_status(cursor, analysis_run_id, 'RUNNING', f"Started {idx}/{total_pending}")

        # Build full config JSON
        config = {
            "AnalysisRunID": analysis_run_id,
            "SwingLookback": swing_lookback,
            "EnableMinSwingFilter": bool(enable_min_swing) if enable_min_swing is not None else False,
            "MinSwingPct": float(min_swing_pct) if min_swing_pct is not None else None,
            "TrendlineRange": trendline_range,
            "Entry": entry,
            "EntryCount": entry_count,
            "TargetDirection": target_direction,
            "L_ProfitTargetPercent": float(l_pt_percent) if l_pt_percent is not None else None,
            "L_StopLossPercent": float(l_sl_percent) if l_sl_percent is not None else None,
            "S_ProfitTargetPercent": float(s_pt_percent) if s_pt_percent is not None else None,
            "S_StopLossPercent": float(s_sl_percent) if s_sl_percent is not None else None,
        }

        config_json = json.dumps(config)

        all_success = True
        for script_name in SCRIPT_LIST:
            script_path = os.path.join(EXECUTION_DIR, script_name)
            if not os.path.exists(script_path):
                logger.error(f"Script not found: {script_path}")
                all_success = False
                continue

            logger.info(f"  Running: {script_name}")

            try:
                child_env = os.environ.copy()
                child_env['PYTHONIOENCODING'] = 'utf-8'

                result = subprocess.run(
                    [sys.executable, script_path, config_json],
                    capture_output=True,
                    text=True,
                    check=True,
                    env=child_env,
                    timeout=900          # 15 minutes timeout per script
                )
                logger.info(f"    {script_name} completed successfully")

            except subprocess.TimeoutExpired:
                logger.error(f"    {script_name} timed out after 15 minutes")
                all_success = False
            except subprocess.CalledProcessError as e:
                logger.error(f"    {script_name} failed with exit code {e.returncode}")
                logger.error(e.stderr)
                all_success = False
            except Exception as e:
                logger.error(f"    Unexpected error running {script_name}: {e}")
                all_success = False

            # Small delay to prevent connection storm
            time.sleep(1.5)

        final_status = 'COMPLETED' if all_success else 'ERROR'
        update_log_status(cursor, analysis_run_id, final_status,
                          f"{'Finished' if all_success else 'Failed at some step'} {idx}/{total_pending}")

    conn.close()
    logger.info("Batch execution finished. All pending configurations processed.")

if __name__ == "__main__":
    run_batch()