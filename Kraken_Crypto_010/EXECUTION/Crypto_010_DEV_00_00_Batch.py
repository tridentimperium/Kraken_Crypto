import os
import sys
import json
import logging
import pyodbc
import subprocess
import time
from datetime import timedelta
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
LOG_TABLE = "dbo.Crypto_010_DEV_01_00_Log"
LOG_SCRIPT = "Crypto_010_DEV_00_00_Log.py"
DELETE_SCRIPT = "Crypto_010_DEV_01_11_Delete_Tables.py"

# ================================
# LIST OF SCRIPTS TO RUN IN ORDER (per configuration)
# ================================
SCRIPT_LIST = [
    "Crypto_010_DEV_01_02_Analysis.py",
    "Crypto_010_DEV_01_04_Backtest.py",
    "Crypto_010_DEV_01_06_Entry_Exit_Order.py",
    "Crypto_010_DEV_01_07_Results_Analysis.py",
    "Crypto_010_DEV_01_08_Portfolio_Balance.py",
    "Crypto_010_DEV_01_09_Portfolio_Summary.py",
]

# ================================
# BATCH SIZE & PAUSE
# ================================
BATCH_SIZE = 10
PAUSE_AFTER_BATCH = 10  # seconds

# ================================
# SQL CONNECTION SETUP
# ================================
def setup_sql_connection():
    sql_env_file = os.path.join(SQL_ENV_DIR, "Crypto_010_sqlserver_local.env")
    if not os.path.exists(sql_env_file):
        sql_env_file = os.path.join(SQL_ENV_DIR, "Crypto_010_sqlserver_remote.env")
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
            "WHERE TABLE_NAME = 'Crypto_010_DEV_01_00_Log' AND TABLE_SCHEMA = 'dbo'"
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
            encoding='utf-8',
            errors='replace',
            check=True,
            env=child_env
        )
        logger.info("Log script completed successfully.")
        if result.stdout.strip():
            print(result.stdout.strip())
    except subprocess.CalledProcessError as e:
        logger.error("Log script failed:")
        if e.stderr:
            logger.error(e.stderr)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Failed to run log script: {e}")
        sys.exit(1)

def run_delete_script():
    delete_script_path = os.path.join(EXECUTION_DIR, DELETE_SCRIPT)
    if not os.path.exists(delete_script_path):
        logger.error(f"Delete script not found: {delete_script_path}")
        return False

    logger.info(f"Executing cleanup: {DELETE_SCRIPT}")
    try:
        child_env = os.environ.copy()
        child_env['PYTHONIOENCODING'] = 'utf-8'
        result = subprocess.run(
            [sys.executable, delete_script_path],
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            check=True,
            env=child_env
        )
        logger.info(f"{DELETE_SCRIPT} completed")
        if result.stdout.strip():
            print(result.stdout.strip())
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"{DELETE_SCRIPT} failed:")
        if e.stderr:
            logger.error(e.stderr)
        return False
    except Exception as e:
        logger.error(f"Failed to run {DELETE_SCRIPT}: {e}")
        return False

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

def format_duration(seconds):
    """Convert seconds to hours + minutes string (no seconds)"""
    if seconds < 60:
        return f"0 hr {int(seconds)} min"
    td = timedelta(seconds=seconds)
    hours = td.days * 24 + td.seconds // 3600
    minutes = (td.seconds % 3600) // 60
    return f"{hours} hr {minutes} min"

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

    if not log_table_exists(cursor):
        conn.close()
        run_log_script()
        conn = setup_sql_connection()
        if not conn:
            sys.exit(1)
        cursor = conn.cursor()

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

    logger.info(f"Found {total_pending} pending configurations. Processing in groups of {BATCH_SIZE}...")

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

    for batch_start in range(0, len(pending_runs), BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, len(pending_runs))
        current_batch = pending_runs[batch_start:batch_end]
        batch_num = (batch_start // BATCH_SIZE) + 1
        batch_total = (len(pending_runs) + BATCH_SIZE - 1) // BATCH_SIZE

        logger.info(f"--- Starting batch {batch_num}/{batch_total} ({len(current_batch)} configs) ---")

        batch_success = True

        for idx, row in enumerate(current_batch, batch_start + 1):
            (analysis_run_id,
             swing_lookback, enable_min_swing, min_swing_pct,
             trendline_range,
             entry, entry_count,
             target_direction,
             l_pt_percent, l_sl_percent,
             s_pt_percent, s_sl_percent) = row

            logger.info(f"  Processing {idx}/{len(pending_runs)} (AnalysisRunID: {analysis_run_id})")

            update_log_status(cursor, analysis_run_id, 'RUNNING', f"Batch {batch_num}/{batch_total}")

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

            config_success = True
            for script_name in SCRIPT_LIST:
                script_path = os.path.join(EXECUTION_DIR, script_name)
                if not os.path.exists(script_path):
                    logger.error(f"Script not found: {script_path}")
                    config_success = False
                    continue

                logger.info(f"    Running: {script_name}")

                start_time = time.time()  # Start timing

                try:
                    child_env = os.environ.copy()
                    child_env['PYTHONIOENCODING'] = 'utf-8'
                    child_env['PYTHONUTF8'] = '1'

                    result = subprocess.run(
                        [sys.executable, script_path, config_json],
                        capture_output=True,
                        text=True,
                        encoding='utf-8',
                        errors='replace',
                        check=True,
                        env=child_env,
                        timeout=None  # No timeout - wait forever
                    )

                    end_time = time.time()
                    duration_sec = end_time - start_time
                    duration_str = format_duration(duration_sec)

                    logger.info(f"      {script_name} completed in {duration_str}")

                except subprocess.TimeoutExpired:
                    logger.error(f"      {script_name} timed out (should not happen with timeout=None)")
                    config_success = False
                except subprocess.CalledProcessError as e:
                    end_time = time.time()
                    duration_sec = end_time - start_time
                    duration_str = format_duration(duration_sec)
                    logger.error(f"      {script_name} failed after {duration_str} with code {e.returncode}")
                    if e.stderr:
                        logger.error(f"      stderr (truncated): {e.stderr[:1000]}...")
                    config_success = False
                except Exception as e:
                    end_time = time.time()
                    duration_sec = end_time - start_time
                    duration_str = format_duration(duration_sec)
                    logger.error(f"      Error running {script_name} after {duration_str}: {e}")
                    config_success = False

                time.sleep(1.5)

            final_status = 'COMPLETED' if config_success else 'ERROR'
            update_log_status(cursor, analysis_run_id, final_status,
                              f"{'Finished' if config_success else 'Failed'} in batch {batch_num}/{batch_total}")

            if not config_success:
                batch_success = False

        logger.info(f"Batch {batch_num}/{batch_total} finished. Pausing {PAUSE_AFTER_BATCH} seconds...")
        time.sleep(PAUSE_AFTER_BATCH)

        logger.info("Running table cleanup script...")
        delete_success = run_delete_script()
        if not delete_success:
            logger.warning("Cleanup script failed — continuing anyway.")

    conn.close()
    logger.info("All batches completed. Batch execution finished.")

def format_duration(seconds):
    """Convert seconds to 'X hr Y min' (no seconds)"""
    td = timedelta(seconds=int(seconds))
    hours = td.days * 24 + td.seconds // 3600
    minutes = (td.seconds % 3600) // 60
    if hours == 0:
        return f"{minutes} min"
    return f"{hours} hr {minutes} min"

if __name__ == "__main__":
    run_batch()