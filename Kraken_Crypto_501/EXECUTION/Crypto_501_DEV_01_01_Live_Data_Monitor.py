import os
import sys
import time
import json
import psutil
import pyodbc
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import pytz

# ================================
# WINDOWS CONSOLE SETUP
# ================================
if os.name == 'nt':  # Windows only
    os.system('mode con: cols=105 lines=35')

# ================================
# CONFIGURATION
# ================================
execution_dir = os.path.dirname(os.path.abspath(__file__))
base_path = os.path.dirname(execution_dir) if os.path.basename(execution_dir) == "EXECUTION" else execution_dir
config_path = os.path.join(base_path, "CONFIG")
params_file = os.path.join(config_path, "ZZ_PARAMETERS", "Crypto_501_parameters.json")

# Scripts to monitor
SCRIPTS_TO_MONITOR = [
    "Live_Data_All.py",
    "Live_Data_Kraken_1_min.py",
    "Live_Data_Coinbase_1_min.py",
    "Live_Data_Coinbase_5_min.py"
]

# Tables to monitor
TABLES_TO_MONITOR = [
    "dbo.Crypto_501_DEV_01_01_Live_Data_All",
    "dbo.Crypto_501_DEV_01_01_Live_Data_Kraken_1_min",
    "dbo.Crypto_501_DEV_01_01_Live_Data_Coinbase_1_min",
    "dbo.Crypto_501_DEV_01_01_Live_Data_Coinbase_5_min"
]

# ================================
# LOAD PARAMETERS
# ================================
params = {}
if os.path.exists(params_file):
    with open(params_file, 'r', encoding='utf-8') as f:
        params = json.load(f)

# ================================
# SQL CONNECTION SETUP
# ================================
def get_sql_connection():
    sql_mode = str(params.get("SQL_Connection_Mode", "2"))
    
    if sql_mode == "1":
        sql_env_file = os.path.join(config_path, "SQLSERVER", "Crypto_501_sqlserver_local.env")
    else:
        sql_env_file = os.path.join(config_path, "SQLSERVER", "Crypto_501_sqlserver_remote.env")
    
    if os.path.exists(sql_env_file):
        load_dotenv(sql_env_file, encoding='utf-8')
    
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={os.getenv('SQL_SERVER')};"
            f"DATABASE={os.getenv('SQL_DATABASE')};"
            f"UID={os.getenv('SQL_USER')};"
            f"PWD={os.getenv('SQL_PASSWORD')};"
            f"TrustServerCertificate=yes;"
        )
        return pyodbc.connect(conn_str)
    except Exception as e:
        return None

# ================================
# PROCESS MONITORING
# ================================
def check_process_running(script_name):
    """Check if a Python script is currently running"""
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = proc.info.get('cmdline', [])
            if cmdline and any(script_name in str(cmd) for cmd in cmdline):
                return True, proc.info['pid']
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return False, None

# ================================
# DATABASE MONITORING
# ================================
def check_table_status(conn, table_name):
    """Check the latest update time and row count for a table"""
    try:
        cursor = conn.cursor()
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        # Get latest timestamp
        cursor.execute(f"SELECT TOP 1 DateTime, DateTime_EST FROM {table_name} ORDER BY DateTime DESC")
        result = cursor.fetchone()
        
        if result:
            latest_utc = result[0]
            latest_est = result[1]
            
            # Calculate time since last update
            if latest_utc.tzinfo is None:
                latest_utc = pytz.utc.localize(latest_utc)
            
            now_utc = datetime.now(timezone.utc)
            time_diff = now_utc - latest_utc
            seconds_ago = int(time_diff.total_seconds())
            
            return {
                'exists': True,
                'row_count': row_count,
                'latest_utc': latest_utc,
                'latest_est': latest_est,
                'seconds_ago': seconds_ago,
                'is_updating': seconds_ago < 120  # Consider updating if < 2 minutes old
            }
        else:
            return {
                'exists': True,
                'row_count': 0,
                'latest_utc': None,
                'latest_est': None,
                'seconds_ago': None,
                'is_updating': False
            }
            
    except Exception as e:
        return {
            'exists': False,
            'error': str(e)
        }

# ================================
# DISPLAY FUNCTIONS
# ================================
def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def format_time_ago(seconds):
    if seconds is None:
        return "No data"
    elif seconds < 60:
        return f"{seconds}s ago"
    elif seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s ago"
    else:
        return f"{seconds // 3600}h {(seconds % 3600) // 60}m ago"

def get_status_symbol(is_good):
    return "✓" if is_good else "✗"

def display_status(process_status, table_status):
    clear_screen()
    
    print("=" * 100)
    print("  CRYPTO DATA COLLECTION - SYSTEM MONITOR")
    print("=" * 100)
    print(f"  Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 100)
    print()
    
    # Process Status
    print("┌─ PROCESS STATUS " + "─" * 81 + "┐")
    print("│" + " " * 98 + "│")
    for script, (running, pid) in process_status.items():
        status = get_status_symbol(running)
        if running:
            line = f"  [{status}] {script:<35} RUNNING (PID: {pid})"
            print(f"│ {line:<97}│")
        else:
            line = f"  [{status}] {script:<35} NOT RUNNING"
            print(f"│ {line:<97}│")
    print("│" + " " * 98 + "│")
    print("└" + "─" * 98 + "┘")
    print()
    
    # Table Status
    print("┌─ DATABASE TABLE STATUS " + "─" * 74 + "┐")
    print("│" + " " * 98 + "│")
    for table, status in table_status.items():
        table_short = table.replace("dbo.", "")
        
        if not status.get('exists', False):
            symbol = get_status_symbol(False)
            line = f"  [{symbol}] {table_short:<50} TABLE NOT FOUND"
            print(f"│ {line:<97}│")
        else:
            is_updating = status.get('is_updating', False)
            symbol = get_status_symbol(is_updating)
            row_count = status.get('row_count', 0)
            time_ago = format_time_ago(status.get('seconds_ago'))
            
            if status.get('latest_est'):
                latest_est_str = status['latest_est'].strftime('%Y-%m-%d %H:%M:%S EST')
                line1 = f"  [{symbol}] {table_short:<50} | Latest: {latest_est_str}"
                line2 = f"      {row_count:>6} rows | Updated: {time_ago}"
                print(f"│ {line1:<97}│")
                print(f"│ {line2:<97}│")
            else:
                line1 = f"  [{symbol}] {table_short:<50} | Latest: No data"
                line2 = f"      {row_count:>6} rows | Updated: {time_ago}"
                print(f"│ {line1:<97}│")
                print(f"│ {line2:<97}│")
    
    print("│" + " " * 98 + "│")
    print("└" + "─" * 98 + "┘")
    print()
    
    # Summary
    running_count = sum(1 for running, _ in process_status.values() if running)
    updating_count = sum(1 for status in table_status.values() if status.get('is_updating', False))
    
    print("┌─ SUMMARY " + "─" * 88 + "┐")
    line1 = f"  Processes Running: {running_count}/{len(process_status)}"
    line2 = f"  Tables Updating:   {updating_count}/{len(table_status)}"
    print(f"│ {line1:<97}│")
    print(f"│ {line2:<97}│")
    
    if running_count == len(process_status) and updating_count == len(table_status):
        line3 = "  Status: ✓ ALL SYSTEMS OPERATIONAL"
    else:
        line3 = "  Status: ✗ ISSUES DETECTED"
    print(f"│ {line3:<97}│")
    
    print("└" + "─" * 98 + "┘")
    print()
    print("Press Ctrl+C to exit")

# ================================
# MAIN MONITORING LOOP
# ================================
def main():
    print("Starting monitor...")
    print("Connecting to database...")
    
    conn = get_sql_connection()
    if not conn:
        print("ERROR: Could not connect to database!")
        print("Check your SQL connection settings in the parameters file.")
        input("Press Enter to exit...")
        sys.exit(1)
    
    print("Database connected successfully!")
    time.sleep(1)
    
    try:
        while True:
            # Check process status
            process_status = {}
            for script in SCRIPTS_TO_MONITOR:
                running, pid = check_process_running(script)
                process_status[script] = (running, pid)
            
            # Check table status
            table_status = {}
            for table in TABLES_TO_MONITOR:
                table_status[table] = check_table_status(conn, table)
            
            # Display
            display_status(process_status, table_status)
            
            # Refresh every 5 seconds
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\n\nMonitor stopped by user.")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()