import sys
import os
import pyodbc
from dotenv import load_dotenv
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
    encoding='utf-8'
)
logger = logging.getLogger(__name__)

# === ROOT PATH SETUP ===
script_dir = os.path.dirname(os.path.abspath(__file__))  # VERSION folder
kraken_root = os.path.dirname(script_dir)  # Kraken_Crypto folder
if os.path.basename(kraken_root) != "Kraken_Crypto":
    logger.error("Script must be in Kraken_Crypto\\VERSION\\ folder.")
    sys.exit(1)

# === ASK USER: Version ===
version = input("Enter the version number (e.g., 001, 003, 050): ").strip()
if not version:
    logger.error("Version cannot be empty.")
    sys.exit(1)
version = version.zfill(3)
logger.info(f"Using version: {version}")

# === DYNAMIC PATHS AND FILES ===
version_folder = f"Kraken_Crypto_{version}"
config_path = os.path.join(kraken_root, version_folder, "CONFIG", "SQLSERVER")
env_filename = f"Crypto_{version}_sqlserver_local.env"
sql_env_file = os.path.join(config_path, env_filename)
if not os.path.exists(sql_env_file):
    logger.error(f"Config file not found: {sql_env_file}")
    logger.error(f"Check folder: {os.path.join(kraken_root, version_folder)}")
    sys.exit(1)
logger.info(f"Loading config from: {sql_env_file}")
load_dotenv(sql_env_file, encoding='utf-8')

# Load SQL credentials
sql_server = os.getenv("SQL_SERVER")
sql_db = os.getenv("SQL_DATABASE")
sql_user = os.getenv("SQL_USER")
sql_password = os.getenv("SQL_PASSWORD")
if not all([sql_server, sql_db, sql_user, sql_password]):
    logger.error("Missing SQL credentials in .env file!")
    sys.exit(1)

# Connect to database
logger.info(f"Connecting to {sql_db}...")
try:
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_db};"
        f"UID={sql_user};"
        f"PWD={sql_password};"
        f"TrustServerCertificate=yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    logger.info("Connected successfully.")
except Exception as e:
    logger.error(f"Connection failed: {e}")
    sys.exit(1)

# === TABLES TO DROP ===
TABLES_TO_DROP = [
    f"Crypto_{version}_DEV_01_00_Log",
    f"Crypto_{version}_DEV_01_02_Analysis_Results",
    f"Crypto_{version}_DEV_01_04_Analysis_Backtest",
    f"Crypto_{version}_DEV_01_06_Entry_Exit_Order",
    f"Crypto_{version}_DEV_01_07_Results_Analysis",
    f"Crypto_{version}_DEV_01_08_Portfolio_Balance",
    f"Crypto_{version}_DEV_01_09_Portfolio_Summary",
    # Add more tables here if needed
]

# Build full qualified table names
tables_to_drop = [f"[{sql_db}].[dbo].[{table}]" for table in TABLES_TO_DROP]

# === Show tables ===
logger.info("\nThe following tables will be checked and dropped if they exist:")
for table in tables_to_drop:
    logger.info(f"  {table}")

print()
# === SIMPLE Y/N CONFIRMATION ONLY ===
while True:
    confirm = input("Proceed with DROP (skip if not exist)? (Y/N): ").strip().lower()
    if confirm in ["y", "yes"]:
        break
    elif confirm in ["n", "no"]:
        logger.info("Operation cancelled by user.")
        conn.close()
        sys.exit(0)
    else:
        print("Please answer Y or N.")

# === Drop tables (skip if not exist) ===
dropped_count = 0
for table in tables_to_drop:
    try:
        cursor.execute(f"DROP TABLE {table}")
        conn.commit()
        logger.info(f"✓ Dropped: {table}")
        dropped_count += 1
    except pyodbc.Error as e:
        # Check if error is "table does not exist"
        if "does not exist" in str(e).lower() or "invalid object name" in str(e).lower():
            logger.info(f"→ Skipped (does not exist): {table}")
        else:
            logger.error(f"✗ Error dropping {table}: {e}")
            conn.rollback()

conn.close()
logger.info(f"Done. Dropped {dropped_count} tables (skipped non-existing ones).")