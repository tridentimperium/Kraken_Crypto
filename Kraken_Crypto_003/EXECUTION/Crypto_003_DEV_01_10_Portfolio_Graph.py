import sys
import os
import pyodbc
import logging
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
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
logger.info("--- STARTING PORTFOLIO EQUITY GRAPH GENERATION ---")

# ================================
# PATHS
# ================================
EXECUTION_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_PATH = os.path.dirname(EXECUTION_DIR)
CONFIG_PATH = os.path.join(BASE_PATH, "CONFIG")
GRAPH_DIR = os.path.join(EXECUTION_DIR, "Graph_Portfolio")
os.makedirs(GRAPH_DIR, exist_ok=True)

# ================================
# TABLE NAME
# ================================
SOURCE_TABLE = "dbo.Crypto_003_DEV_01_08_Portfolio_Balance"

# ================================
# LOAD CONFIG / PROMPT FOR AnalysisRunID
# ================================
vars_config = {}
batch_mode = len(sys.argv) > 1

if batch_mode:
    try:
        vars_config = json.loads(sys.argv[1])
        logger.info("Loaded config from batch (JSON argument)")
    except Exception as e:
        logger.error(f"Failed to parse JSON argument: {e}")
        sys.exit(1)
else:
    # Interactive mode
    print("\n" + "="*70)
    print(" Portfolio Equity Curve Graph Generator (Black Theme)")
    print(" Data source: dbo.Crypto_003_DEV_01_08_Portfolio_Balance")
    print("="*70)
    while True:
        run_id_input = input("Enter the AnalysisRunID to graph: ").strip()
        try:
            ANALYSIS_RUN_ID = int(run_id_input)
            if ANALYSIS_RUN_ID <= 0:
                print("Please enter a positive integer.")
                continue
            break
        except ValueError:
            print("Invalid input. Please enter a valid integer.")
    logger.info(f"User selected AnalysisRunID = {ANALYSIS_RUN_ID}")
    vars_config["AnalysisRunID"] = ANALYSIS_RUN_ID
    vars_config["FetchRunID"] = vars_config.get("FetchRunID", 1)  # default

# Extract IDs
FETCH_RUN_ID = int(vars_config.get("FetchRunID", 1))
ANALYSIS_RUN_ID = int(vars_config.get("AnalysisRunID", 1))

logger.info(f"Graphing for FetchRunID = {FETCH_RUN_ID}, AnalysisRunID = {ANALYSIS_RUN_ID}")

# ================================
# SQL CONNECTION
# ================================
sql_env_file = os.path.join(CONFIG_PATH, "SQLSERVER", "Crypto_003_sqlserver_local.env")
if not os.path.exists(sql_env_file):
    sql_env_file = os.path.join(CONFIG_PATH, "SQLSERVER", "Crypto_003_sqlserver_remote.env")
if not os.path.exists(sql_env_file):
    logger.error(f"SQL env file not found: {sql_env_file}")
    sys.exit(1)

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
    conn = pyodbc.connect(conn_str)
    logger.info("Connected to SQL Server")
except Exception as e:
    logger.error(f"SQL connection failed: {e}")
    sys.exit(1)

# ================================
# LOAD PORTFOLIO BALANCE DATA
# ================================
query = f"""
SELECT 
    Symbol,
    ExecutionDate,
    StartingBalance,
    EndingBalance,
    PercentageChange
FROM {SOURCE_TABLE}
WHERE FetchRunID = ? 
  AND AnalysisRunID = ?
ORDER BY Symbol, ExecutionDate
"""
try:
    df = pd.read_sql(query, conn, params=[FETCH_RUN_ID, ANALYSIS_RUN_ID])
    logger.info(f"Loaded {len(df)} daily balance rows.")
except Exception as e:
    logger.error(f"Failed to load data: {e}")
    conn.close()
    sys.exit(1)

conn.close()

if df.empty:
    logger.warning("No portfolio balance data found for this AnalysisRunID.")
    sys.exit(0)

df['ExecutionDate'] = pd.to_datetime(df['ExecutionDate'])

# ================================
# GENERATE GRAPHS (black theme, two subplots)
# ================================
today_str = datetime.now().strftime("%Y-%m-%d")
output_subdir = os.path.join(GRAPH_DIR, f"Equity_Black_{today_str}_RunID_{ANALYSIS_RUN_ID}")
os.makedirs(output_subdir, exist_ok=True)

plt.style.use('dark_background')

for symbol in df['Symbol'].unique():
    df_sym = df[df['Symbol'] == symbol].sort_values('ExecutionDate').copy()
    
    if len(df_sym) < 2:
        logger.warning(f"Not enough data for {symbol} (only {len(df_sym)} days)")
        continue

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), sharex=True,
                                   gridspec_kw={'height_ratios': [3, 1]})

    # Top plot: Starting & Ending Balance
    ax1.plot(df_sym['ExecutionDate'], df_sym['StartingBalance'], 
             color='#1e90ff', linestyle='--', linewidth=1.8, 
             label='Starting Balance', alpha=0.8)
    ax1.plot(df_sym['ExecutionDate'], df_sym['EndingBalance'], 
             color='#00ffcc', linewidth=2.5, marker='o', markersize=6,
             label='Ending Balance')
    
    ax1.set_ylabel('Portfolio Balance ($)', color='#00ffcc', fontsize=12)
    ax1.tick_params(axis='y', labelcolor='#00ffcc')
    ax1.grid(True, alpha=0.25, linestyle='--', color='gray')

    # Bottom plot: % Change as columns (green positive, red negative)
    colors = ['#00cc66' if x >= 0 else '#ff4444' for x in df_sym['PercentageChange']]
    ax2.bar(df_sym['ExecutionDate'], df_sym['PercentageChange'], 
            color=colors, width=0.6, edgecolor='black', linewidth=0.5)
    
    ax2.set_ylabel('Daily % Change', color='white', fontsize=12)
    ax2.axhline(0, color='gray', linewidth=1, linestyle='--', alpha=0.5)
    ax2.grid(True, alpha=0.25, axis='y', linestyle='--', color='gray')

    # Shared formatting
    for ax in [ax1, ax2]:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator(maxticks=15))
        plt.setp(ax.get_xticklabels(), rotation=45, ha='right')
        ax.tick_params(axis='x', colors='white')
        ax.tick_params(axis='y', colors='white')

    # Title & Legend
    fig.suptitle(f"Portfolio Equity Curve - {symbol}\n"
                 f"AnalysisRunID: {ANALYSIS_RUN_ID} | FetchRunID: {FETCH_RUN_ID}\n"
                 f"Period: {df_sym['ExecutionDate'].min().date()} to {df_sym['ExecutionDate'].max().date()}",
                 fontsize=16, color='white', y=0.98)

    ax1.legend(loc='upper left', fontsize=10, frameon=True, facecolor='black', edgecolor='gray')

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    
    filename = f"Equity_Curve_Black_{symbol}_RunID_{ANALYSIS_RUN_ID}.png"
    save_path = os.path.join(output_subdir, filename)
    plt.savefig(save_path, dpi=300, bbox_inches='tight', facecolor='black')
    plt.close(fig)
    logger.info(f"Saved: {save_path}")

logger.info(f"All graphs saved in: {output_subdir}")
logger.info("Portfolio equity graph generation finished successfully.")