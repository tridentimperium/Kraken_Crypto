import sys
import os
import logging
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as patches
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ================================
# LOGGING
# ================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ================================
# PATHS
# ================================
execution_dir = os.path.dirname(os.path.abspath(__file__))
base_path = os.path.dirname(execution_dir)
config_path = os.path.join(base_path, "CONFIG")

GRAPH_DIR = os.path.join(execution_dir, "Graph_Analysis")
os.makedirs(GRAPH_DIR, exist_ok=True)

# ================================
# TABLE
# ================================
ANALYSIS_TABLE = "dbo.Crypto_010_DEV_01_02_Analysis_Results"

# ================================
# LOAD PARAMETERS
# ================================
params_file = os.path.join(config_path, "ZZ_PARAMETERS", "Crypto_010_parameters.json")
if not os.path.exists(params_file):
    logger.error(f"Parameters file not found: {params_file}")
    sys.exit(1)

try:
    with open(params_file, 'r', encoding='utf-8') as f:
        params = json.load(f)
    logger.info(f"Loaded parameters: {params_file}")
except Exception as e:
    logger.error(f"Failed to load parameters: {e}")
    sys.exit(1)

symbol_id = params.get("Symbol_ID", "").strip().upper()
start_date_str = params.get("StartDate")
end_date_str = params.get("EndDate")

if not all([symbol_id, start_date_str, end_date_str]):
    logger.error("Missing Symbol_ID, StartDate, or EndDate in parameters.json")
    sys.exit(1)

try:
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
except ValueError as e:
    logger.error(f"Invalid date format: {e}")
    sys.exit(1)

# Prompt for AnalysisRunID
analysis_run_id = input("Enter the AnalysisRunID to use for graphing: ").strip()
if not analysis_run_id:
    logger.error("AnalysisRunID is required.")
    sys.exit(1)

logger.info(f"Graphing {symbol_id} from {start_date} to {end_date} with AnalysisRunID {analysis_run_id}")

# Create subdirectory for this AnalysisRunID
graph_subdir = os.path.join(GRAPH_DIR, f"AnalysisRunID_{analysis_run_id}")
os.makedirs(graph_subdir, exist_ok=True)

# ================================
# LOAD SQL CREDENTIALS
# ================================
sql_env_file = os.path.join(config_path, "SQLSERVER", "Crypto_010_sqlserver_local.env")
if not os.path.exists(sql_env_file):
    sql_env_file = os.path.join(config_path, "SQLSERVER", "Crypto_010_sqlserver_remote.env")

if not os.path.exists(sql_env_file):
    logger.error(f"SQL env file not found: {sql_env_file}")
    sys.exit(1)

load_dotenv(sql_env_file, encoding="utf-8")
logger.info(f"Loaded SQL env: {sql_env_file}")

required = ["SQL_SERVER", "SQL_DATABASE", "SQL_USER", "SQL_PASSWORD"]
missing = [k for k in required if not os.getenv(k)]
if missing:
    logger.error(f"Missing SQL env vars: {missing}")
    sys.exit(1)

# ================================
# SQLALCHEMY ENGINE
# ================================
conn_str = (
    f"mssql+pyodbc://{os.getenv('SQL_USER')}:{os.getenv('SQL_PASSWORD')}"
    f"@{os.getenv('SQL_SERVER')}/{os.getenv('SQL_DATABASE')}"
    "?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
)
try:
    engine = create_engine(conn_str, fast_executemany=True)
    logger.info("SQLAlchemy engine created")
except Exception as e:
    logger.error(f"Engine creation failed: {e}")
    sys.exit(1)

# ================================
# GENERATE DAILY CHARTS
# ================================
current_date = start_date
day_count = 0

while current_date <= end_date:
    day_start = datetime.combine(current_date, datetime.min.time())
    day_end = day_start + timedelta(days=1)

    query = f"""
    SELECT DateTime, [Close], [High], [Low], SwingType, Trend, BuySignal, SellSignal
    FROM {ANALYSIS_TABLE}
    WHERE Symbol = :symbol
      AND AnalysisRunID = :analysis_run_id
      AND DateTime >= :start
      AND DateTime < :end
    ORDER BY DateTime
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(
                text(query),
                conn,
                params={"symbol": symbol_id, "analysis_run_id": analysis_run_id, "start": day_start, "end": day_end}
            )
    except Exception as e:
        logger.error(f"Query failed for {current_date}: {e}")
        current_date += timedelta(days=1)
        continue

    if df.empty:
        logger.warning(f"No data for {current_date}")
        current_date += timedelta(days=1)
        continue

    df["DateTime"] = pd.to_datetime(df["DateTime"])
    df = df.set_index("DateTime")

    # Extract swings
    hh = df[df["SwingType"] == "HH"]
    ll = df[df["SwingType"] == "LL"]
    lh = df[df["SwingType"] == "LH"]
    hl = df[df["SwingType"] == "HL"]
    trend = df["Trend"].iloc[-1] if not df["Trend"].isna().all() else "Unknown"

    # Extract signals
    buys = df[df["BuySignal"] == 1]
    sells = df[df["SellSignal"] == 1]

    # Calculate offset for signal labels
    price_range = df['High'].max() - df['Low'].min()
    offset = price_range * 0.02  # 2% of price range for spacing

    # Plot
    plt.style.use("dark_background")
    fig, ax = plt.subplots(figsize=(14, 7))

    ax.plot(df.index, df["Close"], color="white", linewidth=1.2, label="Close")

    ax.scatter(hh.index, hh["High"], color="#00ff00", marker="^", s=120, label="HH", zorder=5, edgecolors="black", linewidth=0.5)
    ax.scatter(ll.index, ll["Low"], color="#ff0000", marker="v", s=120, label="LL", zorder=5, edgecolors="black", linewidth=0.5)
    ax.scatter(lh.index, lh["High"], color="#ff8800", marker="v", s=100, label="LH", zorder=5, edgecolors="black", linewidth=0.5)
    ax.scatter(hl.index, hl["Low"], color="#0088ff", marker="^", s=100, label="HL", zorder=5, edgecolors="black", linewidth=0.5)

    # Plot Buy signals (B below HH or HL)
    for _, row in buys.iterrows():
        y_pos = row['Low'] - offset
        ax.text(row.name, y_pos, 'B', color='white', ha='center', va='center',
                bbox=dict(facecolor='green', edgecolor='black', boxstyle='square,pad=0.3'), fontsize=10, zorder=10)

    # Plot Sell signals (S above LL or LH)
    for _, row in sells.iterrows():
        y_pos = row['High'] + offset
        ax.text(row.name, y_pos, 'S', color='white', ha='center', va='center',
                bbox=dict(facecolor='red', edgecolor='black', boxstyle='square,pad=0.3'), fontsize=10, zorder=10)

    ax.set_title(f"{symbol_id} | {current_date} | Trend: {trend}", fontsize=16, color="white")
    ax.set_ylabel("Price", fontsize=12, color="white")

    # Custom legend handles
    buy_patch = patches.Patch(facecolor='green', edgecolor='black', label='Buy Signal (B)')
    sell_patch = patches.Patch(facecolor='red', edgecolor='black', label='Sell Signal (S)')

    # Get handles and labels from existing legend items
    handles, labels = ax.get_legend_handles_labels()
    handles.extend([buy_patch, sell_patch])
    labels.extend(['Buy Signal (B)', 'Sell Signal (S)'])

    # Legend: right side, outside plot, single column
    ax.legend(handles=handles, labels=labels, 
              loc='center left', bbox_to_anchor=(1, 0.5), 
              ncol=1, frameon=True, fancybox=True, shadow=True, fontsize=10)

    ax.grid(True, alpha=0.3)

    # X-axis: time only (same day)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0)

    plt.tight_layout(rect=[0, 0, 0.85, 1])  # Make room for legend on the right

    # SAVE WITH YOUR NAMING
    filename = f"Analysis_Graph_{current_date}.png"
    output_path = os.path.join(graph_subdir, filename)
    plt.savefig(output_path, dpi=300, bbox_inches="tight", facecolor="black")
    logger.info(f"Saved: {output_path}")
    plt.close(fig)

    day_count += 1
    current_date += timedelta(days=1)

logger.info(f"Generated {day_count} daily charts in {graph_subdir}")