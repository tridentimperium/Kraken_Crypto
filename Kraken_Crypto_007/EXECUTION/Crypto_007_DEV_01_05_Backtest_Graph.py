import sys
import os
import logging
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as patches
import matplotlib.lines as mlines
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
GRAPH_DIR = os.path.join(execution_dir, "Graph_Backtest")
os.makedirs(GRAPH_DIR, exist_ok=True)

# ================================
# TABLE (BACKTEST)
# ================================
BACKTEST_TABLE = "dbo.Crypto_007_DEV_01_04_Analysis_Backtest"

# ================================
# LOAD PARAMETERS
# ================================
params_file = os.path.join(config_path, "ZZ_PARAMETERS", "Crypto_007_parameters.json")
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

analysis_run_id = input("Enter the AnalysisRunID used for backtest graphing: ").strip()
if not analysis_run_id:
    logger.error("AnalysisRunID is required.")
    sys.exit(1)
logger.info(f"Graphing BACKTEST for {symbol_id} from {start_date} to {end_date} with AnalysisRunID {analysis_run_id}")

graph_subdir = os.path.join(GRAPH_DIR, f"Backtest_AnalysisRunID_{analysis_run_id}")
os.makedirs(graph_subdir, exist_ok=True)

# ================================
# LOAD SQL CREDENTIALS & ENGINE
# ================================
sql_env_file = os.path.join(config_path, "SQLSERVER", "Crypto_007_sqlserver_local.env")
if not os.path.exists(sql_env_file):
    sql_env_file = os.path.join(config_path, "SQLSERVER", "Crypto_007_sqlserver_remote.env")
if not os.path.exists(sql_env_file):
    logger.error(f"SQL env file not found: {sql_env_file}")
    sys.exit(1)
load_dotenv(sql_env_file, encoding="utf-8")
required = ["SQL_SERVER", "SQL_DATABASE", "SQL_USER", "SQL_PASSWORD"]
missing = [k for k in required if not os.getenv(k)]
if missing:
    logger.error(f"Missing SQL env vars: {missing}")
    sys.exit(1)

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
    SELECT DateTime, [Close], [High], [Low], SwingType, Trend,
           BuySignal, SellSignal, LongShort, InTrade,
           L_PTPrice, L_SLPrice, S_PTPrice, S_SLPrice,
           EntryExit
    FROM {BACKTEST_TABLE}
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

    # Swings
    hh = df[df["SwingType"] == "HH"]
    ll = df[df["SwingType"] == "LL"]
    lh = df[df["SwingType"] == "LH"]
    hl = df[df["SwingType"] == "HL"]
    trend = df["Trend"].iloc[-1] if not df["Trend"].isna().all() else "Unknown"

    # Entries
    entries = df[df['EntryExit'] == 1.0]
    buys = entries[entries['LongShort'] == 'Long']
    sells = entries[entries['LongShort'] == 'Short']

    price_range = df['High'].max() - df['Low'].min()
    offset = price_range * 0.02

    # Plot
    plt.style.use("dark_background")
    fig, ax = plt.subplots(figsize=(14, 7))
    close_line = ax.plot(df.index, df["Close"], color="white", linewidth=1.2)[0]

    # Swings
    ax.scatter(hh.index, hh["High"], color="#00ff00", marker="^", s=120, zorder=5, edgecolors="black", linewidth=0.5)
    ax.scatter(hl.index, hl["Low"], color="#0088ff", marker="^", s=100, zorder=5, edgecolors="black", linewidth=0.5)
    ax.scatter(ll.index, ll["Low"], color="#ff0000", marker="v", s=120, zorder=5, edgecolors="black", linewidth=0.5)
    ax.scatter(lh.index, lh["High"], color="#ff8800", marker="v", s=100, zorder=5, edgecolors="black", linewidth=0.5)

    # Trade blocks
    df['trade_block'] = (df['LongShort'] != df['LongShort'].shift(1)).cumsum()
    trade_blocks = df[df['LongShort'].notna()].groupby('trade_block')

    long_added = False
    short_added = False
    long_pt_added = False
    long_sl_added = False
    short_pt_added = False
    short_sl_added = False

    for _, block in trade_blocks:
        start_time = block.index.min()
        end_time = block.index.max()
        first_row = block.iloc[0]
        is_long = first_row['LongShort'] == 'Long'

        bg_color = "#004400" if is_long else "#440000"
        trade_label = None
        if is_long and not long_added:
            trade_label = "Long Trade"
            long_added = True
        elif not is_long and not short_added:
            trade_label = "Short Trade"
            short_added = True

        ax.axvspan(start_time, end_time, color=bg_color, alpha=0.35, label=trade_label)

        # Long PT/SL
        if is_long:
            pt_val = block['L_PTPrice'].dropna().iloc[0] if not block['L_PTPrice'].dropna().empty else None
            sl_val = block['L_SLPrice'].dropna().iloc[0] if not block['L_SLPrice'].dropna().empty else None

            if pd.notna(pt_val):
                ax.plot([start_time, end_time], [pt_val, pt_val], color="lime", linewidth=1.8, alpha=0.9,
                        label="Long PT" if not long_pt_added else None, zorder=3)
                long_pt_added = True

            if pd.notna(sl_val):
                ax.plot([start_time, end_time], [sl_val, sl_val], color="red", linewidth=1.8, alpha=0.9,
                        label="Long SL" if not long_sl_added else None, zorder=3)
                long_sl_added = True

        # Short PT/SL
        else:
            pt_val = block['S_PTPrice'].dropna().iloc[0] if not block['S_PTPrice'].dropna().empty else None
            sl_val = block['S_SLPrice'].dropna().iloc[0] if not block['S_SLPrice'].dropna().empty else None

            if pd.notna(pt_val):
                ax.plot([start_time, end_time], [pt_val, pt_val], color="cyan", linewidth=1.8, alpha=0.9,
                        label="Short PT" if not short_pt_added else None, zorder=3)
                short_pt_added = True

            if pd.notna(sl_val):
                ax.plot([start_time, end_time], [sl_val, sl_val], color="magenta", linewidth=1.8, alpha=0.9,
                        label="Short SL" if not short_sl_added else None, zorder=3)
                short_sl_added = True

    # Entry labels
    for idx, row in buys.iterrows():
        y_pos = row['Low'] - offset
        ax.text(idx, y_pos, 'B', color='white', ha='center', va='center',
                bbox=dict(facecolor='green', edgecolor='black', boxstyle='square,pad=0.3'), fontsize=12, fontweight='bold', zorder=10)
    for idx, row in sells.iterrows():
        y_pos = row['High'] + offset
        ax.text(idx, y_pos, 'S', color='white', ha='center', va='center',
                bbox=dict(facecolor='red', edgecolor='black', boxstyle='square,pad=0.3'), fontsize=12, fontweight='bold', zorder=10)

    # Title
    trade_status = " (In Trade)" if df['InTrade'].any() else ""
    ax.set_title(f"{symbol_id} | {current_date} | Trend: {trend}{trade_status} [BACKTEST]", fontsize=16, color="white")
    ax.set_ylabel("Price", fontsize=12, color="white")

    # Legend
    handles = [close_line]
    labels = ["Close"]

    handles += [
        mlines.Line2D([], [], color="#00ff00", marker="^", markersize=10, linestyle='None', markeredgecolor='black'),
        mlines.Line2D([], [], color="#0088ff", marker="^", markersize=9, linestyle='None', markeredgecolor='black'),
        mlines.Line2D([], [], color="#ff0000", marker="v", markersize=10, linestyle='None', markeredgecolor='black'),
        mlines.Line2D([], [], color="#ff8800", marker="v", markersize=9, linestyle='None', markeredgecolor='black'),
    ]
    labels += ["HH", "HL", "LL", "LH"]

    if long_added:
        handles.append(patches.Patch(color="#004400", alpha=0.35))
        labels.append("Long Trade")
    if short_added:
        handles.append(patches.Patch(color="#440000", alpha=0.35))
        labels.append("Short Trade")

    if long_pt_added:
        handles.append(mlines.Line2D([], [], color="lime", linewidth=1.8))
        labels.append("Long PT")
    if long_sl_added:
        handles.append(mlines.Line2D([], [], color="red", linewidth=1.8))
        labels.append("Long SL")

    if short_pt_added:
        handles.append(mlines.Line2D([], [], color="cyan", linewidth=1.8))
        labels.append("Short PT")
    if short_sl_added:
        handles.append(mlines.Line2D([], [], color="magenta", linewidth=1.8))
        labels.append("Short SL")

    ax.legend(handles, labels, loc='center left', bbox_to_anchor=(1, 0.5), ncol=1,
              frameon=True, fancybox=True, shadow=True, fontsize=10)

    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0)
    plt.tight_layout(rect=[0, 0, 0.85, 1])

    filename = f"Backtest_Graph_{current_date}.png"
    output_path = os.path.join(graph_subdir, filename)
    plt.savefig(output_path, dpi=300, bbox_inches="tight", facecolor="black")
    logger.info(f"Saved: {output_path}")
    plt.close(fig)

    day_count += 1
    current_date += timedelta(days=1)

logger.info(f"Generated {day_count} backtest daily charts in {graph_subdir}")