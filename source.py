import yfinance as yf
import pandas as pd
import numpy as np
import sqlite3
import schedule
import time
import logging
import datetime as dt
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
import os

# --- Configuration ---
# CONFIG is kept global as it's typically a module-level setting.
# It is passed to functions that need it to maintain good practice and allow flexibility.
CONFIG = {
    "tickers": [
        "AAPL", "MSFT", "GOOG", "AMZN", "META",
        "JPM", "BAC", "GS", "JNJ", "PFE",
        "XOM", "CVX", "PG", "KO", "WMT",
        "TSLA", "NVDA", "V", "MA", "DIS",
    ],
    "start_date": "2020-01-01",
    "end_date": datetime.now().strftime("%Y-%m-%d"), # Dynamically set for pipeline runs
    "interval": "1d",
    "db_path": "market_data.db",
    "csv_backup_dir": "./data_backup/",
    "run_time": "16:30", # For scheduler, indicates when pipeline should run
    "max_missing_pct": 0.02, # Maximum allowed percentage of missing trading days
    "stale_hours_threshold": 24, # Threshold for considering data stale (not used in current QC, but good config)
    "min_volume_threshold": 100, # Minimum acceptable trading volume for a day
    "extreme_return_threshold": 0.50, # Threshold for detecting extreme daily price movements
    "log_file": "pipeline.log",
    "log_level": "INFO",
    "T_manual": 0.75, # Estimated manual hours per day for data handling
    "D_annual": 252,  # Average annual trading days
    "T_setup": 8,     # Hours required to set up the automated pipeline
    "T_maint": 10,    # Annual maintenance hours for the automated pipeline
    "manual_error_rate": 0.02, # Estimated error rate for manual data handling
    "automated_error_rate": 0.0001, # Estimated error rate for automated data handling
    "avg_data_points_per_ticker": 6, # Average daily data points (e.g., OHLCV) per ticker
}

# --- Setup Logging ---
# Logging setup is typically done once at module load.
os.makedirs(CONFIG["csv_backup_dir"], exist_ok=True) # Ensure backup directory exists

logging.basicConfig(
    filename=CONFIG["log_file"],
    level=getattr(logging, CONFIG["log_level"]),
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("data_pipeline")

# Suppress yfinance verbose logging, if desired
# logging.getLogger("yfinance").setLevel(logging.WARNING)


# --- Core Data Pipeline Functions ---

def init_database(db_path: str):
    """
    Establishes a connection to the SQLite database and creates the 'market_data'
    table if it doesn't already exist. The table includes columns for ticker,
    date, OHLCV, dividend, and a timestamp for when the record was last updated.

    Args:
        db_path (str): The file path to the SQLite database.

    Returns:
        sqlite3.Connection: An active connection object to the database.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS market_data (
            ticker TEXT NOT NULL,
            date DATE NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            dividend REAL,
            updated_at TIMESTAMP,
            PRIMARY KEY (ticker, date)
        )
        """
    )
    conn.commit()
    logger.info(f"Database initialized at {db_path} with `market_data` table.")
    return conn

def fetch_market_data(tickers: list, start: str, end: str, interval="1d", max_retries=3, retry_delay=5):
    """
    Fetches historical OHLCV (Open, High, Low, Close, Volume) data from Yahoo Finance
    for a list of given ticker symbols within a specified date range. Includes
    retry logic for resilient data extraction.

    Args:
        tickers (list): A list of stock ticker symbols (e.g., ["AAPL", "MSFT"]).
        start (str): The start date for data extraction in "YYYY-MM-DD" format.
        end (str): The end date for data extraction in "YYYY-MM-DD" format.
        interval (str): The data interval (e.g., "1d" for daily, "1wk" for weekly).
        max_retries (int): The maximum number of attempts to fetch data for a ticker
                           if an initial attempt fails.
        retry_delay (int): The initial delay in seconds before the first retry.
                           Subsequent retries use an exponential backoff.

    Returns:
        tuple: A tuple containing:
            - all_data (dict): A dictionary where keys are ticker symbols and values
                               are pandas DataFrames containing the fetched OHLCV data.
            - failed_tickers (list): A list of ticker symbols for which data extraction failed
                                     after all retries.
    """
    all_data = {}
    failed_tickers = []

    logger.info(f"Starting data extraction for {len(tickers)} tickers from {start} to {end}.")

    for ticker in tickers:
        df = pd.DataFrame()

        for attempt in range(max_retries):
            try:
                logger.debug(f"Attempt {attempt + 1}/{max_retries} to fetch data for {ticker}...")

                df = yf.download(
                    ticker,
                    start=start,
                    end=end,
                    interval=interval,
                    progress=False,
                    auto_adjust=True, # Automatically adjust for splits and dividends
                )

                if df.empty:
                    # If df is empty, it could mean no data in range or an API issue.
                    # Check for 'Open' column existence as a more robust indicator of valid (though empty) data structure.
                    if "Open" not in df.columns and "open" not in df.columns:
                         raise ValueError(f"Empty or malformed data returned for {ticker} (no 'Open' column).")
                    else:
                        logger.warning(f"No data points retrieved for {ticker} in the specified range. DataFrame is empty.")
                        break # No need to retry if no data points are available

                # Robustly flatten and normalize column names
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns.values]
                
                df.columns = [str(c).lower().replace(" ", "_").replace(".", "") for c in df.columns]

                df["ticker"] = ticker
                df.index.name = "date"
                df.reset_index(inplace=True)

                # Ensure 'date' column is datetime type
                df["date"] = pd.to_datetime(df["date"])

                logger.info(
                    f"OK: {ticker} | {len(df)} rows "
                    f"({df['date'].min().strftime('%Y-%m-%d')} "
                    f"to {df['date'].max().strftime('%Y-%m-%d')})"
                )

                all_data[ticker] = df
                break # Success, move to next ticker

            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed for {ticker}: {e}")

                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1)) # Exponential backoff
                else:
                    logger.error(f"FAILED: {ticker} after {max_retries} attempts.")
                    failed_tickers.append(ticker)

    if failed_tickers:
        logger.warning(f"Failed tickers: {failed_tickers}")

    logger.info(f"Extraction complete: {len(all_data)}/{len(tickers)} tickers successful.")
    return all_data, failed_tickers

def clean_market_data(df: pd.DataFrame, ticker: str, config: dict):
    """
    Performs a series of cleaning and validation steps on a single ticker's
    historical market data DataFrame. This includes handling duplicates,
    missing values, detecting outliers, and adding audit columns.

    Args:
        df (pd.DataFrame): The raw DataFrame containing market data for a single ticker.
        ticker (str): The ticker symbol corresponding to the data.
        config (dict): A configuration dictionary containing thresholds and settings
                       for cleaning (e.g., `extreme_return_threshold`, `min_volume_threshold`).

    Returns:
        tuple: A tuple containing:
            - cleaned_df (pd.DataFrame): The processed and cleaned DataFrame.
            - quality_report (dict): A dictionary summarizing the cleaning operations
                                     and identified data quality issues.
    """
    report = {"ticker": ticker, "raw_rows": len(df)}
    initial_rows = len(df)

    if df.empty:
        logger.warning(f"Empty DataFrame passed for cleaning {ticker}. Returning empty df and partial report.")
        report.update({
            "dupes_removed": 0, "missing_trading_days": 0,
            "open_imputed": 0, "high_imputed": 0, "low_imputed": 0, "close_imputed": 0,
            "price_outliers": 0, "low_volume_days": 0, "clean_rows": 0
        })
        return pd.DataFrame(), report

    # 1. Deduplicate based on 'date'
    df_deduped = df.drop_duplicates(subset=["date"])
    report["dupes_removed"] = initial_rows - len(df_deduped)
    df = df_deduped.copy()

    # 2. Sort by date
    df = df.sort_values("date").reset_index(drop=True)

    # 3. Calculate missing trading days (business-day calendar)
    min_date = pd.to_datetime(df["date"].min())
    max_date = pd.to_datetime(df["date"].max())
    # Use business date range to count expected trading days
    date_range = pd.bdate_range(start=min_date, end=max_date)
    actual_dates = set(pd.to_datetime(df["date"]))
    missing_trading_days = len(date_range) - len(actual_dates.intersection(date_range))
    report["missing_trading_days"] = missing_trading_days

    # 4. Forward-fill missing prices (Open, High, Low, Close)
    for col in ["open", "high", "low", "close"]:
        n_missing = df[col].isnull().sum()
        if n_missing > 0:
            df[col] = df[col].ffill()
        report[f"{col}_imputed"] = int(n_missing)

    # 5. Detect extreme daily returns
    df["daily_return"] = df["close"].pct_change()
    outlier_mask = df["daily_return"].abs() > config["extreme_return_threshold"]
    report["price_outliers"] = int(outlier_mask.sum())

    # 6. Detect low volume days
    low_vol_mask = df["volume"] < config["min_volume_threshold"]
    report["low_volume_days"] = int(low_vol_mask.sum())

    # 7. Ensure 'dividend' column exists and fill NaNs
    if "dividend" not in df.columns:
        df["dividend"] = 0.0
    else:
        df["dividend"] = df["dividend"].fillna(0.0)

    # 8. Add 'updated_at' timestamp
    df["updated_at"] = pd.Timestamp.now()

    # 9. Cleanup temporary columns
    df.drop(columns=["daily_return"], inplace=True, errors="ignore")

    report["clean_rows"] = len(df)

    logger.info(
        f"Cleaned {ticker}: {report['dupes_removed']} duplicates removed, "
        f"{report['price_outliers']} price outliers, {report['low_volume_days']} low volume days, "
        f"{report['missing_trading_days']} missing trading days."
    )

    return df, report

def upsert_data(conn: sqlite3.Connection, df: pd.DataFrame):
    """
    Inserts new records or updates existing ones into the 'market_data' table
    in the database. This operation is idempotent, meaning running it multiple
    times with the same data will produce the same result without creating duplicates.

    Args:
        conn (sqlite3.Connection): An active database connection object.
        df (pd.DataFrame): The DataFrame containing cleaned market data to be upserted.
                          Must include 'ticker', 'date', 'open', 'high', 'low', 'close',
                          'volume', 'dividend', and 'updated_at' columns.
    """
    if df is None or df.empty:
        logger.warning("Attempted to upsert an empty DataFrame. No data loaded.")
        return

    # Select and copy relevant columns for database insertion
    df_to_insert = df[
        ["ticker", "date", "open", "high", "low", "close", "volume", "dividend", "updated_at"]
    ].copy()

    # Format 'date' and 'updated_at' to be SQLite-compatible strings
    df_to_insert["date"] = pd.to_datetime(df_to_insert["date"]).dt.strftime("%Y-%m-%d")
    df_to_insert["updated_at"] = pd.to_datetime(df_to_insert["updated_at"]).dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    # Coerce numeric types to native Python types and handle NaNs by converting to None (for DB NULL)
    numeric_cols = ["open", "high", "low", "close", "dividend"]
    for col in numeric_cols:
        df_to_insert[col] = pd.to_numeric(df_to_insert[col], errors="coerce").replace({np.nan: None})

    # Handle volume specifically, filling NaNs with 0 before converting to integer
    df_to_insert["volume"] = pd.to_numeric(df_to_insert["volume"], errors="coerce").fillna(0).astype(int)

    records = df_to_insert.to_dict("records")

    sql = """
    INSERT OR REPLACE INTO market_data
    (ticker, date, open, high, low, close, volume, dividend, updated_at)
    VALUES (:ticker, :date, :open, :high, :low, :close, :volume, :dividend, :updated_at)
    """
    try:
        conn.executemany(sql, records)
        conn.commit()
        # Log a sample ticker from the batch for better context
        sample_ticker = df_to_insert['ticker'].iloc[0] if not df_to_insert.empty else "N/A"
        logger.info(f"Upserted {len(records)} records for ticker {sample_ticker}.")
    except sqlite3.Error as e:
        logger.error(f"SQLite error during upsert for ticker {sample_ticker}: {e}", exc_info=True)
        conn.rollback()

# --- Quality Check Functions ---

def run_quality_checks(conn: sqlite3.Connection, tickers: list, config: dict):
    """
    Executes a suite of post-load quality checks on the data stored in the database
    for a given list of tickers. Checks include data freshness, completeness,
    validity of prices, and absence of extreme price movements.

    Args:
        conn (sqlite3.Connection): An active database connection object.
        tickers (list): A list of ticker symbols to perform quality checks on.
        config (dict): A configuration dictionary containing thresholds and settings
                       for quality checks (e.g., `max_missing_pct`, `extreme_return_threshold`).

    Returns:
        pd.DataFrame: A DataFrame summarizing the results of all quality checks
                      for each ticker, including a 'PASS' column indicating overall status.
    """
    results = []
    logger.info("Starting data quality checks...")

    # Set timezone to America/New_York to align with US market hours
    now_et = pd.Timestamp.now(tz="America/New_York")

    # Determine the expected latest trading day based on yfinance's end date exclusivity
    # yfinance's `end` parameter is exclusive for daily bars, meaning it fetches data
    # up to, but not including, the `end` date. So, the expected latest bar is typically
    # the business day prior to the `config["end_date"]`.
    end_exclusive = pd.to_datetime(config["end_date"])
    end_exclusive = (
        end_exclusive.tz_localize("America/New_York", ambiguous='NaT', nonexistent='NaT')
        if end_exclusive.tzinfo is None
        else end_exclusive.tz_convert("America/New_York")
    )
    expected_latest_day = (end_exclusive - pd.tseries.offsets.BDay(1)).normalize()

    for ticker in tickers:
        checks = {"ticker": ticker}

        # Fetch recent data for quality checks (e.g., last 252 trading days for annual context)
        df_qc = pd.read_sql(
            "SELECT date, close, volume FROM market_data WHERE ticker=? ORDER BY date DESC LIMIT 252",
            conn,
            params=(ticker,),
            parse_dates=["date"],
        )

        if df_qc.empty:
            logger.warning(f"No data found for {ticker} in DB for quality checks.")
            checks.update(
                {
                    "fresh": False,
                    "freshness_hours": None,
                    "completeness": 0.0,
                    "complete": False,
                    "valid_prices": False,
                    "no_extreme_moves": False,
                    "PASS": False,
                }
            )
            results.append(checks)
            continue

        # ---- Freshness Check ----
        latest_dt_db = pd.to_datetime(df_qc["date"].max())
        latest_day_db = latest_dt_db.normalize()

        # Check if the latest date in the DB is at least the expected latest trading day
        # Compare without timezone as both are normalized dates.
        checks["fresh"] = latest_day_db >= expected_latest_day.tz_localize(None)

        # Calculate freshness in hours since the expected market close of the latest DB day
        latest_db_day_close_dt = latest_day_db + pd.Timedelta(hours=16) # Assume market closes 16:00 ET
        latest_db_day_close_dt_et = latest_db_day_close_dt.tz_localize("America/New_York", ambiguous='NaT', nonexistent='NaT')
        checks["freshness_hours"] = round((now_et - latest_db_day_close_dt_et).total_seconds() / 3600, 1)

        # ---- Completeness Check ----
        min_date_qc = df_qc["date"].min()
        max_date_qc = df_qc["date"].max()

        # Get all unique trading dates for this specific ticker within its date range
        # This provides the 'expected' number of trading days for the ticker's data span.
        df_all_sessions_for_ticker = pd.read_sql(
            "SELECT DISTINCT date FROM market_data WHERE ticker=? AND date BETWEEN ? AND ? ORDER BY date",
            conn,
            params=(ticker, min_date_qc.strftime('%Y-%m-%d'), max_date_qc.strftime('%Y-%m-%d')),
            parse_dates=["date"],
        )
        expected_days = df_all_sessions_for_ticker["date"].nunique()
        actual_days = df_qc["date"].nunique()

        completeness_ratio = actual_days / max(expected_days, 1) if expected_days > 0 else 0.0
        checks["completeness"] = round(completeness_ratio, 4)
        checks["complete"] = completeness_ratio >= (1 - config["max_missing_pct"])

        # ---- Valid Prices Check ----
        checks["valid_prices"] = (df_qc["close"] > 0).all() and (df_qc["volume"] >= 0).all() # Volume can be zero

        # ---- Extreme Moves Check ----
        df_qc = df_qc.sort_values("date")
        df_qc["ret"] = df_qc["close"].pct_change()
        checks["no_extreme_moves"] = (
            df_qc["ret"].abs().fillna(0) < config["extreme_return_threshold"]
        ).all()

        # Overall PASS/FAIL
        checks["PASS"] = all(
            [
                checks["fresh"],
                checks["complete"],
                checks["valid_prices"],
                checks["no_extreme_moves"],
            ]
        )
        results.append(checks)

    report_df = pd.DataFrame(results)
    n_pass = int(report_df["PASS"].sum())
    n_fail = len(report_df) - n_pass

    logger.info(f"Quality Check Summary: {n_pass} PASS, {n_fail} FAIL.")

    if n_fail > 0:
        failed_tickers_report = report_df[~report_df["PASS"]]
        logger.warning(f"FAILED tickers:\n{failed_tickers_report.to_string()}")

    logger.info(
        f"Freshness rule: expected_latest_day={expected_latest_day.date()} "
        f"(derived from config['end_date'](exclusive)={end_exclusive.date()})"
    )

    return report_df

# --- Visualization Functions ---

def plot_price_series(conn: sqlite3.Connection, tickers: list, days: int = 30):
    """
    Generates and returns a Matplotlib figure object containing plots of the
    closing price series for specified tickers over the last 'days'.

    Args:
        conn (sqlite3.Connection): An active database connection object.
        tickers (list): A list of ticker symbols to plot.
        days (int): The number of most recent trading days to include in the plot.

    Returns:
        matplotlib.figure.Figure or None: The generated Matplotlib Figure object,
                                         or None if no tickers are provided or data is unavailable.
    """
    if not tickers:
        logger.warning("No tickers provided for price series plot.")
        return None

    fig, axes = plt.subplots(
        len(tickers),
        1,
        figsize=(12, 4 * len(tickers)),
        sharex=True,
    )

    # Ensure 'axes' is an array-like object even for a single subplot for consistent looping
    if len(tickers) == 1:
        axes = [axes]

    for i, ticker in enumerate(tickers):
        df = pd.read_sql(
            f"SELECT date, close FROM market_data WHERE ticker=? ORDER BY date DESC LIMIT {int(days)}",
            conn,
            params=(ticker,),
            parse_dates=["date"],
        )

        if not df.empty:
            df = df.sort_values("date")
            axes[i].plot(df["date"], df["close"], label=f"{ticker} Close Price", marker='o', markersize=3, linestyle='-')
            axes[i].set_title(f"{ticker} Last {days} Days Close Price")
            axes[i].grid(True, linestyle="--", alpha=0.6)
            axes[i].legend()
        else:
            axes[i].set_title(f"No data for {ticker}")
            logger.warning(f"No data to plot for {ticker} in price series.")

    plt.tight_layout()
    logger.info(f"Generated price series verification plots for {len(tickers)} tickers.")
    return fig

def plot_quality_heatmap(quality_df: pd.DataFrame):
    """
    Generates and returns a Matplotlib figure object displaying a heatmap of
    data quality check results. Each cell indicates pass (1) or fail (0) for
    various quality metrics across different tickers.

    Args:
        quality_df (pd.DataFrame): A DataFrame containing the results of quality checks.
                                   Must include 'ticker' and quality metric columns like
                                   'fresh', 'complete', 'valid_prices', 'no_extreme_moves'.

    Returns:
        matplotlib.figure.Figure or None: The generated Matplotlib Figure object,
                                         or None if the quality DataFrame is empty.
    """
    if quality_df.empty:
        logger.warning("Quality summary DataFrame is empty, cannot generate heatmap.")
        return None

    fig = plt.figure(figsize=(10, 8))

    check_cols = ["fresh", "complete", "valid_prices", "no_extreme_moves"]
    df_heatmap = quality_df[["ticker"] + check_cols].copy()
    df_heatmap[check_cols] = df_heatmap[check_cols].astype(int) # Convert boolean results to 0/1 for heatmap

    sns.heatmap(
        df_heatmap.set_index("ticker")[check_cols],
        annot=True,
        cmap="RdYlGn", # Red-Yellow-Green colormap, typically green for good (1)
        fmt="d",      # Format annotations as integers
        linewidths=0.5,
        linecolor="black",
        cbar=False,   # No need for a color bar for binary values
    )

    plt.title("Data Quality Check Heatmap (0=Fail, 1=Pass)")
    plt.ylabel("Ticker")
    plt.xlabel("Quality Metric")
    logger.info("Generated data quality heatmap.")
    return fig

def plot_completeness_over_time(quality_df: pd.DataFrame, ticker: str, config: dict):
    """
    Generates and returns a Matplotlib figure object showing the data completeness
    ratio for a specific ticker, compared against a target completeness threshold.

    Args:
        quality_df (pd.DataFrame): A DataFrame containing the results of quality checks.
        ticker (str): The ticker symbol for which to plot completeness.
        config (dict): A configuration dictionary, primarily used for `max_missing_pct`.

    Returns:
        matplotlib.figure.Figure or None: The generated Matplotlib Figure object,
                                         or None if the ticker data is not found in the quality summary.
    """
    row = quality_df.loc[quality_df["ticker"] == ticker]
    if row.empty:
        logger.warning(f"{ticker} not in quality summary for completeness plot.")
        return None

    current_completeness = float(row["completeness"].iloc[0])

    fig = plt.figure(figsize=(10, 5))
    plt.bar(["Current Run"], [current_completeness], color='skyblue')
    target_completeness = (1 - config["max_missing_pct"])
    plt.axhline(
        y=target_completeness,
        color='r',
        linestyle="--",
        label=f"Target Completeness (>{target_completeness * 100:.0f}%)",
    )
    plt.title(f"Data Completeness Ratio for {ticker} (Current Run)")
    plt.ylabel("Completeness Ratio")
    plt.ylim(0, 1.05)
    plt.legend()
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    logger.info(f"Generated completeness ratio plot for {ticker} (current run).")
    return fig

# --- ROI Calculation Functions ---

def calculate_roi(config: dict):
    """
    Calculates the Return on Investment (ROI) metrics for automating the data pipeline,
    including annual time savings and the reduction in estimated errors.

    Args:
        config (dict): A configuration dictionary containing parameters for ROI
                       calculation such as manual effort, setup costs, maintenance,
                       and error rates.

    Returns:
        tuple: A tuple containing:
            - time_saved_annual (float): Estimated annual hours saved by automation.
            - expected_manual_errors_daily (float): Estimated daily errors with manual process.
            - expected_automated_errors_daily (float): Estimated daily errors with automated process.
    """
    T_manual = config["T_manual"]
    D_annual = config["D_annual"]
    T_setup = config["T_setup"]
    T_maint = config["T_maint"]

    # Calculate annual time saved
    time_saved_annual = (T_manual * D_annual) - T_setup - T_maint

    # Calculate expected errors for manual vs. automated processes
    manual_error_rate = config["manual_error_rate"]
    automated_error_rate = config["automated_error_rate"]
    num_tickers = len(config["tickers"])
    avg_data_points_per_ticker = config["avg_data_points_per_ticker"]

    # Assuming 'avg_data_points_per_ticker' refers to the average number of daily records
    # per ticker that are subject to error.
    expected_manual_errors_daily = num_tickers * avg_data_points_per_ticker * manual_error_rate
    expected_automated_errors_daily = num_tickers * avg_data_points_per_ticker * automated_error_rate

    logger.info(f"ROI Calculation: Annual time saved: {time_saved_annual:.2f} hours.")
    logger.info(f"Expected daily errors (manual): {expected_manual_errors_daily:.2f}")
    logger.info(f"Expected daily errors (automated): {expected_automated_errors_daily:.4f}")

    return time_saved_annual, expected_manual_errors_daily, expected_automated_errors_daily

def plot_roi_comparison(annual_time_saved: float, T_manual: float, D_annual: int):
    """
    Generates and returns a Matplotlib figure object comparing the annual time investment
    for data ingestion between manual and automated processes.

    Args:
        annual_time_saved (float): The total annual hours saved due to automation.
        T_manual (float): Estimated manual hours spent per day on data handling.
        D_annual (int): Average number of annual trading days.

    Returns:
        matplotlib.figure.Figure: The generated Matplotlib Figure object.
    """
    manual_time_annual = T_manual * D_annual
    labels = ["Manual Process", "Automated Pipeline"]
    times = [manual_time_annual, manual_time_annual - annual_time_saved]

    fig = plt.figure(figsize=(8, 6))
    plt.bar(labels, times, color=['lightcoral', 'lightgreen'])
    plt.ylabel("Hours per Year")
    plt.title("Annual Time Investment: Manual vs. Automated Data Ingestion")

    # Add text labels for bar heights
    for i, v in enumerate(times):
        plt.text(i, v + 10, f"{v:.1f} hrs", ha="center", va="bottom")

    # Add text indicating total savings
    plt.text(
        0.5, # X-position (center of the plot in axes coordinates)
        0.9, # Y-position (top 90% of the plot in axes coordinates)
        f"Savings: {annual_time_saved:.1f} hrs/year",
        horizontalalignment="center",
        fontsize=12,
        bbox=dict(facecolor="white", alpha=0.7, edgecolor='none'), # Bounding box for readability
        transform=plt.gca().transAxes # Position relative to the axes
    )

    logger.info("Generated ROI comparison chart.")
    return fig

# --- Main Pipeline Orchestration ---

def run_pipeline(config: dict):
    """
    Executes the entire data pipeline, encompassing data extraction, transformation,
    loading into the database, and post-load quality checks. This function is
    designed to be the primary entry point for scheduled or manual pipeline runs.

    Args:
        config (dict): The configuration dictionary containing all necessary settings
                       for the pipeline run (e.g., tickers, dates, database path).

    Returns:
        pd.DataFrame: A DataFrame summarizing the results of all quality checks
                      performed during this pipeline run. Returns an empty DataFrame
                      if the pipeline fails before quality checks can be performed.
    """
    logger.info("=" * 60)
    logger.info("PIPELINE START")
    start_time_pipeline = time.time()

    qc_summary_df = pd.DataFrame() # Initialize empty DataFrame to be returned in case of failure

    try:
        # Update end_date to current date for each run to ensure fresh data
        current_end_date = datetime.now().strftime("%Y-%m-%d")
        config["end_date"] = current_end_date # Update the config for the current run

        # 1. Data Extraction
        market_data, failed_tickers = fetch_market_data(
            config["tickers"],
            config["start_date"],
            config["end_date"],
            config["interval"],
        )

        # 2. Initialize/Connect to Database for ETL
        conn_etl = init_database(config["db_path"])

        quality_reports_for_run = []
        for ticker, df in market_data.items():
            if not df.empty:
                # 3. Data Transformation (Cleaning)
                clean_df, report = clean_market_data(df.copy(), ticker, config)
                
                # 4. Data Loading (Upsert into DB)
                if not clean_df.empty: # Only upsert if cleaning resulted in data
                    upsert_data(conn_etl, clean_df)
                else:
                    logger.warning(f"Cleaned DataFrame for {ticker} is empty. Skipping upsert.")
                quality_reports_for_run.append(report)
            else:
                logger.warning(f"No data for {ticker} was fetched, skipping cleaning and upsert.")

        conn_etl.close() # Close ETL connection

        # 5. Post-load Quality Checks
        conn_qc_final = sqlite3.connect(config["db_path"])
        qc_summary_df = run_quality_checks(conn_qc_final, config["tickers"], config)
        conn_qc_final.close() # Close QC connection

        num_passed_qc = int(qc_summary_df["PASS"].sum())
        num_total_qc = len(qc_summary_df)

        elapsed_time_pipeline = time.time() - start_time_pipeline

        logger.info(
            f"PIPELINE COMPLETE in {elapsed_time_pipeline:.1f}s | "
            f"{num_total_qc} tickers processed | "
            f"{num_passed_qc} passed QC"
        )

        if num_passed_qc < num_total_qc:
            logger.error(
                f"Some quality checks failed ({num_total_qc - num_passed_qc}/{num_total_qc} failed). "
                "Review the quality report for details."
            )
        else:
            logger.info("All quality checks passed successfully.")

    except Exception as e:
        logger.critical(f"PIPELINE FAILED: {e}", exc_info=True)
        # Re-raise the exception to allow caller (e.g., a scheduler) to handle it
        raise

    return qc_summary_df


# --- Database Integrity Verification (Utility Function) ---

def verify_db_integrity(db_path: str):
    """
    Performs a series of basic integrity checks on the database to ensure
    that the 'market_data' table exists, contains data, has no future dates,
    all prices are positive, and no duplicate (ticker, date) pairs exist.
    This function acts as a health check for the database.

    Args:
        db_path (str): The file path to the SQLite database.

    Returns:
        bool: True if all integrity checks pass, False otherwise.
    """
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Check 1: Table 'market_data' exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='market_data'")
        table_exists = cursor.fetchone() is not None
        assert table_exists, "Database table 'market_data' does not exist."
        logger.info("Integrity Check: Table 'market_data' exists.")

        # Check 2: Database contains data for at least one ticker
        df_tickers = pd.read_sql("SELECT DISTINCT ticker FROM market_data", conn)
        assert len(df_tickers) >= 1, "Database is empty or has less than 1 ticker."
        logger.info(f"Integrity Check: Database contains data for {len(df_tickers)} distinct tickers.")

        # Check 3: No records with future dates
        future_dates_df = pd.read_sql("SELECT * FROM market_data WHERE date > date('now')", conn)
        assert future_dates_df.empty, f"Found {len(future_dates_df)} records with future dates."
        logger.info("Integrity Check: No future dates found.")

        # Check 4: All closing prices are positive
        non_positive_prices_df = pd.read_sql("SELECT * FROM market_data WHERE close <= 0", conn)
        assert non_positive_prices_df.empty, (
            f"Found {len(non_positive_prices_df)} records with non-positive closing prices."
        )
        logger.info("Integrity Check: All closing prices are positive.")

        # Check 5: No duplicate (ticker, date) pairs
        duplicate_pairs_df = pd.read_sql(
            """
            SELECT ticker, date, COUNT(*) as cnt
            FROM market_data
            GROUP BY ticker, date
            HAVING cnt > 1
            """,
            conn,
        )
        assert duplicate_pairs_df.empty, (
            f"Found {len(duplicate_pairs_df)} duplicate (ticker, date) records."
        )
        logger.info("Integrity Check: No duplicate (ticker, date) pairs found.")

        logger.info("All database integrity checks PASSED successfully.")
        return True

    except AssertionError as ae:
        logger.error(f"Database Integrity Check FAILED: {ae}")
        return False

    except Exception as e:
        logger.error(f"An unexpected error occurred during database integrity check: {e}", exc_info=True)
        return False

    finally:
        if conn:
            conn.close()

# --- Functions to orchestrate visualizations and ROI ---

def generate_all_visualizations(quality_summary_df: pd.DataFrame, config: dict):
    """
    Orchestrates the generation of all defined visualization plots. In an actual
    application (e.g., a web app), these figures would typically be saved to files,
    streamed, or converted to a format suitable for front-end rendering, rather
    than calling `plt.show()`. For demonstration, `plt.show()` is used here.

    Args:
        quality_summary_df (pd.DataFrame): The DataFrame containing quality check results,
                                           used as input for quality-related plots.
        config (dict): The configuration dictionary, used for database path and plot settings.
    """
    logger.info("\n--- Generating Visualizations ---")
    
    # Establish a database connection for visualization functions
    conn_viz = sqlite3.connect(config["db_path"])
    try:
        # Plot price series for a few selected tickers (e.g., top 3 from config)
        selected_tickers_for_plot = config["tickers"][:min(3, len(config["tickers"]))]
        if selected_tickers_for_plot:
            fig_prices = plot_price_series(conn_viz, selected_tickers_for_plot, days=30)
            if fig_prices:
                plt.show(fig_prices)
                plt.close(fig_prices) # Close figure to free memory
        else:
            logger.warning("No tickers configured for price series plot. Skipping.")

        # Plot quality heatmap
        if not quality_summary_df.empty:
            fig_heatmap = plot_quality_heatmap(quality_summary_df)
            if fig_heatmap:
                plt.show(fig_heatmap)
                plt.close(fig_heatmap)
        else:
            logger.warning("Quality summary DataFrame is empty, skipping heatmap visualization.")

        # Plot completeness over time for a specific ticker (e.g., "AAPL")
        target_ticker_for_completeness = "AAPL"
        # Ensure the target ticker is in the configuration and quality summary is not empty
        if target_ticker_for_completeness in config["tickers"] and not quality_summary_df.empty:
            fig_completeness = plot_completeness_over_time(quality_summary_df, target_ticker_for_completeness, config)
            if fig_completeness:
                plt.show(fig_completeness)
                plt.close(fig_completeness)
        else:
            logger.warning(
                f"Ticker '{target_ticker_for_completeness}' not found in configured tickers "
                "or quality summary is empty, skipping completeness plot."
            )

    finally:
        if conn_viz:
            conn_viz.close()
    logger.info("Visualizations generation complete.")

def run_roi_analysis(config: dict):
    """
    Executes the Return on Investment (ROI) analysis, calculates the relevant
    metrics, and generates a comparison plot.

    Args:
        config (dict): The configuration dictionary containing ROI-related parameters.
    """
    logger.info("\n--- Running ROI Analysis ---")
    
    annual_time_saved, manual_errors, automated_errors = calculate_roi(config)

    print(f"\nAnnual Time Saved by Automation: {annual_time_saved:.2f} hours")
    print(f"Daily Errors (Manual vs. Automated): {manual_errors:.2f} vs. {automated_errors:.4f}")

    fig_roi = plot_roi_comparison(annual_time_saved, config["T_manual"], config["D_annual"])
    if fig_roi:
        plt.show(fig_roi)
        plt.close(fig_roi)
    logger.info("ROI analysis complete.")

# --- Scheduling Setup (for app.py integration) ---
# These functions are provided for integration into an `app.py` or similar
# scheduling mechanism. The 'schedule' library is simple but blocking;
# for production, consider APScheduler or cron jobs for more robust scheduling.

def setup_scheduler(config: dict):
    """
    Configures the 'schedule' library to run the data pipeline daily at a
    specified time.

    Args:
        config (dict): The configuration dictionary containing 'run_time'.
    """
    schedule_time = config["run_time"] # e.g., "16:30"
    logger.info(f"Scheduling pipeline to run daily at {schedule_time} (local time).")
    schedule.every().day.at(schedule_time).do(run_pipeline_scheduled, config=config)

def run_pipeline_scheduled(config: dict):
    """
    A wrapper function for `run_pipeline` that is designed to be called by the
    scheduler. It includes robust error handling to prevent the scheduler
    from crashing due to pipeline failures.

    Args:
        config (dict): The configuration dictionary for the pipeline.
    """
    try:
        logger.info(f"Scheduled pipeline run triggered at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        run_pipeline(config)
        logger.info("Scheduled pipeline run completed successfully.")
    except Exception as e:
        logger.error(f"Scheduled pipeline run FAILED: {e}", exc_info=True)

def start_scheduler():
    """
    Starts the scheduler loop. This is a blocking call and will continuously
    check for pending scheduled tasks. It should typically be run in a
    separate thread or process in a production application.
    """
    logger.info("Scheduler started. Waiting for next scheduled run...")
    while True:
        schedule.run_pending()
        time.sleep(1) # Check every second for pending tasks


# --- Main Execution Block (runs only when the script is executed directly) ---
if __name__ == "__main__":
    logger.info("--- Starting main execution block (direct script run) ---")
    print("This script is running directly for demonstration purposes.")

    # 1. Initial database setup (ensures table exists)
    # This is safe to run as init_database is idempotent.
    try:
        conn_initial_setup = init_database(CONFIG["db_path"])
        conn_initial_setup.close()
        logger.info("Initial database setup confirmed/completed.")
        print(f"Database initialized at {CONFIG['db_path']}.")
    except Exception as e:
        logger.critical(f"Initial database setup failed critically: {e}", exc_info=True)
        print(f"ERROR: Initial database setup failed. Check logs for details: {e}")
        exit(1) # Exit if critical setup fails

    # 2. Demonstrate a full pipeline run
    print("\n--- Running pipeline manually for demonstration ---")
    final_quality_summary_df = pd.DataFrame() # Initialize in case pipeline fails early
    try:
        final_quality_summary_df = run_pipeline(CONFIG)
        print("Pipeline run completed. Check pipeline.log for detailed output.")
        logger.info("Manual pipeline run completed.")

        # Display quality summary after the run
        logger.info("\n--- Quality Check Summary Report after pipeline run ---")
        print(final_quality_summary_df.to_string())

        # 3. Generate and display all visualizations
        generate_all_visualizations(final_quality_summary_df, CONFIG)

        # 4. Run and display ROI analysis
        run_roi_analysis(CONFIG)

    except Exception as e:
        print(f"ERROR: Manual pipeline execution failed: {e}")
        logger.error(f"Manual pipeline execution failed: {e}")

    # 5. Verify database integrity after the pipeline run
    print("\n--- Running database integrity verification ---")
    db_ok = verify_db_integrity(CONFIG["db_path"])

    if db_ok:
        print("Database integrity confirmed. Data is ready for downstream use.")
        logger.info("Database integrity confirmed.")
    else:
        print("Database integrity issues detected. Please review logs and data.")
        logger.error("Database integrity issues detected.")

    logger.info("--- Main execution block finished ---")
    print("\nDemonstration complete. Check pipeline.log for full details.")

    # To run the scheduler, you would typically uncomment these lines in a separate
    # application entry point (e.g., app.py or a dedicated scheduler script):
    # print(f"\nSetting up scheduler to run daily at {CONFIG['run_time']}...")
    # setup_scheduler(CONFIG)
    # print("Scheduler is now configured. To start it, call start_scheduler().")
    # print("Note: start_scheduler() is a blocking call, typically run in a production system.")
    # # start_scheduler()
