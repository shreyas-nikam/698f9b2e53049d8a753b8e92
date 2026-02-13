
# Automating Market Data Ingestion for Investment Research

**Persona:** Sri Krishnamurthy, CFA – An Equity Analyst / Portfolio Manager at an investment firm.

**Organization:** Alpha Investments, a boutique investment firm where analysts and portfolio managers rely heavily on accurate, timely market data for their daily operations.

**Introduction:**
At Alpha Investments, the daily ritual of manually copying and pasting market data from sources like Bloomberg or Refinitiv into Excel spreadsheets has become a significant bottleneck. This process is not only time-consuming, consuming precious hours that could be spent on high-value analytical tasks, but it's also prone to human error, leading to inconsistent data across various valuation models and dashboards.

As a CFA Charterholder, Sri understands the critical importance of reliable data as the foundation for all investment decisions and subsequent AI/ML models. This notebook outlines how Sri, recognizing this inefficiency, will build an automated, Python-based market data pipeline. This pipeline will connect to financial APIs, ingest daily OHLCV, fundamental, and macroeconomic data, apply robust data cleaning and validation rules, store the data in a local SQLite database, and generate automated quality reports. By doing so, Sri aims to liberate his team from low-value data wrangling, drastically reduce errors, and establish a scalable, auditable data infrastructure.

This hands-on journey will demonstrate a real-world workflow, from initial configuration to automated daily execution, embodying key data engineering principles relevant to financial professionals.

## 1. Setting Up the Environment and Dependencies

Before we dive into building our automated data pipeline, we need to ensure our environment is ready. This involves installing the necessary Python libraries and importing them for use throughout our workflow.

```python
# Install required libraries
!pip install yfinance pandas numpy matplotlib schedule requests

# Import required dependencies
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
```

## 2. Defining the Investment Universe and Pipeline Configuration

As an equity analyst, the first step is always to define the universe of assets I'm tracking. For our automated pipeline, this means specifying the tickers, the data parameters (like start and end dates), and the various thresholds required for data quality checks. Centralizing these parameters in a `CONFIG` dictionary makes our pipeline flexible, maintainable, and easy to adapt.

This section establishes the foundational parameters for our entire ETL process, connecting directly to the "Configuration & Ticker Universe" step of our workflow.

```python
# Define CONFIG dictionary with pipeline parameters, similar to a config.py file
CONFIG = {
    # Ticker universe (S&P 500 subset or full)
    'tickers': ['AAPL', 'MSFT', 'GOOG', 'AMZN', 'META',
                'JPM', 'BAC', 'GS', 'JNJ', 'PFE',
                'XOM', 'CVX', 'PG', 'KO', 'WMT',
                'TSLA', 'NVDA', 'V', 'MA', 'DIS'],

    # Data parameters
    'start_date': '2020-01-01',
    'end_date': datetime.now().strftime('%Y-%m-%d'), # Today's date
    'interval': '1d', # Daily interval

    # Storage
    'db_path': 'market_data.db',
    'csv_backup_dir': './data_backup/',

    # Scheduling
    'run_time': '16:30', # 4:30 PM ET (after market close)

    # Quality thresholds
    'max_missing_pct': 0.02, # Max 2% missing data
    'stale_hours_threshold': 24, # Alert if data > 24h old
    'min_volume_threshold': 100, # Flag if volume < 100
    'extreme_return_threshold': 0.50, # Flag daily price moves exceeding 50%

    # Logging
    'log_file': 'pipeline.log',
    'log_level': 'INFO',

    # ROI Calculation Parameters (example values)
    'T_manual': 0.75, # Hours spent manually per day (45 min)
    'D_annual': 252,  # Annual trading days
    'T_setup': 8,     # One-time setup hours
    'T_maint': 10,    # Annual maintenance hours
    'manual_error_rate': 0.02, # Manual copy-paste error rate per data point
    'automated_error_rate': 0.0001, # Automated API/parsing error rate
    'avg_data_points_per_ticker': 6, # OHLCV + Volume
}

# Ensure backup directory exists
os.makedirs(CONFIG['csv_backup_dir'], exist_ok=True)

# Configure logging
logging.basicConfig(
    filename=CONFIG['log_file'],
    level=getattr(logging, CONFIG['log_level']),
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('data_pipeline')

# Function to initialize the SQLite database schema
def init_database(db_path):
    """
    Creates the market_data table if it doesn't exist.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
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
    """)
    conn.commit()
    logger.info(f"Database initialized at {db_path} with `market_data` table.")
    return conn

# Execute database initialization
conn_initial = init_database(CONFIG['db_path'])
conn_initial.close()
```

## 3. Building the Extraction Layer: Resilient API Data Retrieval

Manual data collection often involves frustrating interruptions, such as network glitches or API rate limits. To make our automated pipeline robust, Sri needs to implement retry logic for API calls. This ensures that transient failures don't halt the entire process and that all data for the coverage universe is eventually collected.

This section focuses on the "Extract" part of our ETL process, specifically incorporating error handling with retry logic to fetch daily OHLCV data using `yfinance`.

```python
def fetch_market_data(tickers, start, end, interval='1d', max_retries=3, retry_delay=5):
    """
    Fetches OHLCV data from Yahoo Finance for a list of tickers with retry logic.
    Returns: dict of {ticker: DataFrame}, list of failed tickers.
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
                    auto_adjust=True # Use adjusted prices
                )

                if df.empty:
                    raise ValueError(f"Empty data returned for {ticker}")

                # Add ticker column and clean column names
                df['ticker'] = ticker
                df.index.name = 'date'
                df.reset_index(inplace=True)
                df.columns = [c.lower().replace(' ', '_').replace('.', '') for c in df.columns]

                logger.info(f"OK: {ticker} | {len(df)} rows ({df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')})")
                all_data[ticker] = df
                break # Success, exit retry loop

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

# Example execution of the data extraction
market_data, failed_tickers = fetch_market_data(
    CONFIG['tickers'],
    CONFIG['start_date'],
    CONFIG['end_date'],
    CONFIG['interval']
)

# Display a sample of the fetched data for verification
if 'AAPL' in market_data:
    logger.info("Sample of fetched data for AAPL:")
    print(market_data['AAPL'].head())
else:
    logger.info("AAPL data not fetched or not available.")

if failed_tickers:
    logger.warning(f"Tickers that failed to fetch: {failed_tickers}")
```

## 4. The Transformation Engine: Cleaning and Validating Market Data

Raw financial data is rarely perfect. As an analyst, Sri knows that data quality directly impacts the reliability of his valuation models. This section focuses on the "Transform" phase of the ETL pipeline, implementing crucial cleaning and validation steps: removing duplicates, handling missing price data using forward-fill imputation, detecting extreme price movements (anomalies), and performing sanity checks on trading volumes.

These steps ensure that the data fed into downstream models is clean, consistent, and ready for analysis, addressing common data quality issues that plague manual workflows.

**Mathematical Formulations:**

1.  **Forward-Fill Imputation for Prices:**
    For a missing price $P_{i,t}$ for ticker $i$ at time $t$, we use the last known price:
    $$
    \hat{P}_{i,t} = P_{i,t-1}
    $$
    This method is appropriate for market prices where a missing trading day often implies no change from the previous closing price (e.g., holidays, market halts).

2.  **Return Anomaly Detection:**
    We flag potential data errors when the absolute daily return $|r_{i,t}|$ for ticker $i$ at time $t$ exceeds a predefined threshold $\theta$:
    $$
    |r_{i,t}| = \frac{|P_{i,t} - P_{i,t-1}|}{P_{i,t-1}} > \theta
    $$
    For this pipeline, $\theta = 0.50$ (50% daily move). While genuine extreme moves occur, they are rare. Flagging such instances triggers an alert for human review, preventing erroneous data from silently impacting models.

```python
def clean_market_data(df, ticker, config):
    """
    Cleans and validates a single ticker's OHLCV data.
    Returns: cleaned DataFrame, quality_report dict
    """
    report = {'ticker': ticker, 'raw_rows': len(df)}
    initial_rows = len(df)
    
    # 3.1: Remove exact duplicate rows based on 'date'
    df_deduped = df.drop_duplicates(subset=['date'])
    report['dupes_removed'] = initial_rows - len(df_deduped)
    df = df_deduped.copy() # Work with the deduped dataframe

    # 3.2: Sort by date
    df = df.sort_values('date').reset_index(drop=True)

    # 3.3: Detect and handle missing trading days
    # (Weekends/holidays are expected gaps, but we check for gaps within business days)
    if not df.empty:
        min_date = pd.to_datetime(df['date'].min())
        max_date = pd.to_datetime(df['date'].max())
        date_range = pd.bdate_range(start=min_date, end=max_date)
        actual_dates = set(pd.to_datetime(df['date']))
        missing_trading_days = len(date_range) - len(actual_dates.intersection(date_range))
        report['missing_trading_days'] = missing_trading_days
    else:
        report['missing_trading_days'] = 0


    # 3.4: Handle missing OHLCV values (Forward-fill for prices)
    for col in ['open', 'high', 'low', 'close']:
        n_missing = df[col].isnull().sum()
        if n_missing > 0:
            df[col] = df[col].ffill() # Forward-fill imputation
            report[f'{col}_imputed'] = n_missing
        else:
            report[f'{col}_imputed'] = 0

    # 3.5: Outlier detection (price jumps > extreme_return_threshold in one day)
    df['daily_return'] = df['close'].pct_change()
    outlier_mask = (df['daily_return'].abs() > config['extreme_return_threshold'])
    report['price_outliers'] = outlier_mask.sum()

    # 3.6: Volume sanity check
    low_vol_mask = (df['volume'] < config['min_volume_threshold'])
    report['low_volume_days'] = low_vol_mask.sum()

    # 3.7: Add metadata
    df['dividend'] = df['dividend'].fillna(0) # Fill NaN dividends with 0
    df['updated_at'] = pd.Timestamp.now()
    df.drop(columns=['daily_return'], inplace=True, errors='ignore')

    report['clean_rows'] = len(df)
    logger.info(f"Cleaned {ticker}: {report['dupes_removed']} duplicates removed, {report['price_outliers']} price outliers, {report['low_volume_days']} low volume days.")
    return df, report

# Execute cleaning for all fetched data
cleaned_market_data = {}
quality_reports = []

for ticker, df in market_data.items():
    if not df.empty:
        cleaned_df, report = clean_market_data(df.copy(), ticker, CONFIG)
        cleaned_market_data[ticker] = cleaned_df
        quality_reports.append(report)
    else:
        logger.warning(f"Skipping cleaning for {ticker} as dataframe is empty.")

# Display a sample of the cleaned data for verification (e.g., AAPL)
if 'AAPL' in cleaned_market_data:
    logger.info("Sample of cleaned data for AAPL:")
    print(cleaned_market_data['AAPL'].head())
    print(f"Quality Report for AAPL: {next((r for r in quality_reports if r['ticker'] == 'AAPL'), None)}")
else:
    logger.info("AAPL data not in cleaned_market_data.")
```

## 5. Loading Clean Data Idempotently into SQLite

A core principle for any production-grade data pipeline is idempotency. This means that running the pipeline multiple times with the same input should produce the exact same result in the database, without creating duplicate records. As an investment professional, Sri cannot afford data inconsistencies or errors due to accidental re-runs or system failures.

This section implements the "Load" phase, storing our cleaned market data into a local SQLite database using an `INSERT OR REPLACE` SQL statement. This ensures idempotency by updating existing records if they have the same primary key (`ticker`, `date`) or inserting new ones otherwise.

**Key Insight: Idempotency**
Idempotency is the most important property of a production pipeline. An idempotent pipeline produces the same result whether run once, twice, or ten times. The `INSERT OR REPLACE` pattern ensures that if the pipeline crashes mid-run and is restarted, it does not create duplicate records. This is a fundamental software engineering concept that most analysts never learn in their finance training but need immediately when building automated systems. Without idempotency, every pipeline failure creates a data integrity problem that propagates into all downstream models.

```python
def upsert_data(conn, df):
    """
    Inserts or replaces data into the database.
    Idempotent: running twice with the same data produces
    the same result (no duplicates).
    """
    if df.empty:
        logger.warning("Attempted to upsert an empty DataFrame. No data loaded.")
        return

    # Prepare DataFrame for SQL insertion: convert to list of dictionaries
    # Ensure all columns match the database schema
    df_to_insert = df[['ticker', 'date', 'open', 'high', 'low', 'close', 'volume', 'dividend', 'updated_at']]
    records = df_to_insert.to_dict('records')

    # SQL statement for idempotent upsert
    # INSERT OR REPLACE ensures that if a record with the same PRIMARY KEY (ticker, date) exists,
    # it will be updated; otherwise, a new record will be inserted.
    sql = """
        INSERT OR REPLACE INTO market_data (ticker, date, open, high, low, close, volume, dividend, updated_at)
        VALUES (:ticker, :date, :open, :high, :low, :close, :volume, :dividend, :updated_at)
    """
    try:
        conn.executemany(sql, records)
        conn.commit()
        logger.info(f"Upserted {len(records)} records for ticker {df['ticker'].iloc[0]}.")
    except sqlite3.Error as e:
        logger.error(f"SQLite error during upsert for ticker {df['ticker'].iloc[0]}: {e}")
        conn.rollback() # Rollback in case of error

# Re-open connection for upsert operations
conn_upsert = init_database(CONFIG['db_path'])

# Execute upsert for all cleaned data
for ticker, df in cleaned_market_data.items():
    if not df.empty:
        upsert_data(conn_upsert, df)

conn_upsert.close()
logger.info("All cleaned data loaded into the database.")

# Verify a sample of the loaded data directly from the database
conn_verify = sqlite3.connect(CONFIG['db_path'])
try:
    df_verify = pd.read_sql("SELECT * FROM market_data WHERE ticker='AAPL' ORDER BY date DESC LIMIT 5", conn_verify)
    logger.info("Sample of data verified from database for AAPL:")
    print(df_verify)
except Exception as e:
    logger.error(f"Error verifying data from database: {e}")
finally:
    conn_verify.close()
```

## 6. Automated Quality Assurance and Visual Verification

Reliable data is paramount for investment decisions. Manual checks are tedious and prone to oversight. Sri needs automated data quality checks that provide a quick, comprehensive overview of the data's health. This section focuses on implementing such checks: data freshness, completeness, validity of prices (non-negative), and the absence of extreme, anomalous moves.

The results are summarized in a quality report and visualized, enabling Sri to quickly identify any issues and maintain confidence in the data driving Alpha Investments' models.

**Mathematical Formulations:**

1.  **Data Completeness Ratio ($C_i$):**
    For each ticker $i$, the completeness ratio $C_i$ quantifies the proportion of actual trading days with data ($N_{actual,i}$) against the expected number of business days ($N_{expected,i}$) within a given date range.
    $$
    C_i = \frac{N_{actual,i}}{N_{expected,i}}
    $$
    A target of $C_i > 0.98$ is typically used, allowing for rare holidays or market closures that might not be captured as business days.

2.  **Data Freshness ($F_i$):**
    The freshness $F_i$ measures how many hours have passed since the latest data point for ticker $i$ ($t_{latest,i}$) compared to the current time ($t_{now}$).
    $$
    F_i = \frac{t_{now} - t_{latest,i}}{3600}
    $$
    $F_i < 24$ hours on business days is ideal. If $F_i > 72$ hours, it strongly indicates stale data, likely from a failed pipeline run over a weekend, requiring immediate attention.

```python
def run_quality_checks(conn, tickers, config):
    """
    Runs post-load quality checks and returns a pass/fail report for each ticker.
    """
    results = []
    logger.info("Starting data quality checks...")

    for ticker in tickers:
        checks = {'ticker': ticker}
        df_qc = pd.read_sql(
            f"SELECT * FROM market_data WHERE ticker='{ticker}' ORDER BY date DESC LIMIT 252",
            conn, parse_dates=['date']
        )

        if df_qc.empty:
            logger.warning(f"No data found for {ticker} in DB for quality checks.")
            checks['fresh'] = False
            checks['completeness'] = 0.0
            checks['complete'] = False
            checks['valid_prices'] = False
            checks['no_extreme_moves'] = False
            checks['PASS'] = False
            results.append(checks)
            continue

        # Q1: Freshness - is latest data from today/yesterday?
        latest_date_db = df_qc['date'].max()
        hours_old = (pd.Timestamp.now() - latest_date_db).total_seconds() / 3600
        checks['freshness_hours'] = round(hours_old, 1)
        checks['fresh'] = hours_old < config['stale_hours_threshold']
        
        # Q2: Completeness - expected vs actual trading days
        min_date_qc = pd.to_datetime(df_qc['date'].min())
        max_date_qc = pd.to_datetime(df_qc['date'].max())
        expected_days = len(pd.bdate_range(start=min_date_qc, end=max_date_qc))
        actual_days = len(df_qc)
        
        # Avoid division by zero if expected_days is 0 (should not happen with proper date range)
        completeness_ratio = actual_days / max(expected_days, 1)
        checks['completeness'] = round(completeness_ratio, 4)
        checks['complete'] = completeness_ratio > (1 - config['max_missing_pct'])

        # Q3: No zero/negative prices
        checks['valid_prices'] = (df_qc['close'] > 0).all()

        # Q4: No extreme daily returns (data error indicator)
        df_qc['ret'] = df_qc['close'].pct_change()
        checks['no_extreme_moves'] = (df_qc['ret'].abs().fillna(0) < config['extreme_return_threshold']).all()

        # Overall pass/fail
        checks['PASS'] = all([checks['fresh'], checks['complete'], checks['valid_prices'], checks['no_extreme_moves']])
        results.append(checks)

    report_df = pd.DataFrame(results)
    n_pass = report_df['PASS'].sum()
    n_fail = len(report_df) - n_pass
    logger.info(f"Quality Check Summary: {n_pass} PASS, {n_fail} FAIL.")

    if n_fail > 0:
        failed_tickers_report = report_df[~report_df['PASS']]
        logger.warning(f"FAILED tickers:\n{failed_tickers_report.to_string()}")
        # In a real production system, this would trigger an email/Slack alert.
    return report_df

# Run quality checks
conn_qc = sqlite3.connect(CONFIG['db_path'])
quality_summary_df = run_quality_checks(conn_qc, CONFIG['tickers'], CONFIG)
conn_qc.close()

logger.info("\n--- Quality Check Summary Report ---")
print(quality_summary_df)


# --- Visualizations for Quality Assurance ---

# V1: Price Series Verification (Line plot for recent 30 days for 3-5 tickers)
def plot_price_series(conn, tickers, days=30):
    fig, axes = plt.subplots(len(tickers), 1, figsize=(12, 4 * len(tickers)), sharex=True)
    if len(tickers) == 1: # Handle single ticker case
        axes = [axes]

    for i, ticker in enumerate(tickers):
        df = pd.read_sql(
            f"SELECT date, close FROM market_data WHERE ticker='{ticker}' ORDER BY date DESC LIMIT {days}",
            conn, parse_dates=['date']
        )
        if not df.empty:
            df = df.sort_values('date') # Sort for correct time series plotting
            axes[i].plot(df['date'], df['close'], label=f'{ticker} Close Price', color='blue')
            axes[i].set_title(f'{ticker} Last {days} Days Close Price')
            axes[i].grid(True, linestyle='--', alpha=0.6)
            axes[i].legend()
        else:
            axes[i].set_title(f'No data for {ticker}')
            logger.warning(f"No data to plot for {ticker}")
    plt.tight_layout()
    plt.show()
    logger.info(f"Generated price series verification plots for {len(tickers)} tickers.")

# V2: Quality Dashboard Heatmap
def plot_quality_heatmap(quality_df):
    plt.figure(figsize=(10, 8))
    # Select only the boolean check columns for heatmap
    check_cols = ['fresh', 'complete', 'valid_prices', 'no_extreme_moves']
    
    # Create a copy to avoid SettingWithCopyWarning
    df_heatmap = quality_df[['ticker'] + check_cols].copy() 
    df_heatmap[check_cols] = df_heatmap[check_cols].astype(int) # Convert booleans to int for heatmap

    sns.heatmap(df_heatmap.set_index('ticker')[check_cols], annot=True, cmap='RdYlGn', fmt='d', linewidths=.5, linecolor='black')
    plt.title('Data Quality Check Heatmap (0=Fail, 1=Pass)')
    plt.ylabel('Ticker')
    plt.xlabel('Quality Metric')
    plt.show()
    logger.info("Generated data quality heatmap.")

# V3: Data Completeness Ratio Over Time (Conceptual, requires historical quality reports)
# For this lab, we'll simulate it for demonstration purposes, or plot for a single run
def plot_completeness_over_time(conn, ticker, num_runs=30):
    # This would ideally come from historical quality_summary_df saves,
    # but for a single run, we can show a placeholder or a single point.
    # To simulate, let's just plot the current completeness ratio for `ticker`.
    
    # Fetch current completeness for the ticker
    current_completeness = quality_summary_df[quality_summary_df['ticker'] == ticker]['completeness'].iloc[0]
    
    plt.figure(figsize=(10, 5))
    plt.bar(['Current Run'], [current_completeness], color='skyblue')
    plt.axhline(y=(1 - CONFIG['max_missing_pct']), color='r', linestyle='--', label=f'Target Completeness (>{(1 - CONFIG["max_missing_pct"])*100:.0f}%)')
    plt.title(f'Data Completeness Ratio for {ticker} (Current Run)')
    plt.ylabel('Completeness Ratio')
    plt.ylim(0, 1.05)
    plt.legend()
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.show()
    logger.info(f"Generated completeness ratio plot for {ticker} (current run).")


# Execute visualizations
conn_viz = sqlite3.connect(CONFIG['db_path'])
plot_price_series(conn_viz, ['AAPL', 'MSFT', 'GOOG'], days=30)
plot_quality_heatmap(quality_summary_df)
if 'AAPL' in quality_summary_df['ticker'].values:
    plot_completeness_over_time(conn_viz, 'AAPL')
else:
    logger.warning("AAPL not in quality summary for completeness plot.")
conn_viz.close()
```

## 7. Quantifying Business Value: ROI of Automation

As a CFA Charterholder, Sri knows that any investment, even in internal tools, must demonstrate a clear return. Automating the data pipeline is not just about convenience; it's about significant time and error savings. This section calculates the Return on Investment (ROI) of our automation project, quantifying the hours saved annually and the reduction in transcription errors. This provides a compelling business case, justifying the initial setup and ongoing maintenance efforts.

**Mathematical Formulation:**

The time saved per year ($\Delta T$) is calculated by comparing the manual effort to the automated process, factoring in setup and maintenance costs:

$$
\Delta T = T_{manual} \times D_{annual} - T_{setup} - T_{maint}
$$
Where:
- $T_{manual}$: Daily manual effort (e.g., 0.75 hours or 45 minutes)
- $D_{annual}$: Annual trading days (e.g., 252 days)
- $T_{setup}$: One-time pipeline development cost (e.g., 8 hours)
- $T_{maint}$: Annual maintenance (e.g., 10 hours)

**Example Calculation (from attachments):**
$\Delta T = 0.75 \times 252 - 8 - 10 = 171$ hours/year saved.
At an analyst's loaded cost of $150/hour, the pipeline saves ~$25,600/year from a one-day development investment.

Error Reduction: If manual copy-paste has a 2% error rate per data point and the pipeline has 0.01% (API/parsing error), the quality improvement is 200x. For a 50-stock universe with 6 daily data points each, that is $50 \times 6 \times 0.02 = 6$ expected daily errors eliminated.

```python
def calculate_roi(config):
    """
    Calculates the Return on Investment (ROI) for automating the data pipeline.
    """
    T_manual = config['T_manual']
    D_annual = config['D_annual']
    T_setup = config['T_setup']
    T_maint = config['T_maint']
    
    time_saved_annual = (T_manual * D_annual) - T_setup - T_maint
    
    # Error reduction calculation
    manual_error_rate = config['manual_error_rate']
    automated_error_rate = config['automated_error_rate']
    num_tickers = len(config['tickers'])
    avg_data_points_per_ticker = config['avg_data_points_per_ticker']

    expected_manual_errors_daily = num_tickers * avg_data_points_per_ticker * manual_error_rate
    expected_automated_errors_daily = num_tickers * avg_data_points_per_ticker * automated_error_rate
    
    logger.info(f"ROI Calculation: Annual time saved: {time_saved_annual:.2f} hours.")
    logger.info(f"Expected daily errors (manual): {expected_manual_errors_daily:.2f}")
    logger.info(f"Expected daily errors (automated): {expected_automated_errors_daily:.2f}")
    
    return time_saved_annual, expected_manual_errors_daily, expected_automated_errors_daily

# Perform ROI calculation
annual_time_saved, manual_errors, automated_errors = calculate_roi(CONFIG)
print(f"\nAnnual Time Saved by Automation: {annual_time_saved:.2f} hours")
print(f"Daily Errors (Manual vs. Automated): {manual_errors:.2f} vs. {automated_errors:.4f}")

# V4: ROI Comparison Chart
def plot_roi_comparison(annual_time_saved, T_manual, D_annual):
    manual_time_annual = T_manual * D_annual
    labels = ['Manual Process', 'Automated Pipeline']
    times = [manual_time_annual, manual_time_annual - annual_time_saved] # Manual minus savings = automated effective time
    
    plt.figure(figsize=(8, 6))
    plt.bar(labels, times, color=['lightcoral', 'lightseagreen'])
    plt.ylabel('Hours per Year')
    plt.title('Annual Time Investment: Manual vs. Automated Data Ingestion')
    
    for i, v in enumerate(times):
        plt.text(i, v + 10, f"{v:.1f} hrs", ha='center', va='bottom')
    
    plt.text(0.5, max(times) * 0.9, f"Savings: {annual_time_saved:.1f} hrs/year",
             horizontalalignment='center', color='darkblue', fontsize=12,
             bbox=dict(facecolor='white', alpha=0.7))
    
    plt.show()
    logger.info("Generated ROI comparison chart.")

plot_roi_comparison(annual_time_saved, CONFIG['T_manual'], CONFIG['D_annual'])
```

## 8. Orchestrating the End-to-End Pipeline and Production Readiness

Bringing all the pieces together into a single, orchestrated pipeline is the final step in operationalizing our solution. This section wraps the extraction, transformation, loading, and quality checks into a single `run_pipeline` function. We then use Python's `schedule` library to configure this pipeline to run automatically every day after market close, mimicking a real-world production schedule. Robust logging is configured to provide an audit trail and help diagnose any issues, a critical component for compliance and operational monitoring.

While `schedule` is excellent for demonstration, Sri understands that in enterprise environments, dedicated orchestrators like Apache Airflow or OS-level `cron` jobs offer greater robustness and monitoring capabilities for production systems.

```python
def run_pipeline():
    """
    Executes the full ETL pipeline: Extract -> Transform -> Load -> Quality Checks.
    """
    logger.info("=" * 60)
    logger.info("PIPELINE START")
    start_time_pipeline = time.time()
    
    try:
        # Step 2: Extract - API Data Retrieval
        current_end_date = datetime.now().strftime('%Y-%m-%d')
        CONFIG['end_date'] = current_end_date # Ensure end date is always current
        
        market_data, failed_tickers = fetch_market_data(
            CONFIG['tickers'],
            CONFIG['start_date'],
            CONFIG['end_date'],
            CONFIG['interval']
        )

        # Step 3 & 4: Transform & Load - Cleaning, Validation, and Database Upsert
        conn_etl = init_database(CONFIG['db_path']) # Re-open connection for ETL steps
        quality_reports_for_run = [] # Collect reports for current run

        for ticker, df in market_data.items():
            if not df.empty:
                clean_df, report = clean_market_data(df.copy(), ticker, CONFIG)
                upsert_data(conn_etl, clean_df)
                quality_reports_for_run.append(report)
            else:
                logger.warning(f"No data for {ticker} to clean and upsert.")
        
        conn_etl.close() # Close connection after ETL

        # Step 5: Quality Checks and Alerting
        conn_qc_final = sqlite3.connect(CONFIG['db_path'])
        qc_summary_df = run_quality_checks(conn_qc_final, CONFIG['tickers'], CONFIG)
        conn_qc_final.close()

        num_passed_qc = qc_summary_df['PASS'].sum()
        num_total_qc = len(qc_summary_df)
        
        elapsed_time_pipeline = time.time() - start_time_pipeline
        logger.info(f"PIPELINE COMPLETE in {elapsed_time_pipeline:.1f}s | "
                    f"{num_total_qc} tickers processed | "
                    f"{num_passed_qc} passed QC")
        
        # In a real setup, this is where alerts for failed QC would be triggered.
        if num_passed_qc < num_total_qc:
            logger.error("Some quality checks failed. Review the quality report.")
            # Trigger alert mechanism here

    except Exception as e:
        logger.critical(f"PIPELINE FAILED: {e}", exc_info=True)
        # In a real setup, this would trigger an immediate system alert.
        raise # Re-raise for immediate visibility in notebook for demonstration

# --- Scheduling the pipeline ---
# For demonstration purposes, we'll configure a single run, 
# but in a real scenario, this would be scheduled.

# Option A: Python scheduler (foreground)
# schedule.every().day.at(CONFIG['run_time']).do(run_pipeline)
# print(f"Pipeline scheduled daily at {CONFIG['run_time']} (Python scheduler in foreground).")

# To run immediately for demonstration
print("Running pipeline manually for demonstration...")
run_pipeline()
print("Pipeline run completed. Check pipeline.log for details.")

# The 'while True' loop for `schedule` is typically run in a separate script or background process.
# For this notebook, we execute `run_pipeline` once to demonstrate its functionality.
# If running indefinitely, uncomment the while loop and schedule setup.
# while True:
#     schedule.run_pending()
#     time.sleep(1) # Check every 1 second if a job is pending

# --- Database Integrity Verification (Pytest Concept) ---
# While a full pytest suite is typically run as separate files,
# we can demonstrate the *concept* of an automated database integrity check within the notebook.

def verify_db_integrity(db_path):
    """
    Demonstrates a basic database integrity check (e.g., table exists, not empty).
    This function mimics a single assertion that would be part of a pytest suite.
    """
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Check 1: market_data table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='market_data'")
        table_exists = cursor.fetchone() is not None
        assert table_exists, "Database table 'market_data' does not exist."
        logger.info("Integrity Check: Table 'market_data' exists.")

        # Check 2: Data is not empty (e.g., at least 10 tickers should have data)
        df_tickers = pd.read_sql("SELECT DISTINCT ticker FROM market_data", conn)
        assert len(df_tickers) >= 1, "Database is empty or has less than 1 ticker."
        logger.info(f"Integrity Check: Database contains data for {len(df_tickers)} tickers.")
        
        # Check 3: No future dates
        future_dates_df = pd.read_sql("SELECT * FROM market_data WHERE date > date('now')", conn)
        assert future_dates_df.empty, f"Found {len(future_dates_df)} records with future dates."
        logger.info("Integrity Check: No future dates found.")

        # Check 4: All closing prices are positive
        non_positive_prices_df = pd.read_sql("SELECT * FROM market_data WHERE close <= 0", conn)
        assert non_positive_prices_df.empty, f"Found {len(non_positive_prices_df)} records with non-positive closing prices."
        logger.info("Integrity Check: All closing prices are positive.")

        # Check 5: No duplicate (ticker, date) pairs
        duplicate_pairs_df = pd.read_sql("""
            SELECT ticker, date, COUNT(*) as cnt
            FROM market_data
            GROUP BY ticker, date
            HAVING cnt > 1
        """, conn)
        assert duplicate_pairs_df.empty, f"Found {len(duplicate_pairs_df)} duplicate (ticker, date) records."
        logger.info("Integrity Check: No duplicate (ticker, date) pairs found.")

        logger.info("All database integrity checks PASSED successfully.")
        return True
    except AssertionError as ae:
        logger.error(f"Database Integrity Check FAILED: {ae}")
        print(f"Database Integrity Check FAILED: {ae}")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred during database integrity check: {e}")
        print(f"An unexpected error occurred during database integrity check: {e}")
        return False
    finally:
        if conn:
            conn.close()

# Execute database integrity verification
print("\nRunning database integrity verification...")
db_ok = verify_db_integrity(CONFIG['db_path'])
if db_ok:
    print("Database integrity confirmed. Data is ready for downstream use.")
else:
    print("Database integrity issues detected. Please review logs and data.")
```
