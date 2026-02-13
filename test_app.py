
from streamlit.testing.v1 import AppTest
import pandas as pd
import datetime as dt
import os
import sqlite3

# Helper function to clean up dummy files if they exist (for robust testing)
def _cleanup_test_files(config_data):
    if os.path.exists(config_data['db_path']):
        os.remove(config_data['db_path'])
    if os.path.exists(config_data['log_file']):
        os.remove(config_data['log_file'])
    if os.path.exists('test_csv_backup'): # Assuming this directory might be created
        import shutil
        shutil.rmtree('test_csv_backup', ignore_errors=True)

# Define a minimal CONFIG and dummy functions if source.py isn't mocked globally
# In a real test setup, you would ensure source.py exists and is correctly mocked
# or uses a test-specific configuration. For this AppTest, we assume source.py
# and its dependencies are available in the environment where tests are run.

# To ensure the app runs for testing purposes, these functions and CONFIG must be
# available from a 'source.py' file in the same directory or accessible via PYTHONPATH.
# A simplified 'source.py' for testing might look like this (NOT PART OF THE OUTPUT):
"""
# source.py (for testing purposes, not part of the output)
import pandas as pd
import datetime as dt
import sqlite3
import os
import logging
import time

app_logger = logging.getLogger('data_pipeline')
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')

CONFIG = {
    'tickers': ['AAPL', 'MSFT', 'GOOGL', 'AMZN'],
    'db_path': 'test_market_data.db',
    'csv_backup_dir': 'test_csv_backup',
    'log_file': 'test_pipeline.log',
    'logger_name': 'data_pipeline',
    'T_manual': 1.0,
    'D_annual': 252,
    'T_setup': 40.0,
    'T_maint': 10.0,
    'max_missing_pct': 0.02
}

def _create_dummy_db_and_log(db_path, log_file_path):
    os.makedirs(os.path.dirname(db_path) or '.', exist_ok=True)
    os.makedirs(os.path.dirname(log_file_path) or '.', exist_ok=True)

    if os.path.exists(db_path):
        os.remove(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_data (
            ticker TEXT,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            dividend REAL,
            updated_at TEXT,
            PRIMARY KEY (ticker, date)
        )
    ''')
    today = dt.date.today()
    for ticker in CONFIG['tickers']:
        for i in range(60):
            current_date = today - dt.timedelta(days=i)
            cursor.execute(f"INSERT OR IGNORE INTO market_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                           (ticker, current_date.strftime('%Y-%m-%d'), 100+i, 101+i, 99+i, 100+i, 100000+i*100, 0.0, dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    conn.commit()
    conn.close()

    if os.path.exists(log_file_path):
        os.remove(log_file_path)
    with open(log_file_path, 'w') as f:
        f.write("2023-01-01 10:00:00 | INFO | Pipeline started.\n")
        f.write("2023-01-01 10:00:05 | INFO | Extracted data for AAPL.\n")
        f.write("2023-01-01 10:00:10 | INFO | Transformed data for MSFT.\n")
        f.write("2023-01-01 10:00:15 | INFO | Loaded data into DB.\n")
        f.write("2023-01-01 10:00:20 | INFO | Pipeline finished.\n")

_create_dummy_db_and_log(CONFIG['db_path'], CONFIG['log_file'])

def run_pipeline():
    app_logger.info("Dummy pipeline run initiated.")
    time.sleep(0.1)
    _create_dummy_db_and_log(CONFIG['db_path'], CONFIG['log_file'])
    app_logger.info("Dummy pipeline run completed.")
    return True

def run_quality_checks(conn, tickers, config):
    app_logger.info("Running dummy quality checks.")
    data = []
    for ticker in tickers:
        data.append({
            'ticker': ticker,
            'fresh': 1,
            'complete': 0.99,
            'valid_prices': 1,
            'no_extreme_moves': 1,
            'recent_date': dt.date.today().strftime('%Y-%m-%d'),
            'max_age_days': 0,
            'status': 'PASS' # Added for DataFrame completeness
        })
    return pd.DataFrame(data)

def calculate_roi(config):
    app_logger.info("Calculating dummy ROI.")
    annual_time_saved = (config['T_manual'] * config['D_annual']) - config['T_setup'] - config['T_maint']
    manual_errors = 5.0
    automated_errors = 0.01
    return annual_time_saved, manual_errors, automated_errors

def verify_db_integrity(db_path):
    app_logger.info(f"Verifying dummy DB integrity at {db_path}.")
    return True
"""
# End of source.py placeholder comment


def test_initial_load_and_overview_page():
    # Test initial app load and verify session state and content on Overview page
    at = AppTest.from_file("app.py").run()

    # Verify session state initialization
    assert at.session_state["page"] == "Overview"
    assert "config" in at.session_state
    assert "initial_pipeline_run_done" in at.session_state
    assert at.session_state["initial_pipeline_run_done"] is True
    assert "quality_summary_df" in at.session_state
    assert "annual_time_saved" in at.session_state
    assert "db_verified" in at.session_state
    assert at.session_state["selected_tickers"] == at.session_state.config['tickers'][:3]
    assert "pipeline_log_content" in at.session_state
    assert "Log file not found" not in at.session_state["pipeline_log_content"] # Should be populated by dummy log

    # Verify title and main markdown content on the Overview page
    assert at.title[0].value == "QuLab: Lab 10: Automating Data Ingestion"
    assert at.markdown[1].value.startswith("### Automating Market Data Ingestion for Investment Research")
    assert "```mermaid" in at.markdown.value


def test_sidebar_navigation():
    # Test navigation using the sidebar radio buttons
    at = AppTest.from_file("app.py").run()

    # Navigate to "Run Pipeline"
    at.sidebar.radio.set_value("Run Pipeline").run()
    assert at.session_state["page"] == "Run Pipeline"
    assert at.header.value == "Automated Market Data Pipeline Execution"

    # Navigate to "Data Verification & Quality"
    at.sidebar.radio.set_value("Data Verification & Quality").run()
    assert at.session_state["page"] == "Data Verification & Quality"
    assert at.header.value == "Data Verification and Quality Assurance"

    # Navigate to "Performance & ROI"
    at.sidebar.radio.set_value("Performance & ROI").run()
    assert at.session_state["page"] == "Performance & ROI"
    assert at.header.value == "Pipeline Performance and Return on Investment (ROI)"

    # Navigate to "System Integrity & Production"
    at.sidebar.radio.set_value("System Integrity & Production").run()
    assert at.session_state["page"] == "System Integrity & Production"
    assert at.header.value == "System Integrity and Production Readiness"

    # Navigate back to "Overview"
    at.sidebar.radio.set_value("Overview").run()
    assert at.session_state["page"] == "Overview"
    assert at.markdown.value.startswith("### Automating Market Data Ingestion for Investment Research")


def test_sidebar_ticker_selection():
    # Test ticker selection in the sidebar
    at = AppTest.from_file("app.py").run()

    # Change selected tickers
    new_tickers = ['MSFT', 'GOOGL']
    at.sidebar.multiselect.set_value(new_tickers).run()
    assert at.session_state["selected_tickers"] == new_tickers

    # Verify that the app reruns and reflects the change (e.g., in a dependent page)
    # Navigate to "Data Verification & Quality" to see the effect
    at.sidebar.radio.set_value("Data Verification & Quality").run()
    assert f"**Selected Tickers (from sidebar):** {', '.join(new_tickers)}" in at.markdown.value


def test_run_pipeline_page_success():
    # Test running the pipeline successfully
    at = AppTest.from_file("app.py").run()
    at.sidebar.radio.set_value("Run Pipeline").run() # Navigate to "Run Pipeline" page

    # Ensure initial state before run
    initial_status = at.session_state["pipeline_last_run_status"]
    initial_timestamp = at.session_state["pipeline_last_run_timestamp"]
    assert "Successfully completed." not in initial_status

    # Click the "Run Full ETL Pipeline Now" button
    at.button.click().run()

    # Verify success message and balloons
    assert at.success.value == "Pipeline executed successfully!"
    assert at.balloons.exists
    
    # Verify session state updates
    assert at.session_state["pipeline_last_run_status"] == "Successfully completed."
    assert at.session_state["pipeline_last_run_timestamp"] > initial_timestamp # Should be a newer timestamp
    assert not at.session_state["quality_summary_df"].empty
    assert at.session_state["annual_time_saved"] > 0
    assert at.session_state["db_verified"] is True

    # Verify displayed content reflects updates
    assert at.info.value.startswith(f"**Last Pipeline Run:** {at.session_state['pipeline_last_run_timestamp']}")
    assert "Status: Successfully completed." in at.info.value
    assert at.markdown.value == "#### Latest Quality Check Summary"
    assert at.dataframe.exists
    assert "Pipeline Execution Log (Latest)" in at.markdown.value
    assert "Dummy pipeline run completed." in at.code.value


def test_data_verification_page_raw_data_viewer():
    # Test Raw Data Viewer on "Data Verification & Quality" page
    at = AppTest.from_file("app.py").run()
    at.sidebar.radio.set_value("Data Verification & Quality").run()

    # Set tickers for raw data viewer
    viewer_tickers = ['AAPL']
    at.multiselect.set_value(viewer_tickers).run()
    assert at.session_state.raw_data_viewer_tickers == viewer_tickers

    # Set date range
    start_date = (dt.date.today() - dt.timedelta(days=7))
    end_date = dt.date.today()
    at.date_input.set_value(start_date).run()
    at.date_input.set_value(end_date).run()
    assert at.session_state.raw_data_viewer_start_date == start_date
    assert at.session_state.raw_data_viewer_end_date == end_date

    # Load raw data
    at.button.click().run()

    # Verify dataframe exists and contains data (assuming dummy DB has data)
    assert at.dataframe.exists
    assert not at.dataframe.empty


def test_data_verification_page_plots_and_reports():
    # Test plots and quality reports on "Data Verification & Quality" page
    at = AppTest.from_file("app.py").run()
    
    # Ensure pipeline has run to populate quality data
    at.sidebar.radio.set_value("Run Pipeline").run()
    at.button.click().run()
    
    at.sidebar.radio.set_value("Data Verification & Quality").run()

    # Price Series Verification plot
    assert at.pyplot.exists

    # Quality Dashboard Heatmap
    assert at.pyplot.exists
    assert "Data Quality Check Heatmap" in at.pyplot.fig.suptitle.get_text() if at.pyplot.fig.suptitle else at.pyplot.fig.axes.get_title()

    # Data Completeness Over Time - select a ticker and verify plot
    at.selectbox.set_value(at.session_state.config['tickers']).run()
    assert at.pyplot.exists
    assert f"Data Completeness Ratio for {at.session_state.config['tickers']}" in at.pyplot.fig.axes.get_title()

    # Latest Daily Data Quality Report table
    assert at.dataframe.exists
    assert not at.dataframe.empty


def test_performance_roi_page():
    # Test "Performance & ROI" page content
    at = AppTest.from_file("app.py").run()
    
    # Ensure pipeline has run to populate ROI data
    at.sidebar.radio.set_value("Run Pipeline").run()
    at.button.click().run()

    at.sidebar.radio.set_value("Performance & ROI").run()

    # Verify ROI Comparison Chart
    assert at.pyplot.exists
    assert "Annual Time Investment: Manual vs. Automated Data Ingestion" in at.pyplot.fig.axes.get_title()

    # Verify calculated time savings and errors are displayed
    assert f"Calculated Annual Time Savings by Automation: {at.session_state.annual_time_saved:.2f} hours/year." in at.markdown.value
    assert f"Expected Daily Errors Eliminated (Manual vs. Automated): {at.session_state.manual_errors:.2f}" in at.markdown.value

    # Verify Pipeline Execution Log Viewer
    assert at.code.exists
    assert "Dummy pipeline run completed." in at.code.value


def test_system_integrity_page():
    # Test "System Integrity & Production" page
    at = AppTest.from_file("app.py").run()

    # Assume initial db_verified state is True from source.py
    assert at.session_state["db_verified"] is True

    at.sidebar.radio.set_value("System Integrity & Production").run()

    # Verify success message for Pytest Results Summary
    assert at.success.value == "All automated database integrity checks PASSED successfully! Data is confirmed ready for downstream use."

    # Simulate a failed DB verification
    at.session_state["db_verified"] = False
    at.rerun()
    assert at.error.value == "Database integrity issues detected. Please review the pipeline execution logs and the raw data for errors."

    # Cleanup any dummy files created by source.py (optional, for clean test runs)
    _cleanup_test_files(at.session_state.config)
