import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sqlite3
import os
import datetime as dt
from datetime import datetime, timedelta
import logging
import time

# Import business logic
from source import *

# Initialize logger
try:
    app_logger = logging.getLogger('data_pipeline')
except NameError:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
    app_logger = logging.getLogger('streamlit_app')

st.set_page_config(page_title="QuLab: Lab 10: Automating Data Ingestion", layout="wide")
st.sidebar.image("https://www.quantuniversity.com/assets/img/logo5.jpg")
st.sidebar.divider()
st.title("QuLab: Lab 10: Automating Data Ingestion")
st.divider()

# Session State Initialization
if 'page' not in st.session_state:
    st.session_state.page = 'Overview'

if 'config' not in st.session_state:
    # CONFIG is imported from source
    st.session_state.config = CONFIG.copy()

if 'initial_pipeline_run_done' not in st.session_state:
    st.session_state.initial_pipeline_run_done = True
    st.session_state.pipeline_last_run_status = "Initial run on app start (from source.py execution)."
    st.session_state.pipeline_last_run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Access globals imported from source
    st.session_state.quality_summary_df = globals().get('quality_summary_df', pd.DataFrame()).copy() if 'quality_summary_df' in globals() else pd.DataFrame()
    st.session_state.annual_time_saved = globals().get('annual_time_saved', 0)
    st.session_state.manual_errors = globals().get('manual_errors', 0)
    st.session_state.automated_errors = globals().get('automated_errors', 0)
    st.session_state.db_verified = globals().get('db_ok', False)

if 'selected_tickers' not in st.session_state:
    st.session_state.selected_tickers = st.session_state.config['tickers'][:3]

if 'raw_data_viewer_tickers' not in st.session_state:
    st.session_state.raw_data_viewer_tickers = st.session_state.config['tickers'][:1]

if 'raw_data_viewer_start_date' not in st.session_state:
    st.session_state.raw_data_viewer_start_date = (datetime.now() - timedelta(days=30)).date()

if 'raw_data_viewer_end_date' not in st.session_state:
    st.session_state.raw_data_viewer_end_date = datetime.now().date()

if 'pipeline_log_content' not in st.session_state:
    try:
        with open(st.session_state.config['log_file'], 'r') as f:
            st.session_state.pipeline_log_content = f.read()
    except FileNotFoundError:
        st.session_state.pipeline_log_content = "Log file not found or pipeline not run yet."

# Sidebar
st.sidebar.title("Navigation")
page_options = [
    "Overview",
    "Run Pipeline",
    "Data Verification & Quality",
    "Performance & ROI",
    "System Integrity & Production"
]

# Use a temporary variable for the radio widget to handle the state update
current_page_index = page_options.index(st.session_state.page) if st.session_state.page in page_options else 0
selected_page = st.sidebar.radio("Go to", page_options, index=current_page_index)
if selected_page != st.session_state.page:
    st.session_state.page = selected_page
    st.rerun()

st.sidebar.header("Pipeline Configuration")
selected_sidebar_tickers = st.sidebar.multiselect(
    "Select Tickers for Analysis",
    options=st.session_state.config['tickers'],
    default=st.session_state.selected_tickers
)

if selected_sidebar_tickers != st.session_state.selected_tickers:
    st.session_state.selected_tickers = selected_sidebar_tickers
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.subheader("Current Pipeline Settings")
for key, value in st.session_state.config.items():
    if key not in ['tickers', 'db_path', 'csv_backup_dir', 'log_file', 'logger_name']:
        st.sidebar.markdown(f"**{key.replace('_', ' ').title()}:** `{value}`")
st.sidebar.markdown(f"**Database Path:** `{st.session_state.config['db_path']}`")
st.sidebar.markdown(f"**Log File:** `{st.session_state.config['log_file']}`")

# Page Content
if st.session_state.page == "Overview":
    st.markdown(f"### Automating Market Data Ingestion for Investment Research") 

    st.markdown(f"""
        At Alpha Investments, the daily ritual of manually copying and pasting market data
        from sources like Bloomberg or Refinitiv into Excel spreadsheets has become a significant bottleneck.
        This process is not only time-consuming, consuming precious hours that could be spent on high-value
        analytical tasks, but it's also prone to human error, leading to inconsistent data across various
        valuation models and dashboards. As a CFA Charterholder, Sri understands the critical importance
        of reliable data as the foundation for all investment decisions and subsequent AI/ML models.

        This application demonstrates an automated, Python-based market data pipeline. This pipeline
        connects to financial APIs, ingests daily OHLCV, fundamental, and macroeconomic data, applies
        robust data cleaning and validation rules, stores the data in a local SQLite database,
        and generates automated quality reports. By doing so, Sri aims to liberate his team from
        low-value data wrangling, drastically reduce errors, and establish a scalable, auditable
        data infrastructure. This hands-on journey will demonstrate a real-world workflow, from
        initial configuration to automated daily execution, embodying key data engineering principles
        relevant to financial professionals.
    """)

    st.subheader("1. ETL Architecture Overview (V1)")
    st.markdown(f"""
        The automated market data pipeline follows an Extract-Transform-Load (ETL) pattern,
        integrating robust quality checks and scheduled execution.
        This diagram illustrates the flow of data from its source to its consumption by downstream models and dashboards.
        The system ensures a resilient and auditable data supply.
    """)
    
    st.markdown(
        """
        ```mermaid
        graph TD
            A[Data Sources: Yahoo Finance API] --> B(Extract: API Data Retrieval with Error Handling)
            B --> C(Transform: Clean, Validate, Impute)
            C --> D(Load: SQLite DB Upsert with Idempotency)
            D --> E(Quality: Checks & Alerts)
            E --> F(Consumers: Models, Excel, Dashboards)
            G[Scheduler: Daily Automation] --> B
        ```
        """
    )
    st.markdown(f"""
        **Key Stages:**
        *   **Extract**: Data is fetched from financial APIs, incorporating retry logic to handle transient network issues or API rate limits.
        *   **Transform**: Raw data undergoes crucial cleaning and validation steps, including removing duplicates, handling missing prices (e.g., forward-fill), and flagging extreme price movements.
        *   **Load**: Cleaned and validated data is stored idempotently into a local SQLite database, preventing duplicate records on re-runs and ensuring data integrity.
        *   **Quality**: Automated checks are performed post-load to verify data freshness, completeness, price validity, and to detect anomalous movements.
        *   **Scheduler**: The entire pipeline is orchestrated to run automatically at a predefined schedule, typically daily after market close.
        *   **Consumers**: The high-quality, reliable data from the database feeds directly into downstream investment models, analytical dashboards, and other firm-wide systems.
    """)

elif st.session_state.page == "Run Pipeline":
    st.header("Automated Market Data Pipeline Execution")
    st.markdown(f"""
        This section allows you to manually trigger the full ETL pipeline execution.
        In a production environment, this pipeline would typically run automatically
        on a schedule (e.g., daily after market close).
        """)

    st.info(f"**Last Pipeline Run:** {st.session_state.pipeline_last_run_timestamp} | Status: {st.session_state.pipeline_last_run_status}")

    if st.button("Run Full ETL Pipeline Now"):
        with st.spinner("Running the pipeline... This might take a moment."):
            try:
                run_pipeline()

                conn_after_run = sqlite3.connect(st.session_state.config['db_path'])
                st.session_state.quality_summary_df = run_quality_checks(conn_after_run, st.session_state.config['tickers'], st.session_state.config)
                conn_after_run.close()

                st.session_state.annual_time_saved, st.session_state.manual_errors, st.session_state.automated_errors = calculate_roi(st.session_state.config)
                st.session_state.db_verified = verify_db_integrity(st.session_state.config['db_path'])

                try:
                    with open(st.session_state.config['log_file'], 'r') as f:
                        st.session_state.pipeline_log_content = f.read()
                except FileNotFoundError:
                    st.session_state.pipeline_log_content = "Log file not found after run."

                st.session_state.pipeline_last_run_status = "Successfully completed."
                st.session_state.pipeline_last_run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                st.success("Pipeline executed successfully!")
                st.balloons()
                st.rerun()

            except Exception as e:
                st.session_state.pipeline_last_run_status = f"Failed with error: {e}"
                st.error(f"Pipeline execution failed: {e}")
                app_logger.critical(f"Streamlit pipeline trigger failed: {e}", exc_info=True)

    st.subheader("Latest Pipeline Run Details")
    st.markdown(f"**Last Updated:** {st.session_state.pipeline_last_run_timestamp}")

    if not st.session_state.quality_summary_df.empty:
        st.markdown("#### Latest Quality Check Summary")
        st.dataframe(st.session_state.quality_summary_df)
    else:
        st.warning("No quality check summary available. Run the pipeline first.")

    st.markdown("#### Pipeline Execution Log (Latest)")
    st.code(st.session_state.pipeline_log_content, height=300)

elif st.session_state.page == "Data Verification & Quality":
    st.header("Data Verification and Quality Assurance")
    st.markdown(f"""
        Ensuring high data quality is paramount for any investment decision.
        This section provides an interactive dashboard to verify the ingested market data
        and monitor its quality metrics, directly reflecting the 'Transform' and 'Quality' stages.
    """)

    st.subheader("Raw Market Data Viewer")
    st.markdown(f"""
        Browse the raw OHLCV data directly from the `market_data.db` database.
        This allows for quick spot-checks of recently ingested data for selected tickers and date ranges.
    """)
    viewer_tickers_options = st.multiselect(
        "Select Ticker(s) for Raw Data View",
        options=st.session_state.config['tickers'],
        default=st.session_state.raw_data_viewer_tickers
    )
    
    if viewer_tickers_options != st.session_state.raw_data_viewer_tickers:
        st.session_state.raw_data_viewer_tickers = viewer_tickers_options
        # st.rerun() # Not strictly necessary if button is used, but good for sync.

    col1, col2 = st.columns(2)
    with col1:
        viewer_start_date_input = st.date_input(
            "Start Date (Raw Data Viewer)",
            value=st.session_state.raw_data_viewer_start_date,
            max_value=datetime.now().date()
        )
        if viewer_start_date_input != st.session_state.raw_data_viewer_start_date:
            st.session_state.raw_data_viewer_start_date = viewer_start_date_input
    with col2:
        viewer_end_date_input = st.date_input(
            "End Date (Raw Data Viewer)",
            value=st.session_state.raw_data_viewer_end_date,
            max_value=datetime.now().date()
        )
        if viewer_end_date_input != st.session_state.raw_data_viewer_end_date:
            st.session_state.raw_data_viewer_end_date = viewer_end_date_input

    if st.button("Load Raw Data for View"):
        if st.session_state.raw_data_viewer_tickers:
            conn_raw_viewer = sqlite3.connect(st.session_state.config['db_path'])
            query = f"""
                SELECT ticker, date, open, high, low, close, volume, dividend, updated_at
                FROM market_data
                WHERE ticker IN ({','.join([f"'{t}'" for t in st.session_state.raw_data_viewer_tickers])})
                AND date BETWEEN '{st.session_state.raw_data_viewer_start_date.strftime('%Y-%m-%d')}' AND '{st.session_state.raw_data_viewer_end_date.strftime('%Y-%m-%d')}'
                ORDER BY ticker, date DESC
            """
            try:
                df_raw = pd.read_sql(query, conn_raw_viewer, parse_dates=['date', 'updated_at'])
                if not df_raw.empty:
                    st.dataframe(df_raw)
                else:
                    st.warning("No data found for the selected tickers and date range.")
            except Exception as e:
                st.error(f"Error querying database: {e}")
            finally:
                conn_raw_viewer.close()
        else:
            st.warning("Please select at least one ticker to view raw data.")

    st.markdown("---")
    st.subheader("2. Price Series Verification (V2)")
    st.markdown(f"""
        This line plot shows the most recent 30 days of closing price data for your selected tickers.
        It's a quick visual check to confirm that data is flowing accurately and consistently for key assets.
    """)
    st.markdown(f"**Selected Tickers (from sidebar):** {', '.join(st.session_state.selected_tickers)}")
    st.markdown(f"**Data displayed for:** Last 30 trading days.")

    if st.session_state.selected_tickers:
        conn_price_series = sqlite3.connect(st.session_state.config['db_path'])
        
        # Determine subplot layout
        n_tickers = len(st.session_state.selected_tickers)
        fig_price_series, axes = plt.subplots(n_tickers, 1, figsize=(12, 4 * n_tickers), sharex=True)
        
        # Handle single ticker case where axes is not a list
        if n_tickers == 1:
            axes = [axes]

        for i, ticker in enumerate(st.session_state.selected_tickers):
            try:
                df_plot = pd.read_sql(
                    f"SELECT date, close FROM market_data WHERE ticker='{ticker}' ORDER BY date DESC LIMIT 30",
                    conn_price_series, parse_dates=['date']
                )
                if not df_plot.empty:
                    df_plot = df_plot.sort_values('date')
                    axes[i].plot(df_plot['date'], df_plot['close'], label=f'{ticker} Close Price', color='blue')
                    axes[i].set_title(f'{ticker} Last 30 Days Close Price')
                    axes[i].grid(True, linestyle='--', alpha=0.6)
                    axes[i].legend()
                else:
                    axes[i].set_title(f'No data for {ticker}')
                    axes[i].text(0.5, 0.5, 'No Data Available', horizontalalignment='center', verticalalignment='center')
            except Exception as e:
                axes[i].set_title(f'Error loading {ticker}')
                app_logger.error(f"Error plotting {ticker}: {e}")

        plt.tight_layout()
        st.pyplot(fig_price_series)
        plt.close(fig_price_series)
        conn_price_series.close()
    else:
        st.info("Please select tickers in the sidebar to view price series.")


    st.markdown("---")
    st.subheader("3. Quality Dashboard (V3)")
    st.markdown(f"""
        This heatmap visualizes the results of critical data quality checks for each ticker.
        It provides an immediate, color-coded overview of data health:
        *   **Green (1)**: Pass
        *   **Red (0)**: Fail
    """)
    if not st.session_state.quality_summary_df.empty:
        fig_heatmap = plt.figure(figsize=(10, 8))
        check_cols = ['fresh', 'complete', 'valid_prices', 'no_extreme_moves']
        
        # Ensure these columns exist
        available_cols = [c for c in check_cols if c in st.session_state.quality_summary_df.columns]
        
        if available_cols:
            df_heatmap_plot = st.session_state.quality_summary_df[['ticker'] + available_cols].copy()
            df_heatmap_plot[available_cols] = df_heatmap_plot[available_cols].astype(int)
            sns.heatmap(df_heatmap_plot.set_index('ticker')[available_cols], annot=True, cmap='RdYlGn', fmt='d', linewidths=.5, linecolor='black', ax=plt.gca())
            plt.title('Data Quality Check Heatmap (0=Fail, 1=Pass)')
            plt.ylabel('Ticker')
            plt.xlabel('Quality Metric')
            st.pyplot(fig_heatmap)
            plt.close(fig_heatmap)
        else:
            st.warning("Quality columns missing from summary.")
    else:
        st.warning("No quality summary available. Run the pipeline first.")

    st.markdown("---")
    st.subheader("4. Data Completeness Over Time (V5)")
    st.markdown(f"""
        Data completeness is crucial. This chart displays the Data Completeness Ratio ($C_i$)
        for a selected ticker, confirming consistent data delivery against an expected threshold.
        For each ticker $i$, the completeness ratio $C_i$ quantifies the proportion of actual trading days with data ($N_{{actual,i}}$) against the expected number of business days ($N_{{expected,i}}$) within a given date range.
    """)
    st.markdown(r"$$ C_i = \frac{N_{actual,i}}{N_{expected,i}} $$")
    st.markdown(r"where $N_{actual,i}$ is the number of actual trading days with data for ticker $i$, and $N_{expected,i}$ is the expected number of business days (excluding weekends) in the date range. A target of $C_i > 0.98$ is typically used, allowing for rare holidays or market closures that might not be captured as business days.")

    completeness_ticker_select = st.selectbox(
        "Select Ticker for Completeness Ratio View",
        options=st.session_state.config['tickers'],
        index=0
    )

    if completeness_ticker_select and not st.session_state.quality_summary_df.empty:
        current_completeness = 0.0
        if 'completeness' in st.session_state.quality_summary_df.columns:
             row = st.session_state.quality_summary_df[st.session_state.quality_summary_df['ticker'] == completeness_ticker_select]
             if not row.empty:
                 current_completeness = row['completeness'].iloc[0]

        fig_completeness = plt.figure(figsize=(10, 5))
        plt.bar(['Current Run'], [current_completeness], color='skyblue')
        plt.axhline(y=(1 - st.session_state.config.get('max_missing_pct', 0.02)), color='r', linestyle='--',
                    label=f'Target Completeness (>{(1 - st.session_state.config.get("max_missing_pct", 0.02)) * 100:.0f}%)')
        plt.title(f'Data Completeness Ratio for {completeness_ticker_select} (Current Run)')
        plt.ylabel('Completeness Ratio')
        plt.ylim(0, 1.05)
        plt.legend()
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        st.pyplot(fig_completeness)
        plt.close(fig_completeness)
    elif completeness_ticker_select:
        st.warning(f"No quality summary available for {completeness_ticker_select}. Run the pipeline first.")
    else:
        st.info("No tickers available in configuration for completeness view.")

    st.markdown("---")
    st.subheader("Latest Daily Data Quality Report")
    st.markdown(f"""
        This interactive table provides a detailed summary of the latest data quality checks,
        showing pass/fail status for each metric per ticker.
    """)
    if not st.session_state.quality_summary_df.empty:
        st.dataframe(st.session_state.quality_summary_df)
    else:
        st.info("No latest quality report available. Run the pipeline to generate one.")

elif st.session_state.page == "Performance & ROI":
    st.header("Pipeline Performance and Return on Investment (ROI)")
    st.markdown(f"""
        Understanding the operational efficiency and business value of automation is crucial.
        This section quantifies the ROI of the automated data pipeline and provides insights
        into its execution performance.
    """)

    st.subheader("1. Pipeline Execution Timeline (V4 - Partial)")
    st.markdown(f"""
        While `source.py` provides an overall pipeline execution time, direct step-by-step
        timing for a bar chart (V4) is not explicitly exposed by functions in `source.py`
        in a format directly suitable for a visual timeline.
        However, the `pipeline.log` provides timestamped entries for each major step,
        allowing for audit and manual analysis of performance bottlenecks.
        """)
    st.info(f"Overall Pipeline Run Status: **{st.session_state.pipeline_last_run_status}**")
    st.info(f"Last Pipeline Run Timestamp: **{st.session_state.pipeline_last_run_timestamp}**")
    st.markdown("Review the 'Pipeline Execution Log' below for detailed step timings and order of execution.")


    st.markdown("---")
    st.subheader("2. ROI Comparison Chart (V6)")
    st.markdown(f"""
        Quantifying the Return on Investment (ROI) of automation provides a compelling business case.
        This chart visually compares the annual time investment for manual vs. automated data ingestion,
        highlighting the significant time savings and efficiency gains.
    """)
    st.markdown(f"""
        The time saved per year ($\Delta T$) is calculated by comparing the manual effort
        to the automated process, factoring in initial setup and ongoing maintenance costs:
    """)
    st.markdown(r"$$ \Delta T = T_{manual} \times D_{annual} - T_{setup} - T_{maint} $$")
    st.markdown(r"where $T_{manual}$ represents the daily manual effort (in hours), $D_{annual}$ is the annual number of trading days, $T_{setup}$ is the one-time pipeline development cost (in hours), and $T_{maint}$ is the annual maintenance effort (in hours).")

    if st.session_state.annual_time_saved:
        manual_time_annual = st.session_state.config['T_manual'] * st.session_state.config['D_annual']
        labels = ['Manual Process', 'Automated Pipeline']
        # Calculate time for automated pipeline as manual time minus savings
        # Logic: Saved = Manual - Auto => Auto = Manual - Saved
        times = [manual_time_annual, manual_time_annual - st.session_state.annual_time_saved]

        fig_roi = plt.figure(figsize=(8, 6))
        plt.bar(labels, times, color=['lightcoral', 'lightseagreen'])
        plt.ylabel('Hours per Year')
        plt.title('Annual Time Investment: Manual vs. Automated Data Ingestion')
        
        for i, v in enumerate(times):
            plt.text(i, v + 10, f"{v:.1f} hrs", ha='center', va='bottom')
        
        plt.text(0.5, max(times) * 0.9, f"Savings: {st.session_state.annual_time_saved:.1f} hrs/year",
                 horizontalalignment='center', color='darkblue', fontsize=12, bbox=dict(facecolor='white', alpha=0.7))
        st.pyplot(fig_roi)
        plt.close(fig_roi)

        st.markdown(f"""
            **Calculated Annual Time Savings by Automation:** `{st.session_state.annual_time_saved:.2f}` hours/year.
            **Expected Daily Errors Eliminated (Manual vs. Automated):** `{st.session_state.manual_errors:.2f}` vs. `{st.session_state.automated_errors:.4f}`.
            This represents a significant reduction in potential data transcription errors, leading to more reliable investment models.
        """)
    else:
        st.warning("ROI figures not available. Please ensure the pipeline has run successfully.")

    st.markdown("---")
    st.subheader("3. Pipeline Execution Log Viewer")
    st.markdown(f"""
        The pipeline generates a detailed log file (`{st.session_state.config['log_file']}`)
        that provides an audit trail of every execution step, warnings, and errors.
        This log is crucial for debugging, performance monitoring, and compliance purposes.
        """)
    st.code(st.session_state.pipeline_log_content, height=400)

elif st.session_state.page == "System Integrity & Production":
    st.header("System Integrity and Production Readiness")
    st.markdown(f"""
        Building a reliable data pipeline involves more than just scripting; it requires
        attention to system integrity, automated testing, and robust production deployment considerations.
    """)

    st.subheader("1. Pytest Results Summary")
    st.markdown(f"""
        Automated data quality tests, often implemented using frameworks like Pytest,
        are critical for ensuring the integrity of the data stored in the database.
        These tests provide a clear indication of the pass/fail status of the data structure and content,
        acting as a safeguard against corrupted or incomplete data.
    """)
    if st.session_state.db_verified:
        st.success("All automated database integrity checks PASSED successfully! Data is confirmed ready for downstream use.")
        st.markdown(f"""
            The following key integrity checks were performed (mimicking a Pytest suite, as defined in `source.py`):
            *   Database table 'market_data' exists.
            *   Database contains data for a sufficient number of tickers.
            *   No records found with future dates.
            *   All closing prices are positive values.
            *   No duplicate (ticker, date) pairs found, confirming idempotency.
        """)
    else:
        st.error("Database integrity issues detected. Please review the pipeline execution logs and the raw data for errors.")
        st.warning("It is recommended to run the pipeline again to potentially resolve any underlying data integrity problems.")


    st.markdown("---")
    st.subheader("2. From Script to System: The Production Gap")
    st.markdown(f"""
        This pipeline demonstrates a working solution for data ingestion. However, to transition
        from a simple Python script in a notebook to a true production system, additional
        enterprise-grade considerations are crucial:
        *   **Secrets Management**: Secure handling of sensitive API keys (e.g., using environment variables, AWS Secrets Manager, or HashiCorp Vault) rather than hardcoding.
        *   **Containerization**: Packaging the application and its dependencies (e.g., using Docker) for consistent, isolated, and portable deployment across different environments (development, staging, production).
        *   **Orchestration**: Employing dedicated tools like Apache Airflow or Prefect for managing complex Directed Acyclic Graphs (DAGs) of tasks. These orchestrators provide advanced scheduling, robust retries, dependency tracking, backfilling, and centralized monitoring.
        *   **Monitoring & Alerting**: Implementing dedicated dashboards (e.g., Grafana, Prometheus) to track pipeline health, resource utilization, and data quality metrics, coupled with automated alerts for any failures or anomalies.
        *   **CI/CD Pipelines**: Establishing Continuous Integration/Continuous Deployment pipelines to automate testing, building, and deployment of code changes, ensuring reliability and efficiency in updates.

        Recognizing and addressing this "production gap" is essential for financial professionals aiming to build robust, scalable, and maintainable data infrastructure.
    """)

    st.markdown("---")
    st.subheader("3. Free vs. Paid Data: The Trade-off")
    st.markdown(f"""
        The pipeline currently leverages the Yahoo Finance API, which is a free data source. While excellent for educational purposes and rapid prototyping,
        free APIs often come with inherent limitations that are critical in a production financial environment:
        *   **Reliability & SLAs**: Free services typically do not offer Service Level Agreements (SLAs), meaning data availability, accuracy, and timeliness are not guaranteed, leading to potential disruptions for investment models.
        *   **Rate Limits**: Strict limitations on the number of API calls can hinder comprehensive or high-frequency data collection for a large universe of assets.
        *   **Support**: Lack of dedicated technical support for issues, making problem resolution challenging.
        *   **Data Completeness & Quality**: Potential for data gaps, delays, or inconsistencies compared to professional-grade data vendors.

        For production-grade investment systems, firms invariably opt for paid data providers such as Bloomberg Data License, Refinitiv Eikon, or FactSet. These services:
        *   Involve significant annual costs (e.g., $20K-$100K+), reflecting their premium service.
        *   Provide robust SLAs, ensuring high data quality, availability, and a guarantee of service.
        *   Offer dedicated technical support and comprehensive historical data.

        Crucially, the underlying architecture of this ETL pipeline, particularly its 'Transform' and 'Load' stages, is largely source-agnostic. Upgrading to a paid data source would primarily involve modifying only the 'Extract' layer to connect to the new API, while retaining most of the existing data cleaning, validation, and storage logic.
    """)

# License
st.caption('''
---
## QuantUniversity License

© QuantUniversity 2025  
This notebook was created for **educational purposes only** and is **not intended for commercial use**.  

- You **may not copy, share, or redistribute** this notebook **without explicit permission** from QuantUniversity.  
- You **may not delete or modify this license cell** without authorization.  
- This notebook was generated using **QuCreate**, an AI-powered assistant.  
- Content generated by AI may contain **hallucinated or incorrect information**. Please **verify before using**.  

All rights reserved. For permissions or commercial licensing, contact: [info@qusandbox.com](mailto:info@qusandbox.com)
''')
