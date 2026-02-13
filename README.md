Here's a comprehensive `README.md` file for your Streamlit application lab project, designed to be professional and informative.

---

# QuLab: Lab 10: Automated Market Data Ingestion Pipeline

## Project Title and Description
This project, **"QuLab: Lab 10: Automating Data Ingestion"**, presents a Streamlit web application that demonstrates a robust, automated Python-based market data pipeline. Developed within the context of **Alpha Investments**, this system addresses the critical need to move beyond manual data collection (copy-pasting from sources like Bloomberg or Refinitiv into Excel) which is time-consuming, error-prone, and unsustainable for investment research.

The application showcases an end-to-end **Extract-Transform-Load (ETL)** process designed to:
*   **Extract** daily OHLCV (Open, High, Low, Close, Volume), fundamental, and macroeconomic data from financial APIs.
*   **Transform** raw data through cleaning, validation, and imputation, ensuring high data quality.
*   **Load** the processed data into a local SQLite database, leveraging idempotency to prevent duplicates.
*   Generate automated **quality reports** and **performance metrics** (like ROI) to quantify the value of automation.

This hands-on lab provides a practical, real-world workflow for financial professionals, emphasizing key data engineering principles crucial for building scalable, auditable, and reliable data infrastructure for investment decision-making and subsequent AI/ML model development.

## Features

The Streamlit application is organized into several interactive sections, each offering distinct functionalities:

1.  **Overview**:
    *   Detailed explanation of the project's motivation and business context.
    *   Visual representation of the ETL architecture (V1) using a Mermaid diagram.
    *   Description of each stage: Extract, Transform, Load, Quality, Scheduler, Consumers.

2.  **Run Pipeline**:
    *   Manual trigger to execute the full ETL pipeline on demand.
    *   Displays the last pipeline run timestamp and status.
    *   Presents the latest data quality check summary.
    *   Shows a real-time log of the pipeline's execution for auditing and debugging.

3.  **Data Verification & Quality**:
    *   **Raw Market Data Viewer**: Browse ingested OHLCV data directly from the SQLite database for selected tickers and date ranges.
    *   **Price Series Verification (V2)**: Interactive line plots displaying the last 30 days of closing prices for selected tickers, allowing for quick visual checks of data integrity.
    *   **Quality Dashboard (V3)**: Heatmap visualization of critical data quality checks (freshness, completeness, valid prices, no extreme moves), showing pass/fail status for each ticker.
    *   **Data Completeness Over Time (V5)**: Bar chart illustrating the data completeness ratio for a selected ticker against a target threshold, confirming consistent data delivery.
    *   **Latest Daily Data Quality Report**: A detailed interactive table summarizing the outcomes of all data quality checks.

4.  **Performance & ROI**:
    *   **Pipeline Execution Timeline (V4 - Partial)**: Provides overall run status and timestamp, with a reference to the detailed logs for step-by-step timings.
    *   **ROI Comparison Chart (V6)**: Quantifies the Return on Investment (ROI) by comparing annual time investment for manual vs. automated data ingestion, highlighting time savings and efficiency gains. Includes mathematical formulation for time savings.
    *   **Pipeline Execution Log Viewer**: Comprehensive display of the `pipeline.log` file, crucial for debugging, performance monitoring, and compliance.

5.  **System Integrity & Production**:
    *   **Pytest Results Summary**: Reports the status of automated database integrity checks (simulated via `source.py` functions), ensuring data validity and structure.
    *   **From Script to System: The Production Gap**: Discussion on advanced considerations for transitioning to a production environment (Secrets Management, Containerization, Orchestration, Monitoring, CI/CD).
    *   **Free vs. Paid Data: The Trade-off**: Explores the limitations of free data sources (like Yahoo Finance API) and the benefits/costs of professional-grade paid data providers in a financial context, emphasizing the pipeline's source-agnostic architecture.

## Getting Started

Follow these instructions to set up and run the Streamlit application on your local machine.

### Prerequisites

*   Python 3.8+
*   Git (for cloning the repository)

### Installation

1.  **Clone the Repository**:
    ```bash
    git clone <repository_url>
    cd <repository_name>
    ```
    (Replace `<repository_url>` and `<repository_name>` with your actual repository details.)

2.  **Create a Virtual Environment (Recommended)**:
    ```bash
    python -m venv venv
    source venv/bin/activate  # On macOS/Linux
    # venv\Scripts\activate   # On Windows
    ```

3.  **Install Dependencies**:
    First, you need to create a `requirements.txt` file based on the provided `app.py` and the implied `source.py` dependencies. In your project root, create a file named `requirements.txt` with the following content:
    ```
    streamlit
    pandas
    numpy
    matplotlib
    seaborn
    yfinance # Assuming yfinance is used in source.py for data extraction
    ```
    Now, install them:
    ```bash
    pip install -r requirements.txt
    ```

4.  **Review `source.py` Configuration**:
    The `source.py` file contains the core ETL logic and configuration parameters (e.g., `tickers`, `db_path`, `log_file`, `API_KEY` placeholders if any). Ensure you review this file for any specific setup or API key requirements. For Yahoo Finance, an explicit API key is usually not required, but this is good practice for future integrations.

## Usage

1.  **Run the Streamlit Application**:
    Navigate to the root directory of your project (where `app.py` is located) in your terminal and run:
    ```bash
    streamlit run app.py
    ```
    This command will open the Streamlit application in your default web browser.

2.  **Initial Pipeline Run**:
    Upon the first launch of the application, the ETL pipeline defined in `source.py` will automatically execute to populate the SQLite database (`market_data.db`) and generate an initial quality summary and log file (`pipeline.log`).

3.  **Navigate the App**:
    *   Use the **sidebar** on the left to navigate between different sections (Overview, Run Pipeline, Data Verification & Quality, Performance & ROI, System Integrity & Production).
    *   Interact with widgets (e.g., `multiselect` for tickers, `date_input` for data ranges, `buttons` to trigger pipeline runs) to explore the data and functionalities.
    *   Monitor the `Run Pipeline` page for the latest pipeline status and logs.

## Project Structure

The project directory is organized as follows:

```
.
├── README.md                 # This file
├── app.py                    # Main Streamlit application script
├── source.py                 # Core business logic, ETL functions, CONFIG, quality checks, ROI calculations
├── requirements.txt          # List of Python dependencies
├── market_data.db            # SQLite database (generated upon first pipeline run)
├── pipeline.log              # Log file for pipeline execution (generated)
└── csv_backup_dir/           # Directory for CSV backups (as per CONFIG, created if needed)
```

## Technology Stack

This application is built using the following technologies and libraries:

*   **Python**: The core programming language.
*   **Streamlit**: For creating the interactive web application interface.
*   **Pandas**: Essential for data manipulation and analysis.
*   **NumPy**: Fundamental package for numerical computing.
*   **Matplotlib**: For creating static, interactive, and animated visualizations.
*   **Seaborn**: A data visualization library based on Matplotlib, providing a high-level interface for drawing attractive and informative statistical graphics.
*   **SQLite3**: The embedded relational database system used for data storage.
*   **Yfinance** (implied by data ingestion): A popular library for fetching financial data from Yahoo Finance.
*   **Python `logging`**: For robust logging of pipeline activities.
*   **Mermaid**: For generating diagrams within Markdown.

## Contributing

While this is a lab project, contributions in the form of suggestions, bug reports, or feature enhancements are welcome.

1.  **Fork** the repository.
2.  **Create a new branch** (`git checkout -b feature/your-feature-name`).
3.  **Make your changes**.
4.  **Commit your changes** (`git commit -m 'Add new feature'`).
5.  **Push to the branch** (`git push origin feature/your-feature-name`).
6.  **Open a Pull Request**.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details (you would need to create a `LICENSE` file in your repository if you choose this license).

```
MIT License

Copyright (c) 2023 Quant University

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

## Contact

For questions or inquiries regarding this project, please contact:

*   **Quant University Team**
*   **Website:** [www.quantuniversity.com](https://www.quantuniversity.com)
*   **Email:** info@quantuniversity.com

---