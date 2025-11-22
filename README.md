# ETL Project (Educational Demo)

## 🎯 About the Project
This project is a **demonstration of a classic ETL pipeline** (Extract, Transform, Load) in Python.
It simulates a real-world data engineering task:
1.  We have "dirty" raw data (duplicates, input errors).
2.  We need to clean it up.
3.  Load it into storage (Database).
4.  Derive business value (Report).

The script is fully **autonomous**: it generates its own initial test data on every run, so you don't need to download any extra files.

## ⚙️ How It Works (Workflow)
The entire process is executed in `main.py` and consists of 4 steps:

1.  **Data Generation (`generate_dummy_data`)**:
    - Creates `users.json` and `orders.csv`.
    - Intentionally adds "garbage": user duplicates, negative prices, orders for non-existent people.
    - *Goal*: Ensure reproducibility and demonstrate the cleaning process.

2.  **ETL Process (`process_data` & `load_to_db`)**:
    - **Extract**: Reads raw files.
    - **Transform**:
        - Removes user duplicates (keeps the most recent record).
        - Filters invalid orders (price < 0).
        - Checks integrity (removes orders without users).
    - **Load**: Saves clean data into a SQLite database (`shop_data.db`).

3.  **Analytics (`get_ltv_report`)**:
    - Executes a SQL query against the database.
    - Calculates LTV (Lifetime Value) — how much money each customer brought in.
    - Outputs the top 3 buyers.

## Prerequisites
- Python 3.8+

## Installation
1. Clone the repository (or download the files).
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage
Run the main script:
```bash
python main.py
```

### CLI Options
You can customize the execution with the following flags:
- `--generate`: Force regeneration of dummy data.
- `--no-etl`: Skip the ETL process (useful if you only want to see the report).
- `--db <path>`: Specify a custom database file path (default: `shop_data.db`).
- `--data-dir <path>`: Specify a custom data directory (default: `data`).

**Examples:**
```bash
# Only generate data
python main.py --generate --no-etl

# Run with a custom database
python main.py --db my_shop.db
```

## Testing
To run tests without creating temporary cache files (`__pycache__`, `.pytest_cache`), use the provided wrapper:
```bash
python run_tests.py
```

## 🐳 Docker Support
You can run the project in a Docker container to ensure a consistent environment.

1.  **Build the image**:
    ```bash
    docker build -t etl-project .
    ```

2.  **Run the container**:
    ```bash
    docker run --rm etl-project
    ```

## Output
- **Console**: Logs of the process and the final LTV report table.
- **Files**:
    - `data/users.json`: Generated user data.
    - `data/orders.csv`: Generated order data.
    - `shop_data.db`: SQLite database with processed data.
