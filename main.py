import pandas as pd
import sqlite3
import os
import json
import csv
import logging
import argparse
from pathlib import Path
from typing import Tuple, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants (can be overridden by CLI)
DEFAULT_DATA_DIR = Path('data')
DEFAULT_DB_FILE = 'shop_data.db'

def generate_dummy_data(data_dir: Path) -> None:
    """Generates dummy data for testing."""
    data_dir.mkdir(exist_ok=True)
    users_file = data_dir / 'users.json'
    orders_file = data_dir / 'orders.csv'
    
    # Users (with duplicates)
    users = [
        {"user_id": 1, "name": "Alice", "email": "alice@example.com", "reg_date": "2023-01-01"},
        {"user_id": 2, "name": "Bob", "email": "bob@example.com", "reg_date": "2023-01-05"},
        {"user_id": 1, "name": "Alice", "email": "alice_updated@example.com", "reg_date": "2023-01-10"}, # Newer record
        {"user_id": 3, "name": "Charlie", "email": "charlie@example.com", "reg_date": "2023-02-01"}
    ]
    with open(users_file, 'w', encoding='utf-8') as f:
        json.dump(users, f, indent=4)

    # Orders (with "dirty" data)
    orders_header = ['order_id', 'user_id', 'item_name', 'item_price', 'quantity', 'order_date']
    orders_data = [
        [101, 1, 'Laptop', 1000, 1, '2023-02-01'],
        [102, 2, 'Mouse', 50, 1, '2023-02-02'],
        [103, 1, 'Monitor', 200, 2, '2023-02-03'],     # Alice bought something else
        [104, 3, 'Keyboard', -50, 1, '2023-02-04'],    # Error: Price < 0
        [105, 99, 'Cable', 10, 1, '2023-02-05'],       # Error: Non-existent user
        [106, 3, 'Headset', 100, 0, '2023-02-06']      # Error: Quantity 0
    ]
    with open(orders_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(orders_header)
        writer.writerows(orders_data)
    logging.info(f"Test data generated in {data_dir}")

def process_data(data_dir: Path) -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
    """Extracts and transforms data."""
    logging.info("Starting data processing...")
    users_file = data_dir / 'users.json'
    orders_file = data_dir / 'orders.csv'

    # 1. Read Users
    try:
        df_users = pd.read_json(users_file)
    except ValueError as e:
        logging.error(f"Error reading JSON: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error reading users: {e}")
        return None

    # Deduplication
    initial_users_count = len(df_users)
    if 'reg_date' in df_users.columns:
        df_users['reg_date'] = pd.to_datetime(df_users['reg_date'])
        df_users = df_users.sort_values('reg_date').drop_duplicates(subset='user_id', keep='last')
    
    dropped_users = initial_users_count - len(df_users)
    logging.info(f"   -> Users loaded: {len(df_users)} (Dropped duplicates: {dropped_users})")

    # 2. Read Orders
    try:
        df_orders = pd.read_csv(orders_file)
    except Exception as e:
        logging.error(f"Error reading CSV: {e}")
        return None
    
    initial_orders_count = len(df_orders)
    
    # Filter valid prices and quantities
    invalid_price_mask = df_orders['item_price'] <= 0
    invalid_qty_mask = df_orders['quantity'] <= 0
    
    dropped_price = invalid_price_mask.sum()
    dropped_qty = invalid_qty_mask.sum()
    
    df_orders = df_orders[~invalid_price_mask & ~invalid_qty_mask]
    
    # Referential integrity check
    valid_user_ids = df_users['user_id'].unique()
    unknown_user_mask = ~df_orders['user_id'].isin(valid_user_ids)
    dropped_unknown_user = unknown_user_mask.sum()
    
    df_orders = df_orders[~unknown_user_mask]
    
    # Date formatting
    if 'order_date' in df_orders.columns:
        df_orders['order_date'] = pd.to_datetime(df_orders['order_date'])

    # Add user_name to orders for DB visibility
    user_map = df_users.set_index('user_id')['name']
    df_orders['user_name'] = df_orders['user_id'].map(user_map)

    # Reorder columns to match desired DB structure
    df_orders = df_orders[['order_id', 'user_id', 'user_name', 'item_name', 'item_price', 'quantity', 'order_date']]

    logging.info(f"   -> Orders loaded: {len(df_orders)} (Total dropped: {initial_orders_count - len(df_orders)})")
    logging.info(f"      - Invalid Price: {dropped_price}")
    logging.info(f"      - Invalid Quantity: {dropped_qty}")
    logging.info(f"      - Unknown User: {dropped_unknown_user}")
    
    return df_users, df_orders

def load_to_db(df_users: pd.DataFrame, df_orders: pd.DataFrame, db_name: str) -> None:
    """Loads data into SQLite database."""
    try:
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()

            # DDL
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT,
                reg_date TIMESTAMP
            )
            ''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS orders (
                order_id INTEGER PRIMARY KEY,
                user_id INTEGER,
                user_name TEXT,
                item_name TEXT,
                item_price REAL,
                quantity INTEGER,
                order_date TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            )
            ''')
            
            # Load data
            df_users.to_sql('users', conn, if_exists='replace', index=False)
            df_orders.to_sql('orders', conn, if_exists='replace', index=False)
            
        logging.info(f"Data successfully loaded into {db_name}")
    except Exception as e:
        logging.error(f"Database error: {e}")

def get_ltv_report(db_name: str) -> None:
    """Generates LTV report from database."""
    
    sql_query = """
    SELECT 
        u.name as user_name,
        SUM(o.item_price * o.quantity) as total_spent,
        COUNT(o.order_id) as total_orders,
        MAX(o.order_date) as last_order_date
    FROM users u
    JOIN orders o ON u.user_id = o.user_id
    GROUP BY u.user_id, u.name
    ORDER BY total_spent DESC
    LIMIT 3;
    """
    
    print("\n--- TOP 3 CUSTOMERS (LTV Report) ---")
    try:
        with sqlite3.connect(db_name) as conn:
            report = pd.read_sql_query(sql_query, conn)
            if report.empty:
                print("No data found for report.")
            else:
                print(report.to_string(index=False))
    except Exception as e:
        logging.error(f"SQL Error: {e}")

def main():
    parser = argparse.ArgumentParser(description="ETL Project Demo")
    parser.add_argument("--generate", action="store_true", help="Generate dummy data")
    parser.add_argument("--no-etl", action="store_true", help="Skip ETL process (only report if DB exists)")
    parser.add_argument("--db", type=str, default=DEFAULT_DB_FILE, help="Database file path")
    parser.add_argument("--data-dir", type=str, default=str(DEFAULT_DATA_DIR), help="Data directory path")
    
    args = parser.parse_args()
    
    data_dir = Path(args.data_dir)
    
    # Always generate if data doesn't exist, or if forced
    if args.generate or not data_dir.exists():
        generate_dummy_data(data_dir)
    
    if not args.no_etl:
        result = process_data(data_dir)
        if result:
            users, orders = result
            load_to_db(users, orders, args.db)
    
    get_ltv_report(args.db)

if __name__ == "__main__":
    main()