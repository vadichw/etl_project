import pytest
import pandas as pd
import sqlite3
import os
import sys
from pathlib import Path

# Add project root to sys.path to import main
sys.path.append(str(Path(__file__).parent.parent))

from main import process_data, load_to_db, get_ltv_report, USERS_FILE, ORDERS_FILE

# Mock data for testing
@pytest.fixture
def setup_test_data(tmp_path):
    """Creates temporary test data files."""
    # Override global constants in main (monkeypatching would be better but simple assignment works for this scope if we are careful, 
    # but since we import constants, we need to patch the files where they are used or just overwrite the files if they are paths.
    # However, main.py uses global constants. Let's use the actual files for simplicity or better, mock the file reading.
    # Given the structure, let's just create the actual files in the data directory for the test, 
    # but wait, the script uses hardcoded 'data/' dir. 
    # Let's rely on the generate_dummy_data logic but strictly control the input for tests.
    
    # Actually, process_data reads from USERS_FILE and ORDERS_FILE. 
    # We should probably mock these paths or just overwrite the files with known test data.
    # Since this is a simple project, let's overwrite the files with specific test cases.
    
    # 1. Users
    users = [
        {"user_id": 1, "name": "Alice", "email": "alice@example.com", "reg_date": "2023-01-01"},
        {"user_id": 1, "name": "Alice", "email": "alice_new@example.com", "reg_date": "2023-01-10"}, # Duplicate, newer
        {"user_id": 2, "name": "Bob", "email": "bob@example.com", "reg_date": "2023-01-05"}
    ]
    pd.DataFrame(users).to_json(USERS_FILE, orient='records')

    # 2. Orders
    orders_data = [
        [101, 1, 'Laptop', 1000, 1, '2023-02-01'], # Valid
        [102, 2, 'Mouse', 50, 1, '2023-02-02'],    # Valid
        [103, 1, 'BadPrice', -100, 1, '2023-02-03'], # Invalid Price
        [104, 1, 'BadQty', 100, 0, '2023-02-04'],    # Invalid Qty
        [105, 99, 'Ghost', 10, 1, '2023-02-05']      # Non-existent user
    ]
    orders_df = pd.DataFrame(orders_data, columns=['order_id', 'user_id', 'item_name', 'item_price', 'quantity', 'order_date'])
    orders_df.to_csv(ORDERS_FILE, index=False)

    yield

    # Cleanup (optional, maybe leave for inspection)
    pass

def test_process_data(setup_test_data):
    """Test data cleaning and transformation logic."""
    users, orders = process_data()

    # 1. Check User Deduplication
    assert len(users) == 2, "Should have 2 unique users"
    alice = users[users['user_id'] == 1].iloc[0]
    assert alice['email'] == "alice_new@example.com", "Should keep the latest record for Alice"

    # 2. Check Order Filtering
    assert len(orders) == 2, "Should have 2 valid orders (101, 102)"
    assert 103 not in orders['order_id'].values, "Negative price should be filtered"
    assert 104 not in orders['order_id'].values, "Zero quantity should be filtered"
    assert 105 not in orders['order_id'].values, "Non-existent user order should be filtered"

def test_load_to_db(setup_test_data, tmp_path):
    """Test loading data into SQLite."""
    users, orders = process_data()
    
    # Use a temporary file DB because :memory: is lost when connection closes
    db_file = tmp_path / "test_db.sqlite"
    db_name = str(db_file)
    
    load_to_db(users, orders, db_name)
    
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Check tables exist
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    assert 'users' in tables
    assert 'orders' in tables
    
    # Check data count
    cursor.execute("SELECT COUNT(*) FROM users")
    assert cursor.fetchone()[0] == 2
    
    cursor.execute("SELECT COUNT(*) FROM orders")
    assert cursor.fetchone()[0] == 2
    
    conn.close()

def test_ltv_report(setup_test_data, capsys, tmp_path):
    """Test LTV report generation (integration test)."""
    users, orders = process_data()
    db_file = tmp_path / "test_shop.db"
    db_name = str(db_file)
    
    load_to_db(users, orders, db_name)
    get_ltv_report(db_name)
    
    captured = capsys.readouterr()
    output = captured.out
    
    # Check if Alice (1000) and Bob (50) are in output
    assert "Alice" in output
    assert "1000" in output
    assert "Bob" in output
    assert "50" in output

