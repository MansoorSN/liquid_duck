import duckdb
import pytest


@pytest.fixture(scope="module")
def duckdb_connection():
    """Fixture to set up a DuckDB connection for testing."""
    connection = duckdb.connect(database='test_duck.db', read_only=False)
    yield connection
    connection.close()


def test_duckdb_table_creation(duckdb_connection):
    """Test if DuckDB tables are created successfully."""
    duckdb_connection.execute('''
        CREATE OR REPLACE TABLE products (
            product_id INTEGER,
            supplier VARCHAR(255),
            brand VARCHAR(255),
            family VARCHAR(255),
            product_name VARCHAR(255),
            product_cost DECIMAL(10,2),
            inventory_volume INTEGER
        );
    ''')

    duckdb_connection.execute('''
        CREATE OR REPLACE TABLE customers (
            customer_id INTEGER,
            customer_name VARCHAR(255),
            customer_address VARCHAR(255),
            customer_phone VARCHAR(255)
        );
    ''')

    duckdb_connection.execute('''
        CREATE OR REPLACE TABLE sales (
            sale_id INTEGER,
            product_id INTEGER,
            customer_id INTEGER,
            sale_date DATE,
            sale_volume INTEGER,
            sale_revenue DECIMAL(10,2)
        );
    ''')

    tables = duckdb_connection.execute("SHOW TABLES").fetchall()
    assert ('products',) in tables
    assert ('customers',) in tables
    assert ('sales',) in tables
