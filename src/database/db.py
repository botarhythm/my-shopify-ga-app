import duckdb
import os
from src.config.settings import settings

class Database:
    def __init__(self):
        self.db_path = settings.DB_PATH

    def get_connection(self, read_only=False):
        """Get a connection to the DuckDB database."""
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            con = duckdb.connect(self.db_path, read_only=read_only)
            return con
        except Exception as e:
            print(f"Error connecting to database at {self.db_path}: {e}")
            raise

    def init_db(self):
        """Initialize the database schema."""
        con = self.get_connection()
        # Create schema if not exists
        con.execute("CREATE SCHEMA IF NOT EXISTS raw_shopify;")
        con.execute("CREATE SCHEMA IF NOT EXISTS raw_square;")
        con.execute("CREATE SCHEMA IF NOT EXISTS raw_ga4;")
        con.execute("CREATE SCHEMA IF NOT EXISTS raw_ads;")
        con.execute("CREATE SCHEMA IF NOT EXISTS core;")
        con.execute("CREATE SCHEMA IF NOT EXISTS marts;")
        con.close()
        print("Database schemas initialized.")

db = Database()
