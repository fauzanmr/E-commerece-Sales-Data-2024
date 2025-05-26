import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
import psycopg2
import kagglehub

# Load environment variables
load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Global connection setup
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
conn.autocommit = True
cur = conn.cursor()

def create_tables():
    ddl_statements = [
        "DROP TABLE IF EXISTS sales_fact, customer_dim, product_dim CASCADE;",
        """
        CREATE TABLE customer_dim (
            customer_id TEXT PRIMARY KEY,
            customer_name TEXT,
            gender TEXT,
            region TEXT
        );
        """,
        """
        CREATE TABLE product_dim (
            product_id TEXT PRIMARY KEY,
            product_name TEXT,
            category TEXT,
            price NUMERIC
        );
        """,
        """
        CREATE TABLE sales_fact (
            order_id TEXT PRIMARY KEY,
            product_id TEXT,
            customer_id TEXT,
            quantity INTEGER,
            price NUMERIC,
            total_sales NUMERIC,
            date DATE,
            FOREIGN KEY (product_id) REFERENCES product_dim(product_id),
            FOREIGN KEY (customer_id) REFERENCES customer_dim(customer_id)
        );
        """,
        "CREATE INDEX idx_sales_fact_date ON sales_fact(date);",
        "CREATE INDEX idx_sales_fact_product_id ON sales_fact(product_id);",
        "CREATE INDEX idx_sales_fact_customer_id ON sales_fact(customer_id);",
        "CREATE INDEX idx_customer_region_gender ON customer_dim(region, gender);"
    ]

    print("🧠 Creating tables and indexes...")
    for stmt in ddl_statements:
        cur.execute(stmt)
    print(" Tables and indexes created.")

def extract_and_transform():
    dataset_path = kagglehub.dataset_download("datascientist97/e-commerece-sales-data-2024")
    file_map = {
        "customer_details.csv": "customer_dim",
        "product_details.csv": "product_dim",
        "E-commerece sales data 2024.csv": "sales_fact"
    }

    for filename, table in file_map.items():
        filepath = os.path.join(dataset_path, filename)
        df = pd.read_csv(filepath)

        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        df = df.drop_duplicates()

        if table == "customer_dim":
            df = df.dropna(subset=["customer_id", "customer_name"])
            df = df.astype({"customer_id": str})

        elif table == "product_dim":
            df = df.dropna(subset=["product_id", "product_name", "price"])
            df["price"] = df["price"].replace("[\\$,]", "", regex=True).astype(float)
            df = df.astype({"product_id": str})

        elif table == "sales_fact":
            df = df.dropna(subset=["order_id", "product_id", "customer_id", "price", "quantity", "date"])
            df["price"] = df["price"].replace("[\\$,]", "", regex=True).astype(float)
            df["quantity"] = df["quantity"].astype(int)
            df["total_sales"] = df["price"] * df["quantity"]
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.dropna(subset=["date"])
            df = df.astype({"order_id": str, "product_id": str, "customer_id": str})

        df.to_sql(table, engine, if_exists="append", index=False)
        print(f"✅ Loaded {table} ({len(df)} rows)")

def create_views():
    cur.execute("DROP MATERIALIZED VIEW IF EXISTS monthly_sales_summary;")
    cur.execute("""
    CREATE MATERIALIZED VIEW monthly_sales_summary AS
    SELECT 
        DATE_TRUNC('month', date) AS month,
        SUM(total_sales) AS total_monthly_sales
    FROM sales_fact
    GROUP BY month
    ORDER BY month;
    """)
    print("Created materialized view: monthly_sales_summary")

    cur.execute("DROP MATERIALIZED VIEW IF EXISTS customer_demographics_summary;")
    cur.execute("""
    CREATE MATERIALIZED VIEW customer_demographics_summary AS
    SELECT 
        region,
        gender,
        COUNT(*) AS customer_count
    FROM customer_dim
    GROUP BY region, gender
    ORDER BY region, gender;
    """)
    print("Created materialized view: customer_demographics_summary")

def run_etl():
    create_tables()
    extract_and_transform()
    create_views()
    cur.close()
    conn.close()
    print("ETL process complete.")

if __name__ == "__main__":
    run_etl()
