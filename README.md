# E-Commerce Sales Data ETL Pipeline

This repository contains a Python-based ETL pipeline that extracts e-commerce sales data from Kaggle, performs data cleaning and transformation, and loads the data into a PostgreSQL data warehouse. The pipeline also creates materialized views for analytical queries.

An Apache Airflow DAG is provided to run the ETL pipeline autonomously on a daily schedule.

---

## Features

- Extract CSV files from Kaggle dataset
- Data cleaning: remove duplicates, handle missing values
- Data transformation: normalize dates, currencies, calculate total sales
- Load data into PostgreSQL with optimized schema (star schema)
- Create indexes and materialized views for performance
- Airflow DAG to orchestrate ETL tasks (create tables, load data, create views)

---

## Prerequisites

- Python 3.8+
- PostgreSQL server running and accessible
- Apache Airflow installed and configured
- Kagglehub package installed (`pip install kagglehub`)
- `python-dotenv` package (`pip install python-dotenv`)
- Other dependencies: pandas, sqlalchemy, psycopg2-binary

---

## Setup Instructions

1. Clone the repository
git clone https://your-repo-url.git
cd your-repo

2. Create and configure .env file
Create a .env file at the project root with your PostgreSQL credentials:

3. Install Python dependencies

Use the provided requirements.txt:
pip install -r requirements.txt

If you don’t have requirements.txt, manually install:
pip install pandas kagglehub python-dotenv sqlalchemy psycopg2-binary apache-airflow

4. Run ETL script manually (optional)

To verify the ETL script works before Airflow setup:
python main.py

This will:

Create necessary tables and indexes

Extract, clean, transform, and load data

Create materialized views for reporting

## Airflow Integration

1. Place your ETL script and DAG
Put main.py in a folder (e.g., /path/to/etl/)

Place the Airflow DAG script (ecommerce_etl_dag.py) inside your Airflow DAGs directory

2. Update DAG import path

In ecommerce_etl_dag.py, update this line to point to the ETL script folder:
sys.path.insert(0, '/path/to/your/etl_script_directory')

3. Start Airflow services

airflow scheduler
airflow webserver

4. Trigger the DAG
Open the Airflow UI (usually at http://localhost:8080), find the ecommerce_etl_dag, and trigger it manually or wait for scheduled runs.

## Data Warehouse Schema
- customer_dim: customer details (customer_id, name, gender, region)
- product_dim: product info (product_id, name, category, price)
- sales_fact: sales transactions with foreign keys to customer and product, quantity, price, total_sales, date

Indexes created for efficient filtering by date, product, customer, region, and gender.

Materialized views for:
- Monthly total sales (monthly_sales_summary)
- Customer demographics aggregation (customer_demographics_summary)

## Project Structure Example

my_project/
│
├── etl/
│   └── main.py
│
├── airflow_dags/
│   └── ecommerce_etl_dag.py
│
├── .env
├── requirements.txt
└── README.md

## Troubleshooting

- PostgreSQL connection errors: Verify .env values, ensure PostgreSQL server is running.
- Import errors in DAG: Confirm sys.path.insert in DAG points to correct ETL script folder.
- Kagglehub errors: Confirm network access and Kaggle credentials if needed.
- Airflow issues: Check logs under Airflow UI or filesystem for detailed errors.

Contributions
Feel free to open issues or submit pull requests to improve the project!

Contact
For questions or help, contact: fauzanmrabbani@gmail.com