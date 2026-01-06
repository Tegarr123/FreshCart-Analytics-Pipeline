from airflow.sdk import dag, task, chain
import sys
sys.path.append("/opt/airflow")
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from pendulum import datetime
from dlthub.dlt_ingest import *
from dlthub import dlt_ingest
import os
import psycopg2
import logging
@dag(
    dag_id="freshcart_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
)
def freshcart_pipeline():
    
    @task(task_id="get_db_source_connection")
    def get_db_source_connection():
        db_config = {
            "host":os.getenv("POSTGRES_HOST"),
            "port":os.getenv("POSTGRES_PORT"),
            "dbname":os.getenv("POSTGRES_DB"),
            "user":os.getenv("POSTGRES_USER"),
            "password":os.getenv("POSTGRES_PASSWORD")}
        return db_config

    @task(task_id="run_dlt_ingest")
    def run_dlt_ingest(ti):
        
        logging.info("Starting DLT pipeline execution")
        conn = psycopg2.connect(
            **ti.xcom_pull(task_ids="get_db_source_connection", key="return_value")
        )
        logging.info("Database connection established")
        
        product_categories_source = product_categories(conn=conn)
        stores_source = stores(conn=conn)
        products_source = products(conn=conn)
        promotions_source = promotions(conn=conn)
        customers_source = customers(conn=conn)
        sales_transactions_source = sales_transactions(conn=conn)
        sales_transaction_items_source = sales_transaction_items(conn=conn)
        
        pipeline = dlt.pipeline(
            pipeline_name="freshcart",
            destination="snowflake",
            dataset_name="bronze",
        )
        
        logging.info("DLT pipeline initialized")
        load_info = pipeline.run([product_categories_source, 
                  stores_source, 
                  products_source, 
                  promotions_source, 
                  customers_source, 
                  sales_transactions_source, 
                  sales_transaction_items_source]
                 )
        for package in load_info.load_packages:
            for table_name, table in package.schema_update.items():
                print(f"Table {table_name}: {table.get('description')}")
                for column_name, column in table["columns"].items():
                    print(f"\tcolumn {column_name}: {column['data_type']}")
        logging.info(f"Product Categories Loaded (always full loading): {dlt_ingest.product_categories_loaded}")
        logging.info(f"Stores Loaded (always full loading): {dlt_ingest.stores_loaded}")
        logging.info(f"Products Loaded: {dlt_ingest.products_loaded}")
        logging.info(f"Promotions Loaded (always full loading): {dlt_ingest.promotions_loaded}")
        logging.info(f"Customers Loaded: {dlt_ingest.customers_loaded}")
        logging.info(f"Sales Transactions Loaded: {dlt_ingest.sales_transactions_loaded}")
        logging.info(f"Sales Transaction Items Loaded: {dlt_ingest.sales_transaction_items_loaded}")

    
    chain(
        EmptyOperator(task_id="start"), 
        get_db_source_connection(),
        run_dlt_ingest(),
        BashOperator(
            task_id="dbt_source_freshness",
            bash_command="""
            cd /opt/airflow/dbt &&
            dbt source freshness
            """,
        ), 
        BashOperator(
            task_id="dbt_run_staging",
            bash_command="""
            cd /opt/airflow/dbt &&
            dbt run --select +path:models/staging
            """,
        ),
        BashOperator(
            task_id="dbt_run_ints_processing",
            bash_command="""
            cd /opt/airflow/dbt &&
            dbt run --select +path:models/intermediate
            """,
        ),
        BashOperator(
            task_id="dbt_run_marts",
            bash_command="""
            cd /opt/airflow/dbt &&
            dbt run --select +path:models/marts
            """,
        ),
        BashOperator(
            task_id="dbt_run_quality_tests",
            bash_command="""
            cd /opt/airflow/dbt &&
            dbt test
            """,
        ),
        EmptyOperator(task_id="end")
    )

freshcart_pipeline()
    