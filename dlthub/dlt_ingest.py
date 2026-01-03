from dotenv import load_dotenv
import dlt
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import argparse

product_categories_loaded = 0
stores_loaded = 0
products_loaded = 0
promotions_loaded = 0
customers_loaded = 0
sales_transactions_loaded = 0
sales_transaction_items_loaded = 0
@dlt.resource(
    name="product_categories",
    write_disposition="replace",
)
def product_categories(conn):
    global product_categories_loaded
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        query = """
            SELECT category_id, category_name
            FROM product_categories
            ORDER BY category_id ASC
        """
        cursor.execute(query)
        for row in cursor:
            yield row
            product_categories_loaded += 1

@dlt.resource(
    name="stores",
    write_disposition="replace",
)
def stores(conn):
    global stores_loaded
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        query = """
            SELECT store_id, store_name, store_type
            FROM stores
            ORDER BY store_id ASC
        """
        cursor.execute(query)
        for row in cursor:
            yield row
            stores_loaded += 1

@dlt.resource(
    name="products",
    write_disposition="append"
)
def products(conn, updated_at=dlt.sources.incremental("updated_at")):
    global products_loaded
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        query = """
            SELECT product_id, sku, name, category_id, current_base_price, updated_at
            FROM products
            WHERE updated_at > %s
            ORDER BY updated_at ASC
        """
        cursor.execute(query, (updated_at.last_value or "1970-01-01",))
        for row in cursor:
            yield row
            products_loaded += 1

@dlt.resource(
    name="promotions",
    write_disposition="replace"
)
def promotions(conn):
    global promotions_loaded
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        query = """
            SELECT promotion_id, 
                promo_code,
                promo_name,
                start_date,
                end_date, 
                d_type,
                discount_value
            FROM promotions
            ORDER BY promotion_id ASC
        """
        cursor.execute(query)
        for row in cursor:
            yield row
            promotions_loaded += 1

@dlt.resource(
    name="customers",
    write_disposition="append"
)
def customers(conn, updated_at=dlt.sources.incremental("updated_at")):
    global customers_loaded
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        query = """
            SELECT customer_id, first_name, last_name, email, loyalty_tier, updated_at
            FROM customers
            WHERE updated_at > %s
            ORDER BY updated_at ASC
        """
        cursor.execute(query, (updated_at.last_value or "1970-01-01",))
        for row in cursor:
            yield row
            customers_loaded += 1

@dlt.resource(
    name="sales_transactions",
    write_disposition="append"
)
def sales_transactions(conn, updated_at=dlt.sources.incremental("updated_at")):
    global sales_transactions_loaded
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        query = """
            SELECT 
                transaction_id,
                store_id, 
                customer_id, 
                total_amount, 
                tax_amount,
                status,
                created_at, 
                updated_at
            FROM sales_transactions
            WHERE updated_at > %s
            ORDER BY updated_at ASC
        """
        cursor.execute(query, (updated_at.last_value or "1970-01-01",))
        for row in cursor:
            yield row
            sales_transactions_loaded += 1

@dlt.resource(
    name="sales_transaction_items",
    write_disposition="append")
def sales_transaction_items(conn, updated_at=dlt.sources.incremental("updated_at")):
    global sales_transaction_items_loaded
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        query = """
            SELECT 
                item_id,
                transaction_id, 
                product_id, 
                promotion_id,
                quantity, 
                unit_price_at_sale,
                discount_applied,
                line_total, 
                updated_at
            FROM sales_transaction_items
            WHERE updated_at > %s
            ORDER BY updated_at ASC
        """
        cursor.execute(query, (updated_at.last_value or "1970-01-01",))
        for row in cursor:
            yield row
            sales_transaction_items_loaded += 1
            
if __name__ == "__main__":
    load_dotenv()
    conn = psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            port=os.environ["POSTGRES_PORT"],
            dbname=os.environ["POSTGRES_DATABASE"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )

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

    parser = argparse.ArgumentParser(description="DLT Ingest Script")
    parser.add_argument("--refresh", action="store_true", help="Refresh the data by dropping sources")
    args = parser.parse_args()
    
    if args.refresh:
        refresh_option = "drop_sources"
    else:
        refresh_option = None
        
    load_info = pipeline.run([product_categories_source, 
                  stores_source, 
                  products_source, 
                  promotions_source, 
                  customers_source, 
                  sales_transactions_source, 
                  sales_transaction_items_source], 
                refresh=refresh_option
                 )
    for package in load_info.load_packages:
        for table_name, table in package.schema_update.items():
            print(f"Table {table_name}: {table.get('description')}")
            for column_name, column in table["columns"].items():
                print(f"\tcolumn {column_name}: {column['data_type']}")
    print("Metrics:")
    print(f"Product Categories Loaded: {product_categories_loaded}")
    print(f"Stores Loaded: {stores_loaded}")
    print(f"Products Loaded: {products_loaded}")
    print(f"Promotions Loaded: {promotions_loaded}")
    print(f"Customers Loaded: {customers_loaded}")
    print(f"Sales Transactions Loaded: {sales_transactions_loaded}")
    print(f"Sales Transaction Items Loaded: {sales_transaction_items_loaded}")