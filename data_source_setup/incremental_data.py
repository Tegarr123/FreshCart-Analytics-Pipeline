import psycopg2
from psycopg2 import sql
import pandas as pd
import psycopg2
from decimal import Decimal
import random
from faker import Faker
import uuid
from datetime import datetime, timedelta

def get_random_datetime():
    # start_date = datetime.now()
    start_date = datetime.now() + timedelta(days=360)
    end_date = start_date + timedelta(days=360)
    
    time_between = end_date - start_date
    random_days = random.randrange(time_between.days)
    random_seconds = random.randrange(86400)
    
    return start_date + timedelta(days=random_days, seconds=random_seconds)
DB_CONFIG = {
    'dbname': 'freshcart',
    'user':  'fadhinotgr123',
    'password': 'f@dh1n0TGR1234',
    'host': '127.0.0.1',
    'port': '5432'
}

from psycopg2.extras import register_uuid
def calculate_discount(promotion, unit_price, quantity):
    """Calculate discount based on promotion type"""
    if not promotion:
        return Decimal('0.00')
    
    discount_type = promotion['d_type']
    discount_value = promotion['discount_value']
    
    if discount_type == 'PERCENTAGE':
        # Percentage discount on line total
        line_subtotal = unit_price * Decimal(str(quantity))
        discount = (line_subtotal * discount_value / Decimal('100')).quantize(Decimal('0.01'))
    elif discount_type == 'FIXED_AMOUNT':
        # Fixed amount discount per item
        discount = (discount_value * Decimal(str(quantity))).quantize(Decimal('0.01'))
    else:
        discount = Decimal('0.00')
    
    return discount
# Initialize Faker
fake = Faker('id_ID')

# Store IDs (based on your insert statement)
STORE_IDS = [1, 2, 3, 4, 5]

# Transaction statuses (matching your ENUM)
TRANSACTION_STATUSES = ['COMPLETED', 'VOIDED', 'RETURNED']

# Tax rate (11% PPN Indonesia)
TAX_RATE = Decimal('0.11')

conn = None
cur = None

try:
    # Connect to database
    conn = psycopg2.connect(**DB_CONFIG)
    
    # Register UUID adapter
    register_uuid()
    
    cur = conn.cursor()
    
    # Get all customer IDs
    cur.execute("SELECT customer_id FROM customers")
    customer_ids = [row[0] for row in cur.fetchall()]
    
    # Get all product IDs and prices
    cur.execute("SELECT product_id, current_base_price FROM products")
    products = {row[0]: row[1] for row in cur.fetchall()}
    product_ids = list(products.keys())
    
    # Get all promotions with their details
    cur.execute("SELECT promotion_id, d_type, discount_value FROM promotions")
    promotions_data = {}
    for row in cur.fetchall():
        promotions_data[row[0]] = {
            'promotion_id': row[0],
            'd_type': row[1],
            'discount_value': row[2]
        }
    
    promotion_ids = list(promotions_data.keys())
    promotion_ids.append(None)  # Allow transactions without promotions
    
    print(f"📊 Found {len(customer_ids)} customers, {len(product_ids)} products, {len(promotions_data)} promotions")
    
    # Prepare insert queries
    transaction_query = """
    INSERT INTO sales_transactions 
    (transaction_id, store_id, customer_id, total_amount, tax_amount, status, created_at, updated_at)
    VALUES (%s, %s, %s, %s, %s, %s:: transaction_status, %s, %s)
    """
    
    item_query = """
    INSERT INTO sales_transaction_items 
    (transaction_id, product_id, promotion_id, quantity, unit_price_at_sale, discount_applied, line_total, updated_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    transactions = []
    all_items = []
    
    print("\n🔄 Generating 1000 transactions with items...")
    
    for i in range(213):
        # Generate transaction data
        transaction_id = uuid.uuid4()
        store_id = random.choice(STORE_IDS)
        
        # 85% chance of having a customer, 15% anonymous
        customer_id = random.choice(customer_ids) if random.random() < 0.85 else None

        # Status distribution:  90% COMPLETED, 3% VOIDED, 7% RETURNED
        status = random.choices(
            TRANSACTION_STATUSES,
            weights=[90, 3, 7],
            k=1
        )[0]
        
        created_at = get_random_datetime()
        updated_at = created_at
        
        # Generate 1-8 items per transaction
        num_items = random.randint(1, 8)
        transaction_total_before_tax = Decimal('0.00')
        transaction_items = []
        
        for _ in range(num_items):
            product_id = random.choice(product_ids)
            quantity = random.randint(1, 5)
            unit_price_at_sale = products[product_id]
            
            # 30% chance of having a promotion
            promotion_id = random.choice(promotion_ids) if random.random() < 0.30 else None
            
            # Calculate discount
            promotion = promotions_data.get(promotion_id) if promotion_id else None
            discount_applied = calculate_discount(promotion, unit_price_at_sale, quantity)
            
            # Calculate line total (quantity * unit_price - discount)
            line_subtotal = Decimal(str(quantity)) * unit_price_at_sale
            line_total = line_subtotal - discount_applied
            
            transaction_items.append({
                'transaction_id': transaction_id,
                'product_id': product_id,
                'promotion_id': promotion_id,
                'quantity': quantity,
                'unit_price_at_sale': unit_price_at_sale,
                'discount_applied': discount_applied,
                'line_total': line_total,
                'updated_at': created_at
            })
            
            transaction_total_before_tax += line_total
        
        # Calculate tax (11% PPN)
        tax_amount = (transaction_total_before_tax * TAX_RATE).quantize(Decimal('0.01'))
        total_amount = transaction_total_before_tax + tax_amount
        
        transactions.append((
            transaction_id,
            store_id,
            customer_id,
            total_amount,
            tax_amount,
            status,
            created_at,
            updated_at
        ))
        
        # Add items to all_items list
        for item in transaction_items:
            all_items.append((
                item['transaction_id'],
                item['product_id'],
                item['promotion_id'],
                item['quantity'],
                item['unit_price_at_sale'],
                item['discount_applied'],
                item['line_total'],
                item['updated_at']
            ))
        
        # Progress indicator
        if (i + 1) % 100 == 0:
            print(f"  ✓ Generated {i + 1}/1000 transactions...")
    
    print("\n💾 Inserting transactions into database...")
    cur.executemany(transaction_query, transactions)
    
    print("💾 Inserting transaction items into database...")
    cur.executemany(item_query, all_items)
    
    # Commit the transaction
    conn.commit()
    
    # Get statistics
    cur.execute("SELECT COUNT(*) FROM sales_transactions")
    transaction_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM sales_transaction_items")
    item_count = cur.fetchone()[0]
    
    print(f"\n✅ Successfully inserted {len(transactions)} transactions!")
    print(f"✅ Successfully inserted {len(all_items)} transaction items!")
    print(f"📊 Total transactions in database:  {transaction_count}")
    print(f"📊 Total transaction items in database:  {item_count}")
    print(f"📊 Average items per transaction: {len(all_items)/len(transactions):.2f}")
    
    # Display status distribution
    cur.execute("""
        SELECT status, COUNT(*) as count, 
                SUM(total_amount) as total_revenue
        FROM sales_transactions
        GROUP BY status
        ORDER BY count DESC
    """)
    
    print("\n📈 Transaction Status Distribution:")
    print("-" * 70)
    for row in cur.fetchall():
        print(f"{row[0]:12} :  {row[1]:4} transactions | Total: Rp {row[2]:>15,.2f}")

    # Display discount statistics
    cur.execute("""
        SELECT 
            COUNT(*) as items_with_discount,
            SUM(discount_applied) as total_discounts,
            AVG(discount_applied) as avg_discount
        FROM sales_transaction_items
        WHERE discount_applied > 0
    """)
    
    discount_stats = cur.fetchone()
    if discount_stats[0] > 0:
        print("\n💰 Discount Statistics:")
        print("-" * 70)
        print(f"Items with discount  : {discount_stats[0]:4}")
        print(f"Total discounts given:  Rp {discount_stats[1]:>15,.2f}")
        print(f"Average discount     :  Rp {discount_stats[2]:>15,.2f}")
    
    # Display store distribution
    cur.execute("""
        SELECT s.store_name, COUNT(*) as transaction_count,
                SUM(st.total_amount) as total_revenue
        FROM sales_transactions st
        JOIN stores s ON st.store_id = s.store_id
        GROUP BY s.store_name
        ORDER BY transaction_count DESC
    """)
    
    print("\n🏪 Store Performance:")
    print("-" * 80)
    for row in cur.fetchall():
        print(f"{row[0]:45} : {row[1]:4} txns | Rp {row[2]:>15,.2f}")

    # Display sample transactions
    cur.execute("""
        SELECT st.transaction_id, s.store_name, 
                c.first_name || ' ' || c. last_name as customer,
                st.total_amount, st.status, st.created_at
        FROM sales_transactions st
        JOIN stores s ON st.store_id = s.store_id
        LEFT JOIN customers c ON st.customer_id = c.customer_id
        ORDER BY st. created_at DESC
        LIMIT 5
    """)
    
    print("\n🧾 Sample of Recent Transactions:")
    print("-" * 120)
    for row in cur.fetchall():
        customer = row[2] if row[2] else "Anonymous"
        print(f"ID: {str(row[0])[:8]}... | {row[1]:30} | {customer:25} | Rp {row[3]:>12,.2f} | {row[4]:10} | {row[5]}")
    
except psycopg2.Error as e:
    print(f"❌ Database error: {e}")
    if conn:
        conn.rollback()
except Exception as e:
    print(f"❌ Error:  {e}")
    import traceback
    traceback.print_exc()
finally:
    if cur:
        cur.close()
    if conn:
        conn.close()
        
# # Update and Insert new stores    # 
# conn = psycopg2.connect(**DB_CONFIG)
    

# cur = conn.cursor()

# query_1 = """
#     UPDATE stores
#     SET store_type = 'Convenience'
#     WHERE store_id = 3;
#     INSERT INTO stores (store_name, store_type)
#     VALUES ('FreshCart Supercenter - South', 'Supercenter');
# """

# cur.execute(query_1)
# cur.close()
# conn.commit()