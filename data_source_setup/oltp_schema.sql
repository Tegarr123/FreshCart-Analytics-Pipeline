CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TYPE transaction_status AS ENUM ('COMPLETED', 'VOIDED', 'RETURNED');
CREATE TYPE discount_type AS ENUM ('PERCENTAGE', 'FIXED_AMOUNT');

-- Product Categories (Hierarchical)
CREATE TABLE product_categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Products
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    sku VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    category_id INT REFERENCES product_categories(category_id),
    current_base_price DECIMAL(12, 2) NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Stores
CREATE TABLE stores (
    store_id SERIAL PRIMARY KEY,
    store_name VARCHAR(100) NOT NULL,
    store_type VARCHAR(50), -- e.g., 'Express', 'Supercenter'
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Promotions
CREATE TABLE promotions (
    promotion_id SERIAL PRIMARY KEY,
    promo_code VARCHAR(50) UNIQUE NOT NULL,
    promo_name VARCHAR(255),
    start_date TIMESTAMPTZ NOT NULL,
    end_date TIMESTAMPTZ NOT NULL,
    d_type discount_type NOT NULL,
    discount_value DECIMAL(12, 2) NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Customers (Optional for Loyalty Tracking)
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255) UNIQUE,
    loyalty_tier VARCHAR(20) DEFAULT 'BRONZE',
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);


-- Sales Transactions (Header)
CREATE TABLE sales_transactions (
    transaction_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    store_id INT NOT NULL REFERENCES stores(store_id),
    customer_id INT REFERENCES customers(customer_id),
    total_amount DECIMAL(15, 2) NOT NULL DEFAULT 0.00,
    tax_amount DECIMAL(15, 2) NOT NULL DEFAULT 0.00,
    status transaction_status DEFAULT 'COMPLETED',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Sales Transaction Items (Line Items)
CREATE TABLE sales_transaction_items (
    item_id BIGSERIAL PRIMARY KEY,
    transaction_id UUID NOT NULL REFERENCES sales_transactions(transaction_id) ON DELETE CASCADE,
    product_id INT NOT NULL REFERENCES products(product_id),
    promotion_id INT REFERENCES promotions(promotion_id),
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price_at_sale DECIMAL(12, 2) NOT NULL, -- Snapshot of price at time of sale
    discount_applied DECIMAL(12, 2) DEFAULT 0.00,
    line_total DECIMAL(15, 2) NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);


CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_products BEFORE UPDATE ON products FOR EACH ROW EXECUTE FUNCTION update_timestamp();
CREATE TRIGGER trigger_update_transactions BEFORE UPDATE ON sales_transactions FOR EACH ROW EXECUTE FUNCTION update_timestamp();
CREATE TRIGGER trigger_update_transaction_items BEFORE UPDATE ON sales_transaction_items FOR EACH ROW EXECUTE FUNCTION update_timestamp();

INSERT INTO product_categories (category_name, updated_at) 
VALUES 
    ('Fresh Produce', CURRENT_TIMESTAMP - interval '3' year),          -- ID 1 (Fruits & Veggies)
    ('Dairy & Eggs', CURRENT_TIMESTAMP - interval '3' year),           -- ID 2 (Milk, Cheese, Butter)
    ('Bakery', CURRENT_TIMESTAMP - interval '3' year),                 -- ID 3 (Bread, Pastries)
    ('Meat & Seafood', CURRENT_TIMESTAMP - interval '3' year),         -- ID 4 (Chicken, Beef, Fish)
    ('Pantry Staples', CURRENT_TIMESTAMP - interval '3' year),         -- ID 5 (Flour, Sugar, Oil)
    ('Beverages', CURRENT_TIMESTAMP - interval '3' year),              -- ID 6 (Soda, Juice, Coffee)
    ('Frozen Foods', CURRENT_TIMESTAMP - interval '3' year),           -- ID 7 (Ice Cream, Pizza)
    ('Snacks & Sweets', CURRENT_TIMESTAMP - interval '3' year),        -- ID 8 (Chips, Chocolate)
    ('Personal Care', CURRENT_TIMESTAMP - interval '3' year),          -- ID 9 (Shampoo, Soap)
    ('Household Supplies', CURRENT_TIMESTAMP - interval '3' year),     -- ID 10 (Detergent, Paper Towels)
    ('Baby Care', CURRENT_TIMESTAMP - interval '3' year);              -- ID 11 (Diapers, Baby Food)

INSERT INTO stores (store_name, store_type, updated_at)
VALUES 
    ('FreshCart Supercenter - North', 'Supercenter', CURRENT_TIMESTAMP - interval '3' year),
    ('FreshCart Express - Central Station', 'Express', CURRENT_TIMESTAMP - interval '3' year),
    ('FreshCart Suburban - Westside', 'Supercenter', CURRENT_TIMESTAMP - interval '3' year),
    ('FreshCart Metro - Downtown', 'Urban', CURRENT_TIMESTAMP - interval '3' year),
    ('FreshCart Corner - East Village', 'Convenience', CURRENT_TIMESTAMP - interval '3' year);

INSERT INTO promotions (promo_code, promo_name, start_date, end_date, d_type, discount_value, updated_at)
VALUES 
    -- Global / Seasonal Promotions
    ('WINTER', 'Winter Clearance Sale', '2022-12-01', '2028-12-31', 'PERCENTAGE', 15.00, CURRENT_TIMESTAMP - interval '3' year),
    ('NEWYEAR', 'New Year Healthy Start', '2022-01-01', '2028-01-15', 'FIXED_AMOUNT', 5.00, CURRENT_TIMESTAMP - interval '3' year),
    ('FLASH10', 'Flash Friday Discount', '2022-12-26', '2028-12-27', 'PERCENTAGE', 10.00, CURRENT_TIMESTAMP - interval '3' year),

    ('MEAT5', 'Meat & Seafood Weekend', '2022-12-20', '2028-12-28', 'FIXED_AMOUNT', 5.00, CURRENT_TIMESTAMP - interval '3' year),
    ('VEGGIE20', 'Fresh Produce Blowout', '2022-12-15', '2028-12-31', 'PERCENTAGE', 20.00, CURRENT_TIMESTAMP - interval '3' year),

    ('BREAD1', 'Morning Bakery Special', '2022-12-01', '2028-01-31', 'FIXED_AMOUNT', 1.50, CURRENT_TIMESTAMP - interval '3' year),
    ('DAIRY10', 'Dairy Essentials Discount', '2022-12-10', '2028-12-30', 'PERCENTAGE', 10.00, CURRENT_TIMESTAMP - interval '3' year),

    ('SODA_SAVER', 'Beverage Bundle Discount', '2022-12-01', '2028-02-28', 'FIXED_AMOUNT', 2.00, CURRENT_TIMESTAMP - interval '3' year),
    ('SNACK_TIME', 'Weekend Snack Attack', '2022-12-01', '2028-12-31', 'PERCENTAGE', 12.00, CURRENT_TIMESTAMP - interval '3' year),

    ('CLEANUP', 'Home Cleaning Week', '2022-12-01', '2028-12-07', 'PERCENTAGE', 25.00, CURRENT_TIMESTAMP - interval '3' year),
    ('SHAMPOO5', 'Personal Care Steal', '2022-12-15', '2028-12-25', 'FIXED_AMOUNT', 3.00, CURRENT_TIMESTAMP - interval '3' year),

    ('BABY_SAVE', 'New Parents Support', '2025-11-01', '2025-12-31', 'PERCENTAGE', 15.00, CURRENT_TIMESTAMP - interval '3' year),
    ('DIAPER_DEAL', 'Diaper Bulk Purchase', '2022-12-01', '2025-12-31', 'FIXED_AMOUNT', 10.00, CURRENT_TIMESTAMP - interval '3' year),

    ('PANTRY_FILL', 'Stock Your Pantry', '2025-12-01', '2025-12-31', 'PERCENTAGE', 5.00, CURRENT_TIMESTAMP - interval '3' year),
    ('FREEZER_15', 'Frozen Food Fest', '2022-01-05', '2028-01-20', 'PERCENTAGE', 15.00, CURRENT_TIMESTAMP - interval '3' year),

    ('LOYALTY_FIXED', 'Customer Appreciation Reward', '2025-12-01', '2026-12-31', 'FIXED_AMOUNT', 20.00, CURRENT_TIMESTAMP - interval '3' year),
    ('VIP_PERCENT', 'VIP Exclusive Discount', '2022-01-01', '2028-12-31', 'PERCENTAGE', 5.00, CURRENT_TIMESTAMP - interval '3' year);