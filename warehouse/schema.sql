-- Drop tables if they exist
DROP TABLE IF EXISTS fact_orders CASCADE;
DROP TABLE IF EXISTS dim_customers CASCADE;
DROP TABLE IF EXISTS dim_products CASCADE;

-- ================= FACT TABLE =================
CREATE TABLE fact_orders (
    order_id INT PRIMARY KEY,
    order_date DATE,
    customer_id INT,
    product_id INT,
    quantity INT,
    unit_price NUMERIC,
    total_price NUMERIC,
    payment_method TEXT,
    order_status TEXT
);

-- ================= DIM CUSTOMERS =================
CREATE TABLE dim_customers (
    customer_id INT PRIMARY KEY,
    customer_name TEXT,
    email TEXT,
    phone TEXT,
    city TEXT,
    state TEXT,
    signup_date DATE
);

-- ================= DIM PRODUCTS =================
CREATE TABLE dim_products (
    product_id INT PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    brand TEXT,
    cost_price NUMERIC,
    selling_price NUMERIC,
    stock_quantity INT
);