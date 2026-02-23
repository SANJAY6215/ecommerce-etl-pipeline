import psycopg2
import os
from tabulate import tabulate


def display_data():

    conn = psycopg2.connect(
        dbname="ecommerce",
        user="postgres",
        password=os.getenv("DB_PASSWORD"),
        host="localhost",
        port="5432"
    )

    cursor = conn.cursor()

    # ===== FACT ORDERS =====
    cursor.execute("SELECT * FROM fact_orders;")
    orders = cursor.fetchall()
    order_headers = [desc[0] for desc in cursor.description]

    print("\n================ FACT ORDERS ================\n")
    print(tabulate(orders, headers=order_headers, tablefmt="grid"))

    # ===== DIM CUSTOMERS =====
    cursor.execute("SELECT * FROM dim_customers;")
    customers = cursor.fetchall()
    customer_headers = [desc[0] for desc in cursor.description]

    print("\n================ DIM CUSTOMERS ================\n")
    print(tabulate(customers, headers=customer_headers, tablefmt="grid"))

    # ===== DIM PRODUCTS =====
    cursor.execute("SELECT * FROM dim_products;")
    products = cursor.fetchall()
    product_headers = [desc[0] for desc in cursor.description]

    print("\n================ DIM PRODUCTS ================\n")
    print(tabulate(products, headers=product_headers, tablefmt="grid"))

    # ===== TOTAL REVENUE =====
    cursor.execute("SELECT SUM(total_price) FROM fact_orders;")
    revenue = cursor.fetchone()[0]

    print("\n================ TOTAL REVENUE ================\n")
    print(f"Total Revenue: ₹ {revenue}")

    cursor.close()
    conn.close()