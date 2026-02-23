import psycopg2
import os 

def load_to_postgres(df, table_name):

    url = "jdbc:postgresql://localhost:5432/ecommerce"
    properties = {
        "user": "postgres",
        "password": "Sanjay@6215",
        "driver": "org.postgresql.Driver"
    }

    staging_table = table_name + "_staging"

    # 1️⃣ Write DataFrame to staging table
    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", staging_table) \
        .option("user", properties["user"]) \
        .option("password", properties["password"]) \
        .option("driver", properties["driver"]) \
        .mode("overwrite") \
        .save()

    # 2️⃣ Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname="ecommerce",
        user="postgres",
        password=os.getenv("DB_PASSWORD"),
        host="localhost",
        port="5432"
    )

    cursor = conn.cursor()

    # ================= FACT ORDERS =================
    if table_name == "fact_orders":
        cursor.execute(f"""
            INSERT INTO fact_orders (
                order_id,
                order_date,
                customer_id,
                product_id,
                quantity,
                unit_price,
                total_price,
                payment_method,
                order_status
            )
            SELECT
                s.order_id,
                s.order_date::DATE,
                s.customer_id,
                s.product_id,
                s.quantity,
                s.unit_price,
                s.total_price,
                s.payment_method,
                s.order_status
            FROM {staging_table} s
            ON CONFLICT (order_id) DO NOTHING;
        """)

    # ================= DIM CUSTOMERS =================
    elif table_name == "dim_customers":
        cursor.execute(f"""
            INSERT INTO dim_customers (
                customer_id,
                customer_name,
                email,
                phone,
                city,
                state,
                signup_date
            )
            SELECT
                s.customer_id,
                s.customer_name,
                s.email,
                s.phone,
                s.city,
                s.state,
                s.signup_date::DATE
            FROM {staging_table} s
            ON CONFLICT (customer_id) DO NOTHING;
        """)

    # ================= DIM PRODUCTS =================
    elif table_name == "dim_products":
        cursor.execute(f"""
            INSERT INTO dim_products (
                product_id,
                product_name,
                category,
                brand,
                cost_price,
                selling_price,
                stock_quantity
            )
            SELECT
                s.product_id,
                s.product_name,
                s.category,
                s.brand,
                s.cost_price,
                s.selling_price,
                s.stock_quantity
            FROM {staging_table} s
            ON CONFLICT (product_id) DO NOTHING;
        """)

    # Commit inserts
    conn.commit()

    # 3️⃣ Drop staging table after merge
    cursor.execute(f"DROP TABLE IF EXISTS {staging_table};")
    conn.commit()

    cursor.close()
    conn.close()

    print(f"Loaded {table_name} successfully with UPSERT logic.")