from pyspark.sql.functions import col, to_date

def transform_data(orders, customers, products):

    # ================= FACT TABLE =================
    fact_orders = orders.select(
        col("order_id"),
        to_date(col("order_date"), "yyyy-MM-dd").alias("order_date"),
        col("customer_id"),
        col("product_id"),
        col("quantity"),
        col("unit_price"),
        col("total_price"),
        col("payment_method"),
        col("order_status")
    )

    # ================= DIM CUSTOMERS =================
    dim_customers = customers.select(
        col("customer_id"),
        col("customer_name"),
        col("email"),
        col("phone"),
        col("city"),
        col("state"),
        to_date(col("signup_date"), "yyyy-MM-dd").alias("signup_date")
    )

    # ================= DIM PRODUCTS =================
    dim_products = products.select(
        col("product_id"),
        col("product_name"),
        col("category"),
        col("brand"),
        col("cost_price"),
        col("selling_price"),
        col("stock_quantity")
    )

    return fact_orders, dim_customers, dim_products