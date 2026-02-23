from pyspark.sql import SparkSession
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_to_postgres
from etl.db_setup import run_schema
from tabulate import tabulate
from etl.display import display_data

spark = SparkSession.builder \
    .appName("EcommerceETL") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

run_schema()

orders, customers, products = extract_data(spark)

fact_orders, dim_customers, dim_products = transform_data(
    orders, customers, products
)

load_to_postgres(fact_orders, "fact_orders")
load_to_postgres(dim_customers, "dim_customers")
load_to_postgres(dim_products, "dim_products")
display_data()

spark.stop()