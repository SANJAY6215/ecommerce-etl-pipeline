def extract_data(spark):

    orders = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv("data/orders.csv")

    customers = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv("data/customers.csv")

    products = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv("data/products.csv")
        
    print("Orders columns:", orders.columns)
    print("Customers columns:", customers.columns)
    print("Products columns:", products.columns) 

    return orders, customers, products