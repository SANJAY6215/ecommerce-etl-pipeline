import psycopg2
import os

def run_schema():

    password = os.getenv("DB_PASSWORD")

    if not password:
        raise ValueError("DB_PASSWORD environment variable not set!")

    conn = psycopg2.connect(
        dbname="ecommerce",
        user="postgres",
        password=password,
        host="localhost",
        port="5432"
    )

    cursor = conn.cursor()

    with open("warehouse/schema.sql", "r") as file:
        schema_sql = file.read()

    cursor.execute(schema_sql)
    conn.commit()

    cursor.close()
    conn.close()

    print("Database schema ensured successfully.")