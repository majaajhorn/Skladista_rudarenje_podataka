# extract/extract_mysql.py
from spark_session import get_spark_session

def extract_table(table_name):
    spark = get_spark_session("ETL_App")

    jdbc_url = "jdbc:mysql://127.0.0.1:3306/superstore_dw?useSSL=false"

    connection_properties = {
        "user": "root",
        "password": "root",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
    return df

# ovo su tablice iz relacijskog modela
def extract_all_tables():
    return {
        "customer": extract_table("customer"),
        "country": extract_table("country"),
        "order_priority": extract_table("order_priority"),
        "order": extract_table("`order`"),
        "order_details": extract_table("order_details"),
        "ship_mode": extract_table("ship_mode"),
        "product": extract_table("product"),
        "subcategory": extract_table("subcategory"),
        "category": extract_table("category"),
    }