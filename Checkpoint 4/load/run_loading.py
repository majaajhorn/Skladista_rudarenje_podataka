# load/run_loading.py
from pyspark.sql import DataFrame

def write_spark_df_to_mysql(spark_df: DataFrame, table_name: str, mode: str = "append"):
    jdbc_url = "jdbc:mysql://127.0.0.1:3306/superstore_dw?useSSL=false"
    connection_properties = {
        "user": "root",
        "password": "root",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    print(f"Writing to table `{table_name}` with mode `{mode}`...")

    # Ako kolona 'id' postoji, makni je
    if "id" in spark_df.columns:
        spark_df = spark_df.drop("id")

    # Ako pišemo u fact_sales tablicu, dodatna provjera
    if table_name == "fact_sales":
        # Provjeri da DF nije prazan
        row_count = spark_df.count()
        print(f"✅ Broj redova za unos u `{table_name}`: {row_count}")
        if row_count == 0:
            print("⚠️ UPOZORENJE: DataFrame za fact_sales je prazan!")
            return  # Preskačemo pisanje praznog DataFramea

    spark_df.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode=mode,
        properties=connection_properties
    )
    print(f"✅ Done writing to `{table_name}`.")