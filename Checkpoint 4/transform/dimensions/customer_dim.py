from pyspark.sql.functions import col, trim, initcap, lit, row_number, current_timestamp
from pyspark.sql.window import Window

def transform_customer_dim(customer_df, country_df):
    df = (
        customer_df.alias("c")
        .join(country_df.alias("co"), col("c.country_fk") == col("co.id"), "left")
        .select(
            col("c.id").alias("customer_id"),
            initcap(trim(col("c.name"))).alias("customer_name"),
            initcap(trim(col("c.segment"))).alias("segment"),
            initcap(trim(col("c.state"))).alias("state"),
            initcap(trim(col("co.name"))).alias("country"),
            initcap(trim(col("co.region"))).alias("region"),
            initcap(trim(col("co.market"))).alias("market")
        )
        .dropDuplicates(["customer_name", "country", "state"])
    )

    window = Window.orderBy("country", "state", "customer_name")
    df = df.withColumn("customer_tk", row_number().over(window))
    df = df.withColumn("version", lit(1))
    df = df.withColumn("date_from", current_timestamp())
    df = df.withColumn("date_to", lit(None).cast("timestamp"))

    return df.select("customer_tk", "version", "date_from", "date_to", "customer_id", "customer_name", "segment", "state", "country", "region", "market")