from pyspark.sql.functions import col, trim, initcap, lit, row_number, current_timestamp
from pyspark.sql.window import Window

def transform_order_priority_dim(order_priority_df):
    df = (
        order_priority_df
        .select(
            col("id").alias("order_priority_id"),
            initcap(trim(col("name"))).alias("order_priority_name")
        )
        .dropDuplicates()
    )

    window = Window.orderBy("order_priority_name")
    df = df.withColumn("order_priority_tk", row_number().over(window))
    df = df.withColumn("version", lit(1))
    df = df.withColumn("date_from", current_timestamp())
    df = df.withColumn("date_to", lit(None).cast("timestamp"))

    return df.select("order_priority_tk", "version", "date_from", "date_to", "order_priority_id", "order_priority_name")
