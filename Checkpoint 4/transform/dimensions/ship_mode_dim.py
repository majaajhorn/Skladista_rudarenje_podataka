from pyspark.sql.functions import col, trim, initcap, lit, row_number, current_timestamp
from pyspark.sql.window import Window

def transform_ship_mode_dim(ship_mode_df):
    df = (
        ship_mode_df
        .select(
            col("id").alias("ship_mode_id"),
            initcap(trim(col("name"))).alias("ship_mode_name")
        )
        .dropDuplicates()
    )

    window = Window.orderBy("ship_mode_name")
    df = df.withColumn("ship_mode_tk", row_number().over(window))
    df = df.withColumn("version", lit(1))
    df = df.withColumn("date_from", current_timestamp())
    df = df.withColumn("date_to", lit(None).cast("timestamp"))

    return df.select("ship_mode_tk", "version", "date_from", "date_to", "ship_mode_id", "ship_mode_name")