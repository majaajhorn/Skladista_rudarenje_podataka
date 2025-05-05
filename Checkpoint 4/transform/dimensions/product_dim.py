from pyspark.sql.functions import col, trim, initcap, lit, row_number, current_timestamp
from pyspark.sql.window import Window

def transform_product_dim(product_df, subcategory_df, category_df):
    df = (
        product_df.alias("p")
        .join(subcategory_df.alias("s"), col("p.subcategory_fk") == col("s.id"), "left")
        .join(category_df.alias("c"), col("s.category_fk") == col("c.id"), "left")
        .select(
            col("p.id").alias("product_id"),
            initcap(trim(col("p.name"))).alias("product_name"),
            initcap(trim(col("s.name"))).alias("subcategory"),
            initcap(trim(col("c.name"))).alias("category")
        )
        # 🔴 NEMOJ dropDuplicates nad imenom - čuvamo sve ID-eve!
    )

    window = Window.orderBy("product_id")  # ili po nečemu stabilnom
    df = df.withColumn("product_tk", row_number().over(window))
    df = df.withColumn("version", lit(1))
    df = df.withColumn("date_from", current_timestamp())
    df = df.withColumn("date_to", lit(None).cast("timestamp"))

    return df.select("product_tk", "version", "date_from", "date_to", "product_id", "product_name", "subcategory", "category")
