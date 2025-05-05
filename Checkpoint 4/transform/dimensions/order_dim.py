# transform/dimensions/order_dim.py
from pyspark.sql import Window
from pyspark.sql.functions import row_number, col, to_date
from pyspark.sql import DataFrame

def transform_order_dim(order_df: DataFrame) -> DataFrame:
    # Ispiši schema da vidimo s čime radimo
    print("🔍 Ulazna schema order_df:")
    order_df.printSchema()
    
    # Pretvorimo stringove u datume da se mogu pravilno koristiti
    df_clean = order_df.withColumn("order_date", to_date(col("order_date"))) \
                       .withColumn("ship_date", to_date(col("ship_date")))
    
    # Generiraj surrogate key
    window_spec = Window.orderBy("order_id")
    df_with_tk = df_clean.withColumn("order_tk", row_number().over(window_spec))
    
    # Vrati degeneriranu dimenziju koja ima samo TK i business key
    return df_with_tk.select(
        "order_tk",
        "order_id"
    )