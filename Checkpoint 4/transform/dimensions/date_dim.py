# transform/dimensions/date_dim.py
from pyspark.sql.functions import col, to_date, dayofmonth, month, quarter, year, date_format, dayofweek, row_number, lit, explode, sequence, expr
from pyspark.sql.window import Window
from spark_session import get_spark_session

def transform_date_dim(order_df):
    spark = get_spark_session()
    
    # Pretvaramo kolone u date tipove
    order_df_dates = order_df.withColumn("order_date", to_date(col("order_date"))) \
                             .withColumn("ship_date", to_date(col("ship_date")))
    
    # Nađi minimalni i maksimalni datum
    min_date = order_df_dates.agg({"order_date": "min"}).collect()[0][0]
    max_date = order_df_dates.agg({"order_date": "max"}).collect()[0][0]
    ship_min = order_df_dates.agg({"ship_date": "min"}).collect()[0][0]
    ship_max = order_df_dates.agg({"ship_date": "max"}).collect()[0][0]
    
    # Uzmi raniji od dva minimuma i kasniji od dva maksimuma
    if ship_min < min_date:
        min_date = ship_min
    if ship_max > max_date:
        max_date = ship_max
    
    # Generiraj sve datume u tom rasponu
    date_range_df = spark.sql(f"""
        SELECT explode(sequence(to_date('{min_date}'), to_date('{max_date}'), interval 1 day)) as date_value
    """)
    
    # Generiraj sve potrebne kolone
    df = date_range_df.withColumn("day", dayofmonth(col("date_value"))) \
           .withColumn("month", month(col("date_value"))) \
           .withColumn("quarter", quarter(col("date_value"))) \
           .withColumn("year", year(col("date_value"))) \
           .withColumn("is_holiday", lit(0)) \
           .withColumn("day_of_week", dayofweek(col("date_value"))) \
           .withColumn("day_name", date_format(col("date_value"), "EEEE")) \
           .withColumn("month_name", date_format(col("date_value"), "MMMM"))

    # Dodaj surrogate key
    window = Window.orderBy("date_value")
    df = df.withColumn("date_tk", row_number().over(window))

    print(f"✅ Generirano {df.count()} datuma od {min_date} do {max_date}")
    
    return df.select("date_tk", "date_value", "day", "month", "quarter", "year", "is_holiday", "day_of_week", "day_name", "month_name")