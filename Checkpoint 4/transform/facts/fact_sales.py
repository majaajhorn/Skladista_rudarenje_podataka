# transform/facts/fact_sales.py
from pyspark.sql.functions import col, to_date
from pyspark.sql import DataFrame

def transform_fact_sales(order_details_df: DataFrame,
                         order_df: DataFrame,
                         order_dim: DataFrame,
                         date_df: DataFrame,
                         customer_df: DataFrame,
                         product_df: DataFrame,
                         ship_mode_df: DataFrame,
                         order_priority_df: DataFrame) -> DataFrame:

    # Ispišimo uzorak podataka
    print("🔍 Inspecting key columns for debugging:")
    print("--- order_details_df.order_fk ---")
    order_details_df.select("order_fk").show(5)
    
    print("--- order_df.id and order_df.order_id ---")
    order_df.select("id", "order_id").show(5)
    
    # Pogledajmo tip podataka
    print("🔍 Schema tipovi za ključne kolone:")
    print(f"order_details_df.order_fk: {order_details_df.schema['order_fk'].dataType}")
    print(f"order_df.id: {order_df.schema['id'].dataType}")
    print(f"order_df.order_id: {order_df.schema['order_id'].dataType}")
    
    # Aliasi dimenzijskih tablica za jasniji kod
    order_dim_aliased = order_dim.alias("dim_ord")
    product_dim_aliased = product_df.alias("dim_prod")
    customer_dim_aliased = customer_df.alias("dim_cust")
    ship_mode_dim_aliased = ship_mode_df.alias("dim_ship")
    order_priority_dim_aliased = order_priority_df.alias("dim_prio")
    order_date_df = date_df.alias("order_date_dim")
    ship_date_df = date_df.alias("ship_date_dim").withColumnRenamed("date_tk", "ship_date_tk")
    
    # Sada trebamo koristiti order_details_df.order_fk = order_df.id za join
    print("⏳ Joining order_details with order using ID, not order_id...")
    df_step1 = order_details_df.alias("od") \
        .join(order_df.alias("o"), col("od.order_fk") == col("o.id"), "inner")
    
    print(f"✅ Broj redova nakon order_details + order: {df_step1.count()}")
    
    # Ako imamo redove, nastavimo s joinovima
    if df_step1.count() > 0:
        # Pretvorimo datume iz stringa u date tip
        df_step1 = df_step1.withColumn("order_date", to_date(col("o.order_date"))) \
                           .withColumn("ship_date", to_date(col("o.ship_date")))
        
        # Sada joinamo s date dimenzijom za order date
        print("⏳ Joining with order date dimension...")
        df_with_order_date = df_step1 \
            .join(order_date_df, col("order_date") == col("order_date_dim.date_value"), "inner") \
            .withColumnRenamed("date_tk", "order_date_tk")
        
        # Zatim joinamo s date dimenzijom za ship date
        print("⏳ Joining with ship date dimension...")
        df_with_ship_date = df_with_order_date \
            .join(ship_date_df, col("ship_date") == col("ship_date_dim.date_value"), "inner")
        
        print(f"✅ Broj redova nakon joinove s date: {df_with_ship_date.count()}")
        
        # Joinamo s customer dimenzijom
        print("⏳ Joining with customer dimension...")
        df_with_customer = df_with_ship_date \
            .join(customer_dim_aliased, col("o.customer_fk") == col("dim_cust.customer_id"), "inner")
        
        print(f"✅ Broj redova nakon join s customer: {df_with_customer.count()}")
        
        # Joinamo s product dimenzijom
        print("⏳ Joining with product dimension...")
        df_with_product = df_with_customer \
            .join(product_dim_aliased, col("od.product_fk") == col("dim_prod.product_id"), "inner")
        
        print(f"✅ Broj redova nakon join s product: {df_with_product.count()}")
        
        # Joinamo sa ship_mode dimenzijom
        print("⏳ Joining with ship_mode dimension...")
        df_with_ship_mode = df_with_product \
            .join(ship_mode_dim_aliased, col("o.ship_mode_fk") == col("dim_ship.ship_mode_id"), "inner")
        
        print(f"✅ Broj redova nakon join s ship_mode: {df_with_ship_mode.count()}")
        
        # Joinamo s order_priority dimenzijom
        print("⏳ Joining with order_priority dimension...")
        df_with_priority = df_with_ship_mode \
            .join(order_priority_dim_aliased, col("o.order_priority_fk") == col("dim_prio.order_priority_id"), "inner")
        
        print(f"✅ Broj redova nakon join s order_priority: {df_with_priority.count()}")
        
        # Joinamo s order_dim da dobijemo order_tk
        print("⏳ Joining with order dimension (za surrogate key)...")
        df_final = df_with_priority \
            .join(order_dim_aliased, col("o.order_id") == col("dim_ord.order_id"), "inner")
        
        print(f"✅ Broj redova nakon sve joinove: {df_final.count()}")
        
        # Ispišimo imena stupaca koje imamo
        print("📊 Dostupni stupci:")
        for column in df_final.columns:
            print(f" - {column}")
        
        # Konačni select - koristimo jasno imenovane stupce
        print("⏳ Preparing final selection...")
        df_result = df_final.select(
            col("order_date_tk").alias("date_tk"),
            col("dim_cust.customer_tk"),
            col("dim_prod.product_tk"),
            col("dim_ship.ship_mode_tk"),
            col("dim_prio.order_priority_tk"),
            col("dim_ord.order_tk"),
            col("ship_date_tk"),
            col("od.quantity"),
            col("od.sales"),
            col("od.discount"),
            col("od.profit"),
            col("od.shipping_cost")
        )
        
        print("✅ Final selection ready.")
        return df_result
    else:
        # Ako nemamo redova, kreirajmo prazan DataFrame s ispravnom shemom
        print("⚠️ Nema rezultata nakon joinova - vraćamo prazan DataFrame")
        empty_schema = """
            date_tk INT, 
            customer_tk BIGINT, 
            product_tk BIGINT, 
            ship_mode_tk BIGINT, 
            order_priority_tk BIGINT, 
            order_tk BIGINT, 
            ship_date_tk INT, 
            quantity INT, 
            sales FLOAT, 
            discount FLOAT, 
            profit FLOAT, 
            shipping_cost FLOAT
        """
        return order_df.sparkSession.createDataFrame([], empty_schema)