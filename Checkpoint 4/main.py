# main.py
from extract.extract_mysql import extract_all_tables
from extract.extract_csv import extract_from_csv
from transform.pipeline import run_transformations
from spark_session import get_spark_session
from load.run_loading import write_spark_df_to_mysql
import os

# Unset SPARK_HOME if it exists to prevent Spark session conflicts
os.environ.pop("SPARK_HOME", None)

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    spark.catalog.clearCache()

    # Load data
    print("🚀 Starting data extraction")
    mysql_df = extract_all_tables()
    csv_df = {"csv_sales": extract_from_csv("C:/Users/Maja/Desktop/Faks/VI. semestar/Skladistenje i rudarenje podataka/Vjezbe/Skladista_rudarenje_podataka/Checkpoint 2/2_relational_model/processed/SuperStoreOrders_PROCESSED_20.csv")}
    merged_df = {**mysql_df, **csv_df}
    print("✅ Data extraction completed")

    # Transform data
    print("🚀 Starting data transformation")
    load_ready_dict = run_transformations(merged_df)
    print("✅ Data transformation completed")

    # Load data - prvo dimenzije, onda činjenice
    print("🚀 Starting data loading")
    
    # Prvo napuni dimenzijske tablice
    dimension_tables = ["dim_date", "dim_customer", "dim_product", "dim_ship_mode", "dim_order_priority", "dim_order"]
    for table_name in dimension_tables:
        # osiguravamo da se učitavaju samo one tablice za koje imamo pripremljene podatke
        if table_name in load_ready_dict:
            print(f"📦 Loading dimension table `{table_name}`")
            df = load_ready_dict[table_name]
            write_spark_df_to_mysql(df, table_name)
    
    # Nakon dimenzija, napuni činjeničnu tablicu
    if "fact_sales" in load_ready_dict:
        print(f"📦 Loading fact table `fact_sales`")
        df = load_ready_dict["fact_sales"]
        print("📊 Schema before loading `fact_sales`:")
        df.printSchema()
        write_spark_df_to_mysql(df, "fact_sales")

    print("👏 Data loading completed")

if __name__ == "__main__":
    main()