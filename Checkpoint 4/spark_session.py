# spark_session.py
from pyspark.sql import SparkSession

# Lazy-load pattern: Spark is only created when this function is called
def get_spark_session(app_name="ETL_App"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", "C:/Users/Maja/Desktop/Faks/VI. semestar/Skladistenje i rudarenje podataka/Vjezbe/Skladista_rudarenje_podataka/Connectors/mysql-connector-j-9.2.0.jar") \
        .getOrCreate()