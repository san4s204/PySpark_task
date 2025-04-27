# check_spark.py
from pyspark.sql import SparkSession
import sys, os

spark = (
    SparkSession.builder
    .appName("check")
    .master("local[1]")
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
)
print("SPARK USES PYTHON:", spark.conf.get("spark.pyspark.python"))
spark.range(3).show()
spark.stop()