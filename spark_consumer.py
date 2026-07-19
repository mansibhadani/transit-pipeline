from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, LongType

spark = SparkSession.builder \
    .appName("transit-consumer") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define what shape we expect each JSON record to be
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("route_id", StringType()) \
    .add("stop_id", StringType()) \
    .add("arrival_time", LongType()) \
    .add("departure_time", LongType()) \
    .add("feed_timestamp", LongType())