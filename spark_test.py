from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
df = spark.createDataFrame([(1, "hello"), (2, "world")], ["id", "text"])
df.show()