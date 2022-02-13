import pyspark

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

sc = spark.sparkContext
lst = range(100)
rdd = sc.parallelize(lst)
print(rdd.sum())
sc.stop()

