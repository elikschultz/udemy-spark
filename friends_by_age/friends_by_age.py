from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("friends_by_age").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("fakefriends-header.csv")

people.groupBy("age").avg("friends").show()

spark.stop()