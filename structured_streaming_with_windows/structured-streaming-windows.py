from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

from pyspark.sql import functions as func

# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("StructuredStreaming").getOrCreate()

# Monitor the logs directory for new log data, and read in the raw lines as accessLines
accessLines = spark.readStream.text("logs")

# Parse out the common log format to a DataFrame
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

logsDF = accessLines.select(func.regexp_extract('value', hostExp, 1).alias('host'),
                         func.regexp_extract('value', timeExp, 1).alias('timestamp'),
                         func.regexp_extract('value', generalExp, 1).alias('method'),
                         func.regexp_extract('value', generalExp, 2).alias('endpoint'),
                         func.regexp_extract('value', generalExp, 3).alias('protocol'),
                         func.regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                         func.regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))

# Create timestamp column for demonstration purposes
logsDF = logsDF.withColumn("dummyTimestamp", func.current_timestamp())

# Keep a running count of every access by status code and window
statusCountsDF = logsDF.groupBy(
	func.window(func.col("dummyTimestamp"), 
		windowDuration = "30 seconds", 
		slideDuration = "10 seconds"), 
	func.col("endpoint")).count().orderBy(func.col("count").desc())

# Kick off our streaming query, dumping results to the console
query = ( statusCountsDF.writeStream.outputMode("complete").format("console").queryName("counts").start() )

# Run forever until terminated
query.awaitTermination()

# Cleanly shut down the session
spark.stop()

