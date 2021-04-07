from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostObscureSuperheroes").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

# Mapping of IDs to superhero names
names = spark.read.schema(schema).option("sep", " ").csv("Marvel-names.txt")

# Data that shows which superheroes appear together
lines = spark.read.text("marvel-graph.txt")

# For each ID count number of connections (ie how many other superheroes a given superhero appears with)
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
# Filter down to only include records of superheroes that have fewest connections
fewestConnections = connections.sort(func.col("connections").asc()).first()[1]

leastPopularIds = connections.filter(func.col("connections") == fewestConnections)

leastPopularNames = leastPopularIds.join(names, leastPopularIds["id"] == names["id"], "inner").select("name", "connections")

leastPopularNames.show()



