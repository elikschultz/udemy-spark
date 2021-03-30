from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("SpendByCustomer").getOrCreate()

# Manually specify column names/types to use when reading file
schema = StructType([ \
                     StructField("customerID", IntegerType(), True), \
                     StructField("itemID", IntegerType(), True), \
                     StructField("amountSpent", FloatType(), True)])

df = spark.read.schema(schema).csv("customer-orders.csv")

# Compute total spend by customer
spend_by_customer = df.groupby("customerID").sum("amountSpent")
spend_by_customer_sorted = spend_by_customer.withColumn("totalSpent", func.round("sum(amountSpent)", 2)).select("customerID", "totalSpent").sort("totalSpent")
spend_by_customer_sorted.show()
    
spark.stop()