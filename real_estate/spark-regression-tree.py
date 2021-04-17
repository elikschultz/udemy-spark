from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler

# Set up Spark session
spark = SparkSession.builder.appName("RegressionTree").getOrCreate()

# Read in data and prepare it for analysis 
data = spark.read.format('csv').options(header='true').options(inferSchema='true').load('realestate.csv')
assembler = VectorAssembler(
	inputCols = ['HouseAge', 'DistanceToMRT', 'NumberConvenienceStores'],
	outputCol = 'features')
data_final = assembler.transform(data)


trainTest = data_final.randomSplit([0.5, 0.5])
trainingDF = trainTest[0]
testDF = trainTest[1]

# Fit model and make predictions
dt = DecisionTreeRegressor(featuresCol = 'features', labelCol = 'PriceOfUnitArea')

model = dt.fit(trainingDF)

fullPredictions = model.transform(testDF).cache()

# Extract the predictions and the "known" correct labels.
predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
labels = fullPredictions.select("PriceOfUnitArea").rdd.map(lambda x: x[0])

# Zip them together
predictionAndLabel = predictions.zip(labels).collect()

# Print out the predicted and actual values for each point
for prediction in predictionAndLabel:
  print(prediction)

# Stop the session
spark.stop()
