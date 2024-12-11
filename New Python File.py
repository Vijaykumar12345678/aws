from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("PySparkExample").getOrCreate()

# Create a DataFrame
data = [("Alice", 29), ("Bob", 31), ("Cathy", 25)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

# Stop the SparkSession
spark.stop()
