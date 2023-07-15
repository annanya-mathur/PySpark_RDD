from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DateType

# Create a SparkSession
spark = SparkSession.builder.appName("DateRDDExample").getOrCreate()

# Sample data
data = [("2023-07-01",), ("2023-07-02",), ("2023-07-03",), ("2023-07-04",)]

# Create a DataFrame from the sample data
df = spark.createDataFrame(data, ["date_string"])

# Convert the date_string column to a date type
df = df.withColumn("date", col("date_string").cast(DateType()))

# Create an RDD from the DataFrame
rdd = df.rdd

# Print the RDD
rdd.foreach(lambda row: print(row))
