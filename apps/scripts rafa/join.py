from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DataFrame Join Example") \
    .getOrCreate()

# Create two sample DataFrames
df1 = spark.createDataFrame([(1, "Alice"), (2, "Bob"), (3, "Charlie")], ["id", "name"])
df2 = spark.createDataFrame([(1, 25), (2, 30), (4, 35)], ["id", "age"])

# Perform an inner join
inner_join_df = df1.join(df2, "id", "inner")

# Show the result
inner_join_df.show()
