from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col

spark = SparkSession.builder \
    .appName("DataAnalysis") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

df = spark.read.parquet("s3a://datalake/clean/sales_data_cleaned")

print("Tienda con más ingresos:")
df.groupBy("store_id").agg(sum("Revenue").alias("TotalRevenue")) \
    .orderBy(col("TotalRevenue").desc()).show(1)

print("Día con más ingresos:")
df.groupBy("Date").agg(sum("Revenue").alias("TotalRevenue")) \
    .orderBy(col("TotalRevenue").desc()).show(1)

print("Producto más vendido:")
df.groupBy("Product").agg(sum("Quantity Sold").alias("TotalSold")) \
    .orderBy(col("TotalSold").desc()).show(1)

spark.stop()