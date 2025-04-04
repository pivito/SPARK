from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DataIntegration") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.11.1026,"
            "org.postgresql:postgresql:42.7.1") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

def upload_csv_to_s3(local_path, bucket, key):
    print(f"⬆ Subiendo CSV {local_path} a s3a://{bucket}/{key}...")
    df = spark.read.option("header", True).csv(local_path)
    df.write.mode("overwrite").parquet(f"s3a://{bucket}/{key}")
    print("✅ CSV subido correctamente a S3\n")

def export_table_to_s3(table_name, s3_path):
    print(f"⬇ Exportando tabla {table_name} desde PostgreSQL a {s3_path}...")
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://database:5432/retail_db") \
            .option("dbtable", table_name) \
            .option("user", "postgres") \
            .option("password", "casa1234") \
            .option("driver", "org.postgresql.Driver") \
            .load()

        df.write.mode("overwrite").parquet(s3_path)
        print(f"✅ Tabla {table_name} exportada correctamente\n")
    except Exception as e:
        print(f"❌ Error exportando la tabla {table_name}:\n{e}")

if __name__ == '__main__':
    upload_csv_to_s3("data_bda/csv/sales_data.csv", "datalake", "csv/sales_data")
    export_table_to_s3("sales_analysis", "s3a://datalake/raw/from_postgres/sales_analysis")
