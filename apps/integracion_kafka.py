from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType

# Crear SparkSession con configuración para Kafka y S3
spark = SparkSession.builder \
    .appName("KafkaToS3Integration") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.11.1026") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

print("🚀 Sesión de Spark inicializada para integración Kafka → S3")

# Definir esquema del JSON esperado
schema = StructType() \
    .add("store_id", IntegerType()) \
    .add("product_id", IntegerType()) \
    .add("quantity", IntegerType()) \
    .add("price", FloatType()) \
    .add("timestamp", StringType())

# Leer desde Kafka
df_kafka = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "ventas") \
    .option("startingOffsets", "earliest") \
    .load()

print("📥 Datos leídos desde Kafka")

# Convertir el valor a JSON y aplicar esquema
df_json = df_kafka.selectExpr("CAST(value AS STRING)") \
    .withColumn("data", from_json(col("value"), schema)) \
    .select("data.*")

# Filtrar mensajes válidos
df_filtrado = df_json.filter(col("store_id").isNotNull())

if df_filtrado.count() > 0:
    print(f"📊 Se han recibido {df_filtrado.count()} registros válidos")
    df_filtrado.write.mode("append").parquet("s3a://datalake/raw/from_kafka/sales_data")
    print("✅ Datos almacenados en S3 correctamente\n")
else:
    print("⚠️ No se recibieron nuevos datos desde Kafka\n")

spark.stop()
