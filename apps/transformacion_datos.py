from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import IntegerType, DoubleType, StringType, LongType, TimestampType
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder \
    .appName("TransformacionDatos") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
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

# Esquema base que esperamos para todos los DataFrame
expected_schema = {
    "store_id": LongType(),
    "store_name": StringType(),
    "location": StringType(),
    "demographics": StringType(),
    "Date": StringType(),
    "Product": StringType(),
    "Quantity Sold": IntegerType(),
    "Revenue": DoubleType(),
    "Tratados": StringType(),
    "Fecha Inserción": TimestampType()
}

# Forzar columnas y orden según esquema esperado
def align_dataframe(df, schema_dict):
    for col_name, data_type in schema_dict.items():
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None).cast(data_type))
    return df.select(*schema_dict.keys())

def transform_data(df):
    return (
        df.dropDuplicates()
          .withColumn("Quantity Sold", col("Quantity Sold").cast(IntegerType()))
          .withColumn("Revenue", col("Revenue").cast(DoubleType()))
          .withColumn("Tratados", lit(True))
          .withColumn("Fecha Inserción", current_timestamp())
    )

if __name__ == "__main__":
    print("Leyendo CSV desde S3 (exportado)...")
    df_csv = spark.read.parquet("s3a://datalake/csv/sales_data")
    df_csv_cleaned = align_dataframe(transform_data(df_csv), expected_schema)

    print("Leyendo datos desde Kafka (raw)...")
    try:
        df_kafka = spark.read.parquet("s3a://datalake/raw/sales_data_raw")
        df_kafka_cleaned = align_dataframe(transform_data(df_kafka), expected_schema)
    except AnalysisException:
        print("*****No se encontraron datos Kafka en S3. Se omite esta fuente.*****")
        df_kafka_cleaned = None

    print("Leyendo datos directamente desde PostgreSQL...")
    try:
        df_pg = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres-db:5432/retail_db") \
            .option("dbtable", "Stores") \
            .option("user", "postgres") \
            .option("password", "casa1234") \
            .option("driver", "org.postgresql.Driver") \
            .load()
        df_pg_cleaned = df_pg.dropDuplicates() \
            .withColumn("Tratados", lit(True)) \
            .withColumn("Fecha Inserción", current_timestamp())
        df_pg_cleaned = align_dataframe(df_pg_cleaned, expected_schema)
    except Exception as e:
        print(f"Error al leer datos de PostgreSQL:\n{e}")
        df_pg_cleaned = None

    print("Leyendo tabla exportada desde PostgreSQL (sales_analysis)...")
    df_pg_exported = spark.read.parquet("s3a://datalake/raw/from_postgres/sales_analysis")
    df_pg_exported_cleaned = align_dataframe(transform_data(df_pg_exported), expected_schema)

    print("Uniendo datasets...")
    dfs_to_union = [df_csv_cleaned, df_pg_exported_cleaned]
    if df_pg_cleaned is not None:
        dfs_to_union.append(df_pg_cleaned)
    if df_kafka_cleaned is not None:
        dfs_to_union.append(df_kafka_cleaned)

    df_total = dfs_to_union[0]
    for df in dfs_to_union[1:]:
        df_total = df_total.unionByName(df)

    print("Guardando datos combinados en S3...")
    df_total.write.mode("overwrite").parquet("s3a://datalake/clean/sales_data_cleaned")

    print("Transformación y guardado completados.")
