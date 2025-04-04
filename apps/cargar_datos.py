from pyspark.sql import SparkSession

def get_spark_session():
    return SparkSession.builder \
        .appName("DataLoadApp") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-hadoop-cloud_2.13:3.5.1,"
                "software.amazon.awssdk:s3:2.25.11,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "org.postgresql:postgresql:42.7.1") \
        .config("spark.hadoop.fs.s3a.access.key", "test") \
        .config("spark.hadoop.fs.s3a.secret.key", "test") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()


def load_to_redshift(input_path, table_name):
    print("Entrando en load_to_redshift...")

    spark = get_spark_session()
    print(f"Intentando leer desde: {input_path}")

    try:
        df = spark.read.parquet(input_path)
        row_count = df.count()
        print(f"Número de filas leídas: {row_count}")
        df.show(5, truncate=False)
        print("Datos mostrados correctamente")
    except Exception as e:
        print("Error durante la lectura del Parquet:")
        print(e)
        raise

    print(f"Escribiendo en PostgreSQL (tabla: {table_name})...")

    try:
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres-db:5432/retail_db") \
            .option("dbtable", table_name) \
            .option("user", "postgres") \
            .option("password", "casa1234") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        print("Datos guardados en PostgreSQL correctamente")
    except Exception as e:
        print("Error guardando en PostgreSQL:")
        print(e)
        print("Guardando copia de seguridad local en Parquet...")
        df.write.mode("overwrite").parquet(f"/tmp/{table_name}_backup")
        print(f"Backup guardado en /tmp/{table_name}_backup")


# Esto asegura que se ejecuta al hacer python data_load.py
if __name__ == "__main__":
    print("Script iniciado")
    input_path = "s3a://datalake/clean/sales_data_cleaned"
    load_to_redshift(input_path, "sales_analysis")
