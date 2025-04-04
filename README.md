├── apps/data_bda/ /n
│   ├── sales_data.csv
├── data_generation/
│   |── generar_csv.py 
│   ├── generar_bbdd.py 
│   ├── generarkafka.py
│   ├── scheduler.py 
├── apps/
   ├── integracion_datos.py (a S3/LocalStack)
   ├── transformacion_datos.py (con Spark)
   ├── cargar_datos.py (a Redshift simulado)
   └── analisis_datos.py (consultas Spark y Python)


# ORDEN DE EJECUCIÓN Y VERIFICACIÓN DE SCRIPTS

1. GENERACIÓN DE DATOS
-----------------------
- CSV:
  python data_generation/generate_csv_data.py
  Verifica: Se genera el archivo en data_bda/csv/sales_data.csv

- PostgreSQL:
  python data_generation/generate_db_data.py
  Verifica: Ejecuta en pgAdmin o similar: SELECT * FROM Stores;

- Kafka:
  python data_generation/generate_kafka_data.py
  Verifica: En consola aparecen los mensajes enviados.

2. EXTRACCIÓN A DATA LAKE
--------------------------

  python apps/integracion_csv_bbdd.py   -----   Ejecutar dentro del contenedor
  Verifica: Se crea el directorio parquet en S3 simulado (LocalStack).

  el datalake no existía así que lo he creado entrando en el contenedor de localstak y ejecutando:

  aws configure
  # Access Key: test
  # Secret Key: test
  # Region: us-east-1

  export AWS_ACCESS_KEY_ID=test
  export AWS_SECRET_ACCESS_KEY=test
  aws --endpoint-url=http://localhost:4566 --region us-east-1 s3 mb s3://datalake



3. TRANSFORMACIÓN DE DATOS ---- ejecutar dentro del cotenedor
---------------------------
 
  python app/transformacion_datos.py

  Verifica: Se genera el parquet limpio en s3a://datalake/clean/

  Ejecutar en el contenedor de localstack " awslocal s3 ls  

  s3://datalake/clean/sales_data_cleaned/ --recursive "


4. CARGA EN DATA WAREHOUSE
---------------------------

  Desde donde estamos ejecutando los scripts:
  export AWS_ACCESS_KEY_ID=test
  export AWS_SECRET_ACCESS_KEY=test


  python apps/cargar_datos.py

  Verifica: Verifica tabla Stores en Postgrest

5. ANÁLISIS DE DATOS
---------------------
  python apps/analisis_datos.py

  Verifica: Resultados impresos por pantalla con Spark.show()

6. EJECUCIÓN CÍCLICA AUTOMÁTICA ---- ejecutar desde winsows
------------------------------------------
  python apps/scheduler.py
  Verifica: Verás que se lanza cada script en bucle cada 30 o 60 segundos.
