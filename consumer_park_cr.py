from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

# Configurar el esquema de los datos recibidos
schema = StructType() \
    .add("sensor_id", StringType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("timestamp", LongType())

# Leer los datos desde Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "tarea3") \
    .option("startingOffsets", "earliest") \
    .load()

# Convertir los datos en formato JSON al esquema definido
df_kafka_value = df_kafka.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Realizar el análisis: contar eventos y calcular el promedio de temperatura y humedad
df_agg = df_kafka_value \
    .groupBy() \
    .agg(
        count("*").alias("event_count"),
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity")
    )

# Mostrar los resultados en la consola
query = df_agg.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

#Ver los datos
df_kafka_value.printSchema()
