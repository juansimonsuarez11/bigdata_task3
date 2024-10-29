from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, >
import logging

# Configura el nivel de log a WARN para reducir los mensajes INFO
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Definir el esquema de los datos de entrada (ajustado para el nuevo productor)
schema = StructType([
    StructField("device_id", IntegerType()),
    StructField("wind_speed", FloatType()),
    StructField("pressure", FloatType()),
    StructField("timestamp", TimestampType())
])

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("SensorDataAnalysis") \
    .getOrCreate()

# Configurar el lector de streaming para leer desde Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .load()

# Parsear los datos JSON usando el nuevo esquema
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Calcular estadísticas por ventana de tiempo para cada dispositivo
windowed_stats = parsed_df \
    .groupBy(window(col("timestamp"), "1 minute"), "device_id") \
    .agg(
        {"wind_speed": "avg", "pressure": "avg"}  # Promedio de velocidad del viento y presión
    ) \
    .withColumnRenamed("avg(wind_speed)", "avg_wind_speed") \
    .withColumnRenamed("avg(pressure)", "avg_pressure")

# Escribir los resultados en la consola
query = windowed_stats \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()

