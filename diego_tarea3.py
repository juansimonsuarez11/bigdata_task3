from pyspark.sql import SparkSession, functions as F

# Inicializamos la sesión de Spark como BatchTarea3
spark = SparkSession.builder.appName("BatchTarea3").getOrCreate()

# Definimos la ruta en la que se encuentra la data
file_path = 'hdfs://localhost:9000/Tarea3/spotify-2023.csv'

# Leemos el archivo como csv
df = spark.read.format('csv').option('header' ,'true').option('inferSchema' ,'true').load(file_path)

# Mostramos el esquema
df.printSchema()

# Mostramos un resumen estadístico de la data
df.summary().show()

# Antes de realizar nuestros algoritmos, convertimos las columnas relevantes que sean string a tipo numérico
df = df.withColumn("energy_%", F.col("energy_%").cast("double")) \
       .withColumn("bpm", F.col("bpm").cast("double")) \
       .withColumn("streams", F.col("streams").cast("double"))


# Primero, mostraremos la cantidad de reproducciones por artista

# Agrupamos por artista y sumar las reproducciones
df_reproducciones_artistas = df.groupBy("artist(s)_name").agg(F.sum("streams").alias("total_streams"))

# Mostrar el resultado
df_reproducciones_artistas.show()


# Ahora, haremos una relación entre la cantidad de reproducciones en relación con la energía de la canción y el BPM.

# Seleccionamos las columnas necesarias
df_energia_bpm_reproducciones = df.select("energy_%", "bpm", "streams")

# Calculamos las correlaciones
correlacion_energia_reproducciones = df_energia_bpm_reproducciones.stat.corr("energy_%", "streams")
correlacion_bpm_reproducciones = df_energia_bpm_reproducciones.stat.corr("bpm", "streams")

# Mostramos las correlaciones
print(f"Correlación entre energía y reproducciones: {correlacion_energia_reproducciones}")
print(f"Correlación entre BPM y reproducciones: {correlacion_bpm_reproducciones}")

