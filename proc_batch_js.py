#Importamos librerias necesarias
from pyspark.sql import SparkSession, functions as F
# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('Tarea3').getOrCreate()
# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea3/StudentPerformanceFactors.csv'
# Lee el archivo .csv
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)
#imprimimos el esquema
df.printSchema()
# Muestra las primeras filas del DataFrame
df.show()
# Estadisticas básicas
df.summary().show()
# Seleccionar las columnas numéricas relevantes
numeric_columns = ['Hours_Studied', 'Attendance', 'Sleep_Hours', 'Previous_Scores',
                   'Tutoring_Sessions', 'Physical_Activity', 'Exam_Score']

# Calcular la correlación de cada columna numérica con 'Exam_Score'
correlations = {col: df.stat.corr(col, 'Exam_Score') for col in numeric_columns[:-1]}

# Convertir el diccionario a un DataFrame y ordenar por correlación
correlation_df = spark.createDataFrame(correlations.items(), ["Column", "Correlation"]) \
                       .orderBy("Correlation", ascending=False)

# Mostrar los resultados
correlation_df.show()
# Calcular media, mediana y moda
results = []

for col in numeric_columns:
    mean_value = df.select(F.mean(col)).first()[0]
    median_value = df.approxQuantile(col, [0.5], 0.01)[0]
    mode_value = df.groupBy(col).count().orderBy("count", ascending=False).first()[0]
    results.append((col, mean_value, median_value, mode_value))

# Crear un DataFrame con los resultados
results_df = spark.createDataFrame(results, ["Column", "Mean", "Median", "Mode"])
# Mostrar el DataFrame con los resultados
results_df.show()

# Contar el total de datos analizados
total_count = df.count()
print(f"Total datos analizados: {total_count}")


