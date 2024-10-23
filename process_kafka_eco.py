from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, DoubleType, IntegerType

# Start Spark
spark = SparkSession.builder \
    .appName("ProcessKafkaTask3") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

# Schema
schema = StructType() \
    .add("ID", IntegerType()) \
    .add("Price", DoubleType()) \
    .add("OpenPrice", DoubleType()) \
    .add("HighPrice", DoubleType()) \
    .add("LowPrice", DoubleType()) \
    .add("ClosePrice", DoubleType()) \
    .add("Volume", DoubleType())

dfKafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic-task3") \
    .load()

kafkaSchema = dfKafka.selectExpr("CAST(value as STRING) as json") \
    .select(F.from_json(F.col("json"), schema).alias("data")) \
    .select("data.*")

kafkaSchema.printSchema()

kafkaSchemaWithTimestamp = kafkaSchema.withColumn("Timestamp", F.current_timestamp())

kafkaSchemaWithWatermark = kafkaSchemaWithTimestamp.withWatermark("Timestamp", "1 minute")

#AVG
average_prices = kafkaSchemaWithWatermark.groupBy(F.window("Timestamp", "10 seconds")) \
    .agg(F.avg("Price").alias("average_price"))

#Filter
filtered_data = kafkaSchemaWithWatermark.filter(F.col("Volume") > 100000)

average_prices.writeStream \
    .outputMode("update") \
    .format("console") \
    .start() \
    .awaitTermination()

filtered_data.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start() \
    .awaitTermination()
