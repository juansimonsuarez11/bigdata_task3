from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("ProcessBatchTask3").getOrCreate()

file_path = "hdfs://localhost:9000/batch_task3/dataset_task3.csv"

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path)

df.printSchema()

print("average of opening and closing prices by year and month\n")

df = df.withColumn("Date", F.to_date(df["Date"], "yyyy-MM-dd"))

df = df.withColumn("year", F.year(df["Date"])) \
    .withColumn("month", F.month(df["Date"]))

avg_prices = df.groupBy("year", "month") \
    .agg(F.mean("Open").alias("avg_open"), F.mean("Close").alias("avg_close"))

avg_prices.show()
