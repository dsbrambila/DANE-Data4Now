spark.conf.set("spark.sql.shuffle.partitions", 5)
static = spark.read.json("activity-data")
streaming = spark\
  .readStream\
  .schema(static.schema)\
  .option("maxFilesPerTrigger", 10)\
  .json("activity-data")


# COMMAND ----------

withEventTime = streaming.selectExpr(
  "*",
  "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")


# COMMAND ----------

from pyspark.sql.functions import window, col
withEventTime\
  .withWatermark("event_time", "30 minutes")\
  .groupBy(window(col("event_time"), "10 minutes", "5 minutes"))\
  .count()\
  .writeStream\
  .queryName("pyevents_per_window")\
  .format("memory")\
  .outputMode("complete")\
  .start()

from time import sleep
for x in range(5):
    spark.sql("SELECT * FROM pyevents_per_window").show()
    sleep(1)

