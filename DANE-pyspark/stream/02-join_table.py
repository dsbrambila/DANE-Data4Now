static = spark.read.json("activity-data/")
dataSchema = static.schema


# COMMAND ----------

streaming = spark.readStream.schema(dataSchema).option("maxFilesPerTrigger", 1)\
  .json("activity-data")


# COMMAND ----------

historicalAgg = static.groupBy("gt", "model").avg()
deviceModelStats = streaming.drop("Arrival_Time", "Creation_Time", "Index")\
  .cube("gt", "model").avg()\
  .join(historicalAgg, ["gt", "model"])\
  .writeStream.queryName("device_counts").format("memory")\
  .outputMode("complete")\
  .start()

from time import sleep
for x in range(5):
    spark.sql("SELECT * FROM device_counts").show()
    sleep(1)
