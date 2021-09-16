static = spark.read.json("activity-data/")
dataSchema = static.schema


# COMMAND ----------

streaming = spark.readStream.schema(dataSchema).option("maxFilesPerTrigger", 1)\
  .json("activity-data")


# COMMAND ----------

activityCounts = streaming.groupBy("gt").count()

activityQuery = activityCounts.writeStream.queryName("activity_counts")\
  .format("memory").outputMode("complete")\
  .start()

# COMMAND ----------

from time import sleep
for x in range(5):
    spark.sql("SELECT * FROM activity_counts").show()
    sleep(1)


