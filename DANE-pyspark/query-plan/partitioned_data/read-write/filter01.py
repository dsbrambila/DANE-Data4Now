from pyspark.sql import functions as F

data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
rdd = spark.sparkContext.parallelize(data)
df = rdd.toDF(["language","users_count"])
df\
    .groupBy("language")\
    .agg(F.sum(df.users_count))\
    .where(df.language == "Java")\
    .show()
