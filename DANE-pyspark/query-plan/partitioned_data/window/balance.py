from pyspark.sql import Window
from pyspark.sql.functions import *
transaciones = [("James","2020-01-01", 3000),\
                 ("James","2020-01-02", -1000),\
                 ("Robert", "2020-01-02", 4100),\
                 ("Robert", "2020-01-05", 1000),\
                 ("James", "2020-01-05", -500),\
                 ]
rdd = spark.sparkContext.parallelize(transaciones)
df = rdd.toDF(["name", "date", "amount"])
df2 = df.withColumn("Date",to_date(df.date))

windowSpec = Window.partitionBy('name').orderBy('Date')\
             .rangeBetween(Window.unboundedPreceding, 0)
balance_df = df2.withColumn('balance', sum('amount').over(windowSpec))
df.show()
balance_df.show()
