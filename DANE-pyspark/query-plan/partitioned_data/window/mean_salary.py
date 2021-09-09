from pyspark.sql import Window
from pyspark.sql.functions import *

simpleData = [("James", "Sales", 3000),\
                 ("Michael", "Sales", 4600),\
                 ("Robert", "Sales", 4100),\
                 ("Maria", "Finance", 3000),\
                 ("James", "Sales", 3000),\
                 ("Scott", "Finance", 3300),\
                 ("Jen", "Finance", 3900),\
                 ("Jeff", "Marketing", 3000),\
                 ("Kumar", "Marketing", 2000),\
                 ("Saif", "Sales", 4100)\
                 ]

rdd = spark.sparkContext.parallelize(simpleData)
df = rdd.toDF(["employee_name", "department", "salary"])

# con joins
df_agg = df\
    .groupBy("department")\
    .agg(mean(df.salary).alias("mean_salary"))

df\
    .join(df_agg, "department")\
    .withColumn("diff", df.salary - df_agg.mean_salary)\
    .show()

# window
windowSpec = Window.partitionBy("department")
df.withColumn("diff",df.salary - mean(df.salary).over(windowSpec)).show()

