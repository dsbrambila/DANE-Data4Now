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
df.show()

from pyspark.sql import Window
from pyspark.sql.functions import row_number
windowSpec = Window.partitionBy("department").orderBy("salary")
"""row_number"""
df\
    .withColumn("row_number",row_number().over(windowSpec)) \
    .show(truncate=False)

"""rank"""
from pyspark.sql.functions import rank
df.withColumn("rank",rank().over(windowSpec)) \
    .show()

"""dens_rank"""
from pyspark.sql.functions import dense_rank
df.withColumn("dense_rank",dense_rank().over(windowSpec)) \
    .show()

"""lag"""
from pyspark.sql.functions import lag
df.withColumn("lag",lag("salary",2).over(windowSpec)) \
      .show()

 """lead"""
from pyspark.sql.functions import lead
df.withColumn("lead",lead("salary",2).over(windowSpec)) \
    .show()

