import pyspark.sql.functions as F
from pyspark.sql.functions import udf
import pyspark.sql.types as T
from fuzzywuzzy import fuzz
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, LongType
import numpy as np


def match_string(s1,s2):
  return fuzz.ratio(s1,s2)

df = spark.read.option("header",True).csv("names.csv")

MatchUDF = udf(match_string,IntegerType())
cross_joined = df.select(F.col("name").alias("name1")).crossJoin(df)

cj_scores = cross_joined.withColumn("score", MatchUDF(F.col("Name"),F.col("Name1")))


