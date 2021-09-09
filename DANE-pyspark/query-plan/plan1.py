import pyspark.sql.functions as F
from pyspark.sql.functions import year, to_date

names_df = spark.read.option("header",True).csv("names.csv")
counts_df = spark.read.option("header",True).csv("counts.csv")

names_year_df = names_df.withColumn("year", year(to_date("Date", "yyyy/mm/DD")))

names_2019 = names_year_df.filter(names_year_df.year == 2019)
names_2020 = names_year_df.filter(names_year_df.year == 2020)
names_2021 = names_year_df.filter(names_year_df.year == 2021)

cons_2019 = names_2019.join(counts_df, names_2019.year == counts_df.Year)
cons_2020 = names_2020.join(counts_df, names_2020.year == counts_df.Year)
cons_2021 = names_2021.join(counts_df, names_2021.year == counts_df.Year)

cons_2019.union(cons_2020).union(cons_2021).show()

cons_total = names_year_df.join(counts_df, names_year_df.year == counts_df.Year)
cons_total.show()
