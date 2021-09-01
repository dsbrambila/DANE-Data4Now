import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum}

object ejemplo1 {
  val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val df = spark.read.json("path")

  df.select("user_id", "income")
    .groupBy("user_id")
    .agg(sum("income"))
    .where(col("user_id").startsWith("10"))

}
