package p3type

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.columnar.STRUCT
import org.apache.spark.sql.functions._

object ComplexTypes extends App {
  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()

  val movieDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Date
  movieDF.select(
    col("Title"),
    col("Release_Date"),
    to_date(col("Release_Date"), "d-MMM-yy").as("Actual_Release")
  ).withColumn("Today", current_date())
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365)
    .show()

  // Structure(Struct) - group of columns aggregated 1
  movieDF.select(
    col("Title"),
    struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit")
  ).show()

  movieDF.select(
    col("Title"),
    struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit")
  ).select(
    col("Title"), col("Profit").getField("US_Gross").as("US_Profit")
  ).show()

  // 2 - with expression strings
  movieDF
    .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")

  // Arrays

  val moviesWithWords = movieDF.select(
    col("Title"),
    split(col("Title"), " |,").as("Title_Words")
  ) // ARRAY of strings

  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"), // indexing
    size(col("Title_Words")), // array size
    array_contains(col("Title_Words"), "Love") // look for value in array
  ).show()
}
