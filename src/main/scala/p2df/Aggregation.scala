package p2df

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{
  approx_count_distinct, avg, col, count, countDistinct, mean, min, stddev, sum
}

object Aggregation extends App {
  val spark = SparkSession.builder()
    .appName("Aggregation and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val movieDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // counting
  val genresCountDf = movieDf.select(count(col("Major_Genre")))
  genresCountDf.show()

  // counting all the rows, and will INCLUDE nulls
  movieDf.select(count("*")).show()

  // count distinct
  movieDf.select(countDistinct(col("Major_Genre"))).show()

  // approximate count
  movieDf.select(approx_count_distinct(col("Major_Genre"))).show()

  // min and max
  movieDf.selectExpr("min(IMDB_Rating)").show()

  // sum
  movieDf.select(sum(col("US_Gross"))).show()
  movieDf.selectExpr("sum(US_Gross)").show()

  // avg
  movieDf.select(avg(col("Rotten_Tomatoes_Rating"))).show()
  movieDf.selectExpr("avg(Rotten_Tomatoes_Rating)").show()

  // data science
  movieDf.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  ).show()

  // Grouping

  movieDf
    .groupBy(col("Major_Genre")) // includes null
    .count() // select count(*) from movieDf group by Major_Genre
    .show()

  movieDf
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")
    .show()

  movieDf
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))
    .show()
}
