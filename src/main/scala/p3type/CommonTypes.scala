package p3type

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

object CommonTypes extends App {
  val spark = SparkSession.builder()
    .appName("Common Spark Type")
    .config("spark.master", "local")
    .getOrCreate()

  val movieDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // adding a plain value to a DF
  // lit = リテラルの意　　
  movieDF.select(col("Title"), lit(47).as("plain_value")).show()

  // bool
  val dramaFilter = col("Major_Genre") equalTo  "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferedFilter = dramaFilter and goodRatingFilter

  movieDF.select("Title").where(dramaFilter).show()
  movieDF.select(col("Title"), preferedFilter.as("good_movie")).show()
  movieDF.select(col("Title")).where(preferedFilter).show()

  // Numbers

  // math operators
  val moviesAvgRatingDF = movieDF.select(
    col("Title"),
    (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) /2
  )

  // correlation = number between -1 and 1
  println(movieDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating"))

  // regex

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "")

  vwDF.show()

  /**
    * Exercise
    *
    * Filter the cars DF by a list of car names obtained by an API call
    * Versions:
    * - contains
    * - regex
    */
  def getCarName: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")
  val complexRegex = getCarName.map(_.toLowerCase()).mkString("|")

  carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), complexRegex, 0).as("regex_extract")
    ).where(col("regex_extract") =!= "")
    .show()
}
