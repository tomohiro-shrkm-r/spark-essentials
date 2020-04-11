package p2df

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ColumnAndExpressions extends App {
  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  carsDF.show()

  // Columns
  val firstColumn = carsDF.col("Name")
  // selecting (projecting)
  val carNamesDF = carsDF.select(firstColumn)

  // various select methods

  import spark.implicits._

  carsDF.select(
    col("Acceleration"), // org.apache.spark.sql.functions._ が必要
    column("Name"), // org.apache.spark.sql.functions._ が必要。colと同じ
    'Year, // import spark.implicits._ が必要
    $"Horsepower", // interpolated string, returns a Column object
    expr("Origin")
  )

  // select with plain column names
  carsDF.select("Name", "Year")

  // expressions
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDf = carsDF.select(
    col("Name"),
    simplestExpression,
    weightInKgExpression.as("Weight_in_kg"), // DoubleType
    expr("Weight_in_lbs / 2.2").as("weigh_in_kg_2") // DecimalType
  )

  carsWithWeightsDf.show()
  println(carsWithWeightsDf.schema)

  // selectExpr
  val carsWithSelectExprWeightDf = carsDF.selectExpr(
    "name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  /**
    * DF processing
    */
  // adding a column
  val carsWithKg3Df = carsDF.withColumn(
    "weigh_in_kg_3",
    col("Weight_in_lbs") / 2.2
  )
  // rename a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed(
    "Weight_in_lbs",
    "Weight_in_pounds"
  )
  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // filtering
  // （!= はScalaの標準と被るので =!= であり、== は、同様に===となる）
  val europeanCarsDf = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDf2 = carsDF.where((col("Origin") =!= "USA"))

  // filter with expressions strings
  val americanCarsDf = carsDF.filter("Origin = 'USA'")

  // chain filteres
  val americanPowerfulCarsDf = carsDF
    .filter((col("Origin") === "USA"))
    .filter(col("Horsepower") > 150)
  val americanPowerfulCarsDf2 = carsDF
    .filter((col("Origin") === "USA") and col("Horsepower") > 150)
  val americanPowerfulCarsDf3 = carsDF
    .filter("Origin = 'USA' and Horsepower > 150")

  // union (adding more rows)
  val moreCarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/more_cars.json")
  val allCarsDf = carsDF.union(moreCarsDF)

  // distinct values
  val allCountriesDf = carsDF.select("Origin").distinct()

  allCountriesDf.show()

  /**
    * Exercise
    *
    * 1. read the movie DF and select 2 col of my choice
    * 2. create another col summing up the total profit of the movie = us_gross + world...
    * 3. select all comedy movies with IMDB rating 6
    */

  // 0. prepare
  val moviesDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")
  moviesDf.show()

  // 1.
  val selectedMoviesDf = moviesDf.select("US_Gross", "Worldwide_Gross")
  selectedMoviesDf.show()

  //2.
  val selectAndAddMoviesDf = moviesDf.select(
    col("US_Gross"),
    col("Worldwide_Gross"),
    expr("US_Gross + Worldwide_Gross").as("sum_up")
  )
  selectAndAddMoviesDf.show()

  //3.
  val filteredMoviesDf = moviesDf.filter(
    "Major_Genre = 'Comedy' and IMDB_Rating > 6 "
  )
  filteredMoviesDf.show()
}
