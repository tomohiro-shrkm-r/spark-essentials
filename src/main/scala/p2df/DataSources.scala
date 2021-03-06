package p2df

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}
import part2dataframes.DataFramesBasics.spark

object DataSources extends App {
  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /**
    * Reading a DF
    * - format
    * - schema (optional) or inferSchema = true
    * - zero or more options
    *
    * オプションのmodeがfailFastの場合、おかしなデータがあるとエラーで処理が止まる
    * （止めることができる、とも言える。例えば、数値が入る箇所に文字列が入ってくるとかの場合。）
    */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema) // OR .option("inferSchema", "true")
    .option("mode", "failFast") // dropMalformed, permissive (default)
    .option("path", "src/main/resources/data/cars.json")
    .load()

  // オプションがたくさんある時はMapを使って簡略に書ける
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  /**
    * Writing DFs
    * - format
    * - save mode = overwrite, append, ignore, errorIfExists
    * - path
    * - zero or more options
    */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_duplicate.json")

  // JSON flags
  spark.read
    .format("json")
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd") // couple with schema; if spark fails parsing, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // gzip,,,,etc
    .json("src/main/resources/data/cars.json")

  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
    .format("csv")
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",") // sep = セパレータ
    .option("nullValue", "")
    .csv("src/main/resources/data/")

  // Parquet
  carsDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet") // saveのデフォルトはパーケ形式なので、saveだけでよい

  // text file
  spark.read
    .text("src/main/resources/data/text.txt").show()

  private val driver = "org.postgresql.Driver"
  private val url = "jdbc:postgresql://localhost:5432/rtjvm"
  private val user = "docker"
  private val pass = "docker"
  // reading from a remote DB
  val empDf = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", pass)
    .option("dbtable", "public.employees")
    .load()

  empDf.show()

  /**
    * Exercise read the movie DF, then write it as
    * - tab-separated value file
    * - snappy Parquet
    * - table public.movies in the postgres DB
    */

  val moviesDf = spark.read.json("src/main/resources/data/movies.json")

  // tsv
  moviesDf.write
    .format("csv")
    .option("header", "true")
    .option("sep", ",")
    .save("src/main/resources/data/movies.csv")

  // parquet
  moviesDf.write.save("src/main/resources/data/movies.csv")

  // save to DB
  moviesDf.write
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", pass)
    .option("dbtable", "public.movies")
    .save()
}
