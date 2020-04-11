package p2df

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
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
    StructField("Year", StringType),
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
    .schema(carsSchema)  // OR .option("inferSchema", "true")
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

}
