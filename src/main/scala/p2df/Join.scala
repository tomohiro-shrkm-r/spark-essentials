package p2df

import org.apache.spark.sql.SparkSession

object Join extends App {
  val spark = SparkSession.builder()
    .appName("Join")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // INNER Join
  private val condition = guitaristDf.col("band") === bandDf.col("id")
  val innerJoinedDf = guitaristDf.join(
    bandDf,
    condition,
    "inner"
  )

  innerJoinedDf.show()

  // OUTER Join

  val leftJoinedDf = guitaristDf.join(
    bandDf,
    condition,
    "left_outer"
  )

  leftJoinedDf.show()

}
