package example

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().getOrCreate()


    val athletes = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/mnt/tokyo-olympic-data/raw-data/athletes.csv")

    val coaches = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/mnt/tokyo-olympic-data/raw-data/coaches.csv")

    val entriesgender = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/mnt/tokyo-olympic-data/raw-data/entriesgender.csv")

    val medals = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/mnt/tokyo-olympic-data/raw-data/medals.csv")

    val teams = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/mnt/tokyo-olympic-data/raw-data/teams.csv")

    athletes.show()
    coaches.show()
    entriesgender.show()
    medals.show()
    teams.show()
  }
}