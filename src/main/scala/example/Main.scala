package example

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, DoubleType, BooleanType, DateType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object Main {
  def main(args: Array[String]): Unit = {
    try {
      val blobAccountKey = dbutils.secrets.get("testScope", "blobAccountKey")
      println(s"blobAccountKey: $blobAccountKey")

      dbutils.fs.mount(
        source = "wasbs://tokyo-olympic-data@tokyoolympicdatademo.blob.core.windows.net",
        mountPoint = "/mnt/tokyo-olympic-data",
        extraConfigs = Map(
          "fs.azure.account.key.tokyoolympicdatademo.blob.core.windows.net" ->
            dbutils.secrets.get(scope = "testScope", key = "blobAccountKey")
        )
      )
      println("Mount successful!")

      val spark = SparkSession.builder().getOrCreate()

      var athletes = spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/mnt/tokyo-olympic-data/raw-data/athletes.csv")

      var coaches = spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/mnt/tokyo-olympic-data/raw-data/coaches.csv")

      var entriesgender = spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/mnt/tokyo-olympic-data/raw-data/entriesgender.csv")

      var medals = spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/mnt/tokyo-olympic-data/raw-data/medals.csv")

      var teams = spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/mnt/tokyo-olympic-data/raw-data/teams.csv")

      athletes.show()
      coaches.show()
      entriesgender.show()
      medals.show()
      teams.show()

      println("Athletes Schema:")
      athletes.printSchema()
      println("Coaches Schema:")
      coaches.printSchema()
      println("Medals Schema:")
      medals.printSchema()
      println("Teams Schema:")
      teams.printSchema()
      println("Entries Gender Schema:")
      entriesgender.printSchema()

      entriesgender
        .withColumn("Female", col("Female").cast(IntegerType))
        .withColumn("Male", col("Male").cast(IntegerType))
        .withColumn("Total", col("Total").cast(IntegerType))

      println("Entries Gender Schema after modification:")
      entriesgender.printSchema()

      val topGoldMedalCountries: DataFrame = medals.orderBy(col("Gold").desc).select("Team_Country", "Gold")
      topGoldMedalCountries.show()

      val averageEntriesByGender = entriesgender
        .withColumn("Avg_Female", col("Female") / col("Total"))
        .withColumn("Avg_Male", col("Male") / col("Total"))
      averageEntriesByGender.show()

      athletes.repartition(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv("/mnt/tokyo-olympic-data/transformed-data/athletes")

      coaches.repartition(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv("/mnt/tokyo-olympic-data/transformed-data/coaches")

      medals.repartition(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv("/mnt/tokyo-olympic-data/transformed-data/medals")

      teams.repartition(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv("/mnt/tokyo-olympic-data/transformed-data/teams")

      topGoldMedalCountries.repartition(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv("/mnt/tokyo-olympic-data/transformed-data/topGoldMedalCountries")

      averageEntriesByGender.repartition(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv("/mnt/tokyo-olympic-data/transformed-data/averageEntriesByGender")

      println("The data has been successfully transformed.")
    } catch {
      case e: Throwable =>
        println(s"An error occurred during mounting: ${e.getMessage}")
    } finally {
      try {
        dbutils.fs.unmount("/mnt/tokyo-olympic-data")
        println("Unmount successful!")
      } catch {
        case unmountError: Throwable =>
          println(s"Error occurred during unmounting: ${unmountError.getMessage}")
      }
      println("Execution completed.")
    }
  }
}