package example

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, DoubleType, BooleanType, DateType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.MetadataBuilder

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

      val expectedSchema = StructType(
        Seq(
          StructField("PersonName", StringType, nullable = true),
          StructField("Country", StringType, nullable = true),
          StructField("Discipline", StringType, nullable = true),
        )
      )

      var athletes = spark.read
        .format("parquet")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/mnt/tokyo-olympic-data/level2/athletes.parquet")

      var coaches = spark.read
        .format("parquet")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/mnt/tokyo-olympic-data/level2/coaches.parquet")

      var entriesgender = spark.read
        .format("parquet")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/mnt/tokyo-olympic-data/level2/entriesgender.parquet")

      var medals = spark.read
        .format("parquet")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/mnt/tokyo-olympic-data/level2/medals.parquet")

      var teams = spark.read
        .format("parquet")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/mnt/tokyo-olympic-data/level2/teams.parquet")

      val realSchema = athletes.schema

      // Validate column names
      val expectedColumnNames = expectedSchema.map(_.name).toSet
      val actualColumnNames = realSchema.map(_.name).toSet
      val columnNamesMatch = expectedColumnNames == actualColumnNames

      // Validate data types for each column
      val columnTypeMatches = expectedSchema.forall { expectedField =>
        val realField = realSchema.find(_.name == expectedField.name)
        realField match {
          case Some(field) => field.dataType == expectedField.dataType
          case None => false
        }
      }

      // Check if all column names and types match the expected schema
      val schemaValid = columnNamesMatch && columnTypeMatches

      println(expectedSchema)
      println(realSchema)
      println(s"Expected Column Names: ${expectedColumnNames}")
      println(s"Actual Column Names: ${actualColumnNames}")
      println(s"Column Names Match: ${columnNamesMatch}")
      println(s"Column Type Match: ${columnTypeMatches}")

      if (schemaValid) {
        println("The schemas match!")
      } else {
        println("The schemas do not match.")
        if (!columnNamesMatch) {
          println("Column names are different:")
          val missingColumns = expectedColumnNames -- actualColumnNames
          val extraColumns = actualColumnNames -- expectedColumnNames
          if (missingColumns.nonEmpty) {
            println(s"Missing columns: ${missingColumns.mkString(", ")}")
          }
          if (extraColumns.nonEmpty) {
            println(s"Extra columns: ${extraColumns.mkString(", ")}")
          }
        }
        if (!columnTypeMatches) {
          println("Column data types are different:")
          expectedSchema.foreach { expectedField =>
            val realField = realSchema.find(_.name == expectedField.name)
            realField match {
              case Some(field) =>
                if (field.dataType != expectedField.dataType) {
                  println(s"Column '${expectedField.name}' has an unexpected data type. Expected: ${expectedField.dataType}, Actual: ${field.dataType}")
                }
              case None =>
                println(s"Column '${expectedField.name}' is missing in the actual schema.")
            }
          }
        }
      }

      athletes.show()
      coaches.show()
      entriesgender.show()
      teams.show()
      medals.show()

      val renamedMedalData = medals.withColumnRenamed("Rank by Total", "Rank_by_Total")
      println("Successfully renamed the Rank by  column to Rank_by_Total.")
      renamedMedalData.show()
      // Define the metadata with the tag
      val metadata = new MetadataBuilder().putString("tag", "this column has been modified").build()

      // Add metadata to the column "c_mod"
      val dataWithMetadata = renamedMedalData.withColumn(
        "Rank_by_Total",
        col("Rank_by_Total").as("Rank_by_Total", metadata)
      )

      // Show the DataFrame with metadata added to the column
      dataWithMetadata.show()

      // Remove duplicate rows
      val dfWithoutDuplicates = renamedMedalData.dropDuplicates()

      // Show the DataFrame without duplicates
      println("DataFrame after removing duplicates:")
      dfWithoutDuplicates.show()

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
      entriesgender.show()

      entriesgender
        .withColumn("Female", col("Female").cast(IntegerType))
        .withColumn("Male", col("Male").cast(IntegerType))
        .withColumn("Total", col("Total").cast(IntegerType))

      println("Entries Gender Schema after modification:")

      val averageEntriesByGender = entriesgender
        .withColumn("Avg_Female", col("Female") / col("Total"))
        .withColumn("Avg_Male", col("Male") / col("Total"))
      averageEntriesByGender.printSchema()
      averageEntriesByGender.show()

      val topGoldMedalCountries: DataFrame = medals.orderBy(col("Gold").desc).select("Team_Country", "Gold")
      topGoldMedalCountries.show()

      athletes.repartition(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .parquet("/mnt/tokyo-olympic-data/level3/athletes")

      coaches.repartition(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .parquet("/mnt/tokyo-olympic-data/level3/coaches")

      renamedMedalData.repartition(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .parquet("/mnt/tokyo-olympic-data/level3/medals")

      teams.repartition(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .parquet("/mnt/tokyo-olympic-data/level3/teams")

      averageEntriesByGender.repartition(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .parquet("/mnt/tokyo-olympic-data/level3/entriesgender")

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