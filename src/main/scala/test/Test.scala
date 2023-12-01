import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Test {

  final val SAS: String  = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-12-29T16:56:18Z&st=2023-11-29T08:56:18Z&spr=https&sig=0SxOd7tpvtXH2K4Fsj4z1GOzAlsKYLEgP7x3UDO515Q%3D"
  final val INPUT_MOUNT_POINT: String = "/mnt/inputFile"
  final val EXPECTED_MOUNT_POINT: String = "/mnt/expectedFile"
  final val NEW_COLUMN_NAME = "cc_mod"
  final val TAG = "this column has been modified"
  final val FINAL_MOUNT_POINT = "/mnt/finalFile"
  final val SCOPE_NAME_KEY_VAULT = "petertmasecretatkeyvault"

  def test(array: Array[String]): Unit = {

    try {
      val spark = SparkSession.builder()
        .getOrCreate()

      checkPointExist("userdata1.parquet", INPUT_MOUNT_POINT)
      checkPointExist("userdata3.parquet", EXPECTED_MOUNT_POINT)

      //Read data
      readData(spark)

      //Validate data
      validateData(spark)

      //Rename column
      val renamedDF = renamedColumn(spark, NEW_COLUMN_NAME)
      renamedDF.printSchema()

      //Add tag
      addTagToMetadataOfColumn(renamedDF)

      //Remove duplicate rows
      val finalDF = removeDuplicateRows(renamedDF)

      //Write transformed data to parquet file to level 3 container
      println("Write transformed data ti parquet file to level 3 container -----------------------------------")
      checkPointExist(fileName = "", path = FINAL_MOUNT_POINT, containerName = "level3")
      finalDF.write.mode(SaveMode.Overwrite).parquet(FINAL_MOUNT_POINT)
      println("Created! -----------------------------------")

      //Write transformed data to Data Warehouse
      println("Start write transformed data to Data Warehouse -----------------------------------")
      writeDataToDataWarehouse(finalDF)
      println("Wrote! -----------------------------------")

    } finally {
      val cleanupThread = new Thread {
        override def run(): Unit = jobCleanup()
      }
      Runtime.getRuntime.addShutdownHook(cleanupThread)
    }
  }

  private def writeDataToDataWarehouse(dataFrame: DataFrame): Unit = {

    val sc = SparkContext.getOrCreate()
    val storageKey = dbutils.secrets.get("StorageScopeName", "StorageScopeName")
    val sqlPoolUser = dbutils.secrets.get("StorageScopeName", "SQLToolScopeName")
    val sqlPoolPassword = dbutils.secrets.get("StorageScopeName", "SQLToolPasswordScope")

    sc.hadoopConfiguration.set(s"fs.azure.account.key.petertmastorage.dfs.core.windows.net", storageKey)

    val sqlDwUrl = s"jdbc:sqlserver://petertma190.sql.azuresynapse.net:1433;database=petertmasqlpool;user=$sqlPoolUser;password=$sqlPoolPassword;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;"
    val tempDir = "abfss://level3@petertmastorage.dfs.core.windows.net/finalUser"

    dataFrame.write
      .format("com.databricks.spark.sqldw")
      .option("url", sqlDwUrl)
      .option("forwardSparkAzureStorageCredentials", "true")
      .option("dbTable", "USERDATA")
      .option("tempDir", tempDir)
      .mode(SaveMode.Overwrite)
      .save()
  }

  private def removeDuplicateRows(dataFrame: DataFrame): DataFrame = {
    println("REMOVE DUPLICATE ROWS -----------------------------------")
    println(s"BEFORE REMOVING HAS ${dataFrame.count()} ITEMS")
    val distinctDF = dataFrame.distinct()
    val duplicateItems = dataFrame.count() - distinctDF.count()
    if (duplicateItems > 0) {
      println(s"$duplicateItems DUPLICATE ITEMS EXIST")
    } else {
      println("NO ANY DUPLICATE ITEMS")
    }
    println(s"AFTER REMOVING HAS ${dataFrame.distinct().count()} ITEMS")
    distinctDF
  }

  private def renamedColumn(sparkSession: SparkSession, newColumnName: String): DataFrame = {
    println("RENAME COLUMN FROM cc TO " + newColumnName + " -----------------------------------")
    val mydf = sparkSession.read.parquet(INPUT_MOUNT_POINT)
    mydf.withColumnRenamed("cc", newColumnName)
  }

  private def addTagToMetadataOfColumn(dataFrame: DataFrame): Unit = {
    println("ADD TAG: " + TAG + " TO METADATA OF cc_mod COLUMN -----------------------------------")
    val metadata = new MetadataBuilder().putString("tag", TAG).build()
    val newColumn = dataFrame.col(NEW_COLUMN_NAME).as(NEW_COLUMN_NAME, metadata)
    dataFrame.withColumn(NEW_COLUMN_NAME, newColumn).schema.foreach(e => println(s"Column name: ${e.name} - Metadata: ${e.metadata}"))
  }


  private def jobCleanup(): Unit = {
    unmount(INPUT_MOUNT_POINT)
    unmount(EXPECTED_MOUNT_POINT)
    unmount(FINAL_MOUNT_POINT)
  }

  private def getConfig(containerName: String): String = {
    val storageAccountName = "petertmastorage"
    "fs.azure.sas." + containerName + "." + storageAccountName + ".blob.core.windows.net"
  }

  private def checkPointExist(fileName: String, path: String, containerName: String = "level2"): Unit = {
    var isExist = false
    val mounts = dbutils.fs.mounts()
    mounts.foreach(e => {
      if (e.mountPoint == path) isExist = true
    })
    if (!isExist) {
      mount(fileName, path, containerName)
    }
  }


  private def mount(fileName: String, mountPoint: String, containerName: String): Unit = {
    dbutils.fs.mount(
      source = "wasbs://" + containerName + "@petertmastorage.blob.core.windows.net/" + fileName,
      mountPoint = mountPoint,
      extraConfigs = Map(getConfig(containerName) -> SAS))
  }

  private def unmount(mountPoint: String): Unit = {
    dbutils.fs.unmount(mountPoint)
  }

  private def readData(sparkSession: SparkSession): Unit = {
    println("READ DATA -----------------------------------")
    val mydf = sparkSession.read.parquet(INPUT_MOUNT_POINT)
    mydf.printSchema()
  }

  private def validateData(sparkSession: SparkSession): Unit = {
    println("VALIDATE  -----------------------------------")
    val inputFile = sparkSession.read.parquet(INPUT_MOUNT_POINT)
    val expected = sparkSession.read.parquet(EXPECTED_MOUNT_POINT)

    val inputSchema = inputFile.schema.fields.map(e => (e.name, e.dataType))
    val expectedSchema = expected.schema.fields.map(e => (e.name, e.dataType))

    if (inputSchema.length != expectedSchema.length || !inputSchema.sameElements(expectedSchema)) {
      println("Does not match")
    } else {
      println("Match")
    }
  }
}