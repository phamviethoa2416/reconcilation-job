package utils

import config.AppConfig
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

class CheckpointManager(runId: String)(implicit spark: SparkSession) {

  private val checkpointDir = s"${AppConfig.CHECKPOINT_HDFS_PATH}/$runId"

  private val fs: FileSystem =
    FileSystem.get(spark.sparkContext.hadoopConfiguration)

  private def tablePath(dbName: String, tableName: String): Path =
    new Path(s"$checkpointDir/$dbName/$tableName")

  private def donePath(dbName: String, tableName: String): Path =
    new Path(s"$checkpointDir/$dbName/$tableName/_DONE")

  def markDone(dbName: String, tableName: String): Unit = {
    val dir = tablePath(dbName, tableName)

    if (!fs.exists(dir)) {
      fs.mkdirs(dir)
    }

    val out = fs.create(donePath(dbName, tableName), true)
    out.close()

    println(s"[CHECKPOINT] DONE: $dbName.$tableName")
  }

  def isDone(dbName: String, tableName: String): Boolean = {
    fs.exists(donePath(dbName, tableName))
  }

  def clearAll(): Unit = {
    val path = new Path(checkpointDir)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
  }
}