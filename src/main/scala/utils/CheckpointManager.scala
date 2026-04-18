package utils

import config.AppConfig
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

class CheckpointManager(runId: String)(implicit spark: SparkSession) {

  private val checkpointDir = s"${AppConfig.CHECKPOINT_HDFS_PATH}/$runId"

  private val fs: FileSystem =
    FileSystem.get(spark.sparkContext.hadoopConfiguration)

  private def tablePath(dbName: String, tableName: String): Path =
    new Path(s"$checkpointDir/${dbName.toUpperCase}/${tableName.toUpperCase}")

  private def donePath(dbName: String, tableName: String): Path =
    new Path(s"${tablePath(dbName, tableName)}/_DONE")

  private def inProgressPath(dbName: String, tableName: String): Path =
    new Path(s"${tablePath(dbName, tableName)}/_IN_PROGRESS")

  private def failedPath(dbName: String, tableName: String): Path =
    new Path(s"${tablePath(dbName, tableName)}/_FAILED")

  private def withRetry[T](maxRetries: Int = 3)(fn: => T): T = {
    var attempts = 0
    var lastError: Throwable = null

    while (attempts < maxRetries) {
      try {
        return fn
      } catch {
        case e: Throwable =>
          lastError = e
          attempts += 1
          println(s"[CHECKPOINT] Retry $attempts/$maxRetries due to error: ${e.getMessage}")
          Thread.sleep(500)
      }
    }
    throw lastError
  }

  def markDone(dbName: String, tableName: String): Unit = {
    val dir = tablePath(dbName, tableName)

    withRetry() {
      if (!fs.exists(dir)) {
        fs.mkdirs(dir)
      }

      fs.delete(inProgressPath(dbName, tableName), false)
      fs.delete(failedPath(dbName, tableName), false)

      val out = fs.create(donePath(dbName, tableName), true)
      out.close()
    }

    println(s"[CHECKPOINT] DONE: $dbName.$tableName")
  }

  def markInProgress(dbName: String, tableName: String): Unit = {
    val dir = tablePath(dbName, tableName)

    withRetry() {
      if (!fs.exists(dir)) {
        fs.mkdirs(dir)
      }

      fs.delete(donePath(dbName, tableName), false)
      fs.delete(failedPath(dbName, tableName), false)

      val out = fs.create(inProgressPath(dbName, tableName), true)
      out.close()
    }

    println(s"[CHECKPOINT] IN_PROGRESS: $dbName.$tableName")
  }

  def markFailed(dbName: String, tableName: String): Unit = {
    val dir = tablePath(dbName, tableName)

    withRetry() {
      if (!fs.exists(dir)) {
        fs.mkdirs(dir)
      }

      fs.delete(inProgressPath(dbName, tableName), false)

      val out = fs.create(failedPath(dbName, tableName), true)
      out.close()
    }

    println(s"[CHECKPOINT] FAILED: $dbName.$tableName")
  }

  def isDone(dbName: String, tableName: String): Boolean = {
    fs.exists(donePath(dbName, tableName))
  }

  def isInProgress(dbName: String, tableName: String): Boolean = {
    fs.exists(inProgressPath(dbName, tableName))
  }

  def isFailed(dbName: String, tableName: String): Boolean = {
    fs.exists(failedPath(dbName, tableName))
  }

  def clearAll(): Unit = {
    val path = new Path(checkpointDir)

    println(s"[CHECKPOINT] CLEAR ALL: $checkpointDir")

    withRetry() {
      if (fs.exists(path)) {
        fs.delete(path, true)
      }
    }
  }
}