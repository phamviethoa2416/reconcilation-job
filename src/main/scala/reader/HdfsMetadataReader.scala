package reader

import config.DatabaseConfig
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

class HdfsMetadataReader(config: DatabaseConfig)(implicit spark: SparkSession) {
  def listTables(): Seq[String] = {
    Try {
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val basePath = new Path(config.hdfsBasePath)

      if (!fs.exists(basePath)) {
        println(s"[ERROR] HDFS path does not exist: ${config.hdfsBasePath}")
        Seq.empty[String]
      } else {
        val tables = fs.listStatus(basePath)
          .filter(_.isDirectory)
          .flatMap { ownerStatus =>
            val owner = ownerStatus.getPath.getName.toUpperCase
            fs.listStatus(ownerStatus.getPath)
              .filter(_.isDirectory)
              .map { tableStatus =>
                s"$owner.${tableStatus.getPath.getName.toUpperCase}"
              }
          }
          .toSeq
          .sorted

        println(s"[INFO] ${config.name} - tables fetched: ${tables.size}")
        tables
      }
    } match {
      case Success(tables) => tables
      case Failure(e) =>
        println(s"[ERROR] fetching table list from HDFS: ${e.getMessage}")
        Seq.empty[String]
    }
  }

  def readTable(tableName: String): Option[DataFrame] = {
    val parts = tableName.split("\\.")
    if (parts.length != 2) {
      println(s"[ERROR] Invalid table name format for HDFS path (expected OWNER.TABLE): $tableName")
      return None
    }
    val owner = parts(0).toUpperCase
    val table = parts(1).toUpperCase
    val path = s"${config.hdfsBasePath.stripSuffix("/")}/$owner/$table"

    Try(spark.read.parquet(path)) match {
      case Success(df) =>
        Some(df)
      case Failure(e) =>
        println(s"[ERROR] reading table $tableName from HDFS ($path): ${e.getMessage}")
        None
    }
  }
}
