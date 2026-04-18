package reader

import config.DatabaseConfig
import config.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties
import scala.util.{Failure, Success, Try}

class MySQLMetadataReader(config: DatabaseConfig)(implicit spark: SparkSession) {
  private def jdbcProps: Properties = {
    val props = new Properties()
    props.put("user", config.user)
    props.put("password", config.password)
    props.put("driver", "com.mysql.cj.jdbc.Driver")
    props.put("fetchSize", AppConfig.JDBC_FETCH_SIZE.toString)
    props
  }

  def listTables(): Seq[String] = {
    val excludeSchemas = Seq(
      "information_schema", "mysql", "performance_schema", "sys"
    )

    val excludeClause =
      if (excludeSchemas.nonEmpty)
        excludeSchemas.map(s => s"'$s'").mkString(",")
      else
        "''"

    val query =
      s"""(
         |  SELECT TABLE_SCHEMA AS OWNER, TABLE_NAME
         |  FROM INFORMATION_SCHEMA.TABLES
         |  WHERE TABLE_TYPE   = 'BASE TABLE'
         |    AND TABLE_SCHEMA NOT IN ($excludeClause)
         |) t""".stripMargin

    Try {
      val df = spark.read.jdbc(config.jdbcUrl, query, jdbcProps)

      val tables = df
        .selectExpr("UPPER(CONCAT(OWNER, '.', TABLE_NAME)) AS table")
        .collect()
        .map(_.getString(0))
        .toSeq

      println(s"[INFO] ${config.name} - tables fetched: ${tables.size}")
      tables

    } match {
      case Success(tables) => tables
      case Failure(e) =>
        println(s"[ERROR] fetching table list: ${e.getMessage}")
        Seq.empty
    }
  }

  def getTableSchema(tableName: String): Map[String, String] = {
    val parts = tableName.split("\\.")
    if (parts.length != 2) return Map.empty

    val owner = parts(0)
    val table = parts(1)

    val query =
      s"""(
         |  SELECT COLUMN_NAME, DATA_TYPE
         |  FROM INFORMATION_SCHEMA.COLUMNS
         |  WHERE TABLE_SCHEMA = '$owner'
         |    AND TABLE_NAME   = '$table'
         |  ORDER BY ORDINAL_POSITION
         |) t""".stripMargin

    Try {
      spark.read.jdbc(config.jdbcUrl, query, jdbcProps)
        .collect()
        .map(r => r.getString(0) -> r.getString(1))
        .toMap
    } match {
      case Success(schema) => schema
      case Failure(e) =>
        println(s"[ERROR] getTableSchema $tableName: ${e.getMessage}")
        Map.empty
    }
  }

  def readTable(tableName: String): Option[DataFrame] = {
    Try {
      spark.read
        .format("jdbc")
        .option("url", config.jdbcUrl)
        .option("dbtable", s"(SELECT * FROM $tableName) t")
        .option("user", config.user)
        .option("password", config.password)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("fetchsize", AppConfig.JDBC_FETCH_SIZE.toString)
        .option("numPartitions", AppConfig.JDBC_NUM_PARTITIONS.toString)
        .load()
    } match {
      case Success(df) => Some(df)
      case Failure(e) =>
        println(s"[ERROR] reading table $tableName: ${e.getMessage}")
        None
    }
  }

  def hasData(tableName: String): Boolean = {
    val parts = tableName.split("\\.")
    if (parts.length != 2) {
      println(s"[WARN] Invalid table name format (expected DATABASE.TABLE): $tableName")
      return false
    }

    val existsQuery =
      s"""(
         |  SELECT 1 AS has_row
         |  FROM $tableName
         |  LIMIT 1
         |) t""".stripMargin

    Try {
      spark.read.jdbc(config.jdbcUrl, existsQuery, jdbcProps).limit(1).count() > 0
    } match {
      case Success(hasRows) => hasRows
      case Failure(e) =>
        println(s"[ERROR] checking row existence for $tableName: ${e.getMessage}")
        false
    }
  }
}
