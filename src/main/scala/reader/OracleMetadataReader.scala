package reader

import config.DatabaseConfig
import config.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties
import scala.util.{Failure, Success, Try}

class OracleMetadataReader(config: DatabaseConfig)(implicit spark: SparkSession) extends JdbcMetadataReader {
  private def jdbcProps: Properties = {
    val props = new Properties()
    props.put("user", config.user)
    props.put("password", config.password)
    props.put("driver", "oracle.jdbc.driver.OracleDriver")
    props.put("fetchsize", AppConfig.JDBC_FETCH_SIZE.toString)

    props
  }

  def listTables(): Seq[String] = {
    val excludeSchemas = Seq(
      "SYS", "SYSTEM", "XDB", "MDSYS", "CTXSYS",
      "DBSNMP", "OUTLN", "SYSMAN", "AUDSYS",
      "APPQOSSYS", "WMSYS", "OLAPSYS",
      "ORDSYS", "ORDDATA", "ORDPLUGINS",
      "DVSYS", "LBACSYS", "EXFSYS",
      "GSMADMIN_INTERNAL", "DBSFWUSER",
      "ANONYMOUS", "SI_INFORMTN_SCHEMA"
    )

    val excludeClause =
      if (excludeSchemas.nonEmpty)
        excludeSchemas.map(s => s"'${s.toUpperCase}'").mkString(",")
      else
        "''"

    val query =
      s"""(
       SELECT OWNER, TABLE_NAME
       FROM ALL_TABLES
       WHERE OWNER NOT IN ($excludeClause)
         AND OWNER NOT LIKE 'APEX_%'
         AND OWNER NOT LIKE 'FLOWS_%'
         AND OWNER NOT LIKE 'C##%'
     ) t"""

    Try {
      val df = spark.read.jdbc(config.jdbcUrl, query, jdbcProps)

      val tables = df
        .selectExpr("UPPER(OWNER || '.' || TABLE_NAME) as table")
        .collect()
        .map(_.getString(0))
        .toSeq

      println(s"[INFO] ${config.name} - tables fetched: ${tables.size}")

      tables

    } match {
      case Success(tables) => tables
      case Failure(exception) =>
        println(s"Error fetching table list: ${exception.getMessage}")
        Seq.empty[String]
    }
  }

  def getTableSchema(tableName: String): Map[String, String] = {
    val parts = tableName.split("\\.")
    if (parts.length != 2) return Map.empty

    val owner = parts(0)
    val table = parts(1)

    val query =
      s"""(
         SELECT COLUMN_NAME, DATA_TYPE
         FROM ALL_TAB_COLUMNS
         WHERE OWNER = '$owner'
           AND TABLE_NAME = '$table'
         ORDER BY COLUMN_ID
       ) t"""

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
        .option("driver", "oracle.jdbc.driver.OracleDriver")
        .option("fetchsize", AppConfig.JDBC_FETCH_SIZE.toString)
        .option("numPartitions", AppConfig.JDBC_NUM_PARTITIONS.toString)
        .load()
    } match {
      case Success(df) => Some(df)
      case Failure(exception) =>
        println(s"Error reading table ${tableName}: ${exception.getMessage}")
        None
    }
  }

  def hasData(tableName: String): Boolean = {
    val parts = tableName.split("\\.")
    if (parts.length != 2) {
      println(s"[WARN] Invalid table name format (expected OWNER.TABLE): $tableName")
      return false
    }

    val owner = parts(0)
    val table = parts(1)
    val existsQuery =
      s"""(
         |SELECT 1
         |FROM $owner.$table
         |WHERE ROWNUM = 1
         |) t""".stripMargin

    Try {
      spark.read.jdbc(config.jdbcUrl, existsQuery, jdbcProps).limit(1).count() > 0
    } match {
      case Success(hasRows) =>
        hasRows
      case Failure(e) =>
        println(s"[ERROR] checking row existence for $tableName: ${e.getMessage}")
        false
    }
  }
}