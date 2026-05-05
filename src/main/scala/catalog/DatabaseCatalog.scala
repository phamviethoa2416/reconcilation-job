package catalog

import config.DatabaseConfig
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DatabaseCatalog {
  def load(hdfsPath: String)(implicit spark: SparkSession): Seq[DatabaseConfig] = {
    val df: DataFrame = spark.read
      .option("multiline", "true")
      .json(hdfsPath)

    val requiredCols = Seq("name", "dbType", "jdbcUrl", "user", "password", "driver", "hdfsBasePath")
    val missing = requiredCols.filterNot(col => df.columns.contains(col))
    require(missing.isEmpty, s"Missing required columns in database catalog: ${missing.mkString(", ")}")

    val idxName = df.schema.fieldIndex("name")
    val idxDbType = df.schema.fieldIndex("dbType")
    val idxJdbcUrl = df.schema.fieldIndex("jdbcUrl")
    val idxUser = df.schema.fieldIndex("user")
    val idxPassword = df.schema.fieldIndex("password")
    val idxDriver = df.schema.fieldIndex("driver")
    val idxHdfsBasePath = df.schema.fieldIndex("hdfsBasePath")

    def readRequired(row: Row, idx: Int, fieldName: String): String = {
      require(!row.isNullAt(idx), s"Null value for required field '$fieldName'")
      val value = row.getString(idx).trim
      require(value.nonEmpty, s"Empty value for required field '$fieldName'")
      value
    }

    df.collect().toSeq
      .map { row =>
        DatabaseConfig(
          name = readRequired(row, idxName, "name"),
          dbType = readRequired(row, idxDbType, "dbType").toLowerCase,
          jdbcUrl = readRequired(row, idxJdbcUrl, "jdbcUrl"),
          user = readRequired(row, idxUser, "user"),
          password = readRequired(row, idxPassword, "password"),
          driver = readRequired(row, idxDriver, "driver"),
          hdfsBasePath = readRequired(row, idxHdfsBasePath, "hdfsBasePath")
        )
      }
      .filter(_.name.nonEmpty)
  }
}
