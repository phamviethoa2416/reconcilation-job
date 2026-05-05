package catalog

import org.apache.spark.sql.{DataFrame, SparkSession}

object TableCatalog {
  def load(hdfsPath: String)(implicit spark: SparkSession): Map[String, Seq[String]] = {
    val df: DataFrame = spark.read
      .option("multiline", "true")
      .json(hdfsPath)

    val cols = df.columns.toSet
    require(
      cols.contains("database") && cols.contains("tables"),
      s"Table catalog must contain 'database' and 'tables' columns, got: ${cols.mkString(",")}"
    )

    val tablesIdx = df.schema.fieldIndex("tables")
    df.collect()
      .groupBy { row =>
        val raw = row.getAs[Any]("database")
        require(raw != null, "database is null")
        val db = raw.toString.trim.toUpperCase
        require(db.nonEmpty, "database is empty")
        db
      }
      .map { case (db, rows) =>
        db -> rows
          .flatMap { row =>
            if (row.isNullAt(tablesIdx)) Seq.empty
            else {
              row.get(tablesIdx) match {
                case seq: Seq[_] =>
                  seq.map {
                    case null => ""
                    case v => v.toString.trim.toUpperCase
                  }
                case _ =>
                  throw new IllegalArgumentException("tables must be array")
              }
            }
          }
          .filter(_.nonEmpty)
          .distinct
          .toSeq
      }
  }
}
