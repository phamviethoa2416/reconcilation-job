package utils

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}

object DataPrep {
  private val NullSentinel: String = "\u0001__NULL__\u0001"
  private val FieldDelimiter: String = "\u001E"

  private def normalizeCol(name: String, dt: DataType): Column = {
    val c = col(name)

    dt match {
      case StringType =>
        when(c.isNull, lit(null).cast(StringType))
          .otherwise(upper(trim(c)))

      case ByteType | ShortType | IntegerType | LongType =>
        c.cast(StringType)

      case FloatType | DoubleType | _: DecimalType =>
        when(c.isNull, lit(null).cast(StringType))
          .otherwise(c.cast(DecimalType(38, 10)).cast(StringType))

      case DateType =>
        when(c.isNull, lit(null).cast(StringType))
          .otherwise(date_format(c, "yyyy-MM-dd"))

      case TimestampType =>
        when(c.isNull, lit(null).cast(StringType))
          .otherwise(date_format(c, "yyyy-MM-dd HH:mm:ss.SSS"))
      case BooleanType =>
        c.cast(StringType)

      case BinaryType =>
        when(c.isNull, lit(null).cast(StringType))
          .otherwise(base64(c))

      case _ =>
        when(c.isNull, lit(null).cast(StringType))
          .otherwise(trim(c.cast(StringType)))
    }
  }

  def prepare(df: DataFrame): DataFrame = {
    require(df != null, "DataFrame must not be null")

    val upperSortedCols: Array[String] = df.columns.map(_.toUpperCase).sorted
    val upperNames = df.columns.map(_.toUpperCase)
    require(
      upperNames.distinct.length == upperNames.length,
      s"DataFrame has columns that collide when uppercased: ${upperNames.mkString(",")}"
    )

    val normalized: DataFrame = df.select(
      df.schema.fields.map { f =>
        normalizeCol(f.name, f.dataType).alias(f.name.toUpperCase)
      }: _*
    )

    val reordered: DataFrame = normalized.select(upperSortedCols.map(col): _*)

    reordered.withColumn(
      "row_hash",
      sha2(
        concat_ws(
          FieldDelimiter,
          upperSortedCols.map(c => coalesce(col(c), lit(NullSentinel))): _*
        ),
        256
      )
    )
  }

  def checksumByBucket(df: DataFrame, bucketCount: Int): DataFrame = {
    require(bucketCount > 0, s"bucketCount must be positive, got $bucketCount")
    require(
      df.columns.contains("row_hash"),
      "DataFrame must contain 'row_hash' column (call prepare() first)"
    )

    df.withColumn(
        "bucket_id",
        pmod(xxhash64(col("row_hash")), lit(bucketCount)).cast(IntegerType)
      )
      .groupBy("bucket_id")
      .agg(
        count(lit(1)).alias("row_count"),
        sum(xxhash64(col("row_hash")).cast(DecimalType(38, 0))).alias("bucket_hash")
      )
  }
}
