package reconciler

import config.AppConfig
import model.types.SchemaIssue
import model.{RowDiffResult, SchemaFieldDiff}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import utils.DataPrep

object ReconChecks {
  private sealed trait TypeCategory

  private object TypeCategory {
    case object Numeric extends TypeCategory

    case object StringType extends TypeCategory

    case object DateType extends TypeCategory

    case object Binary extends TypeCategory

    case object Boolean extends TypeCategory

    case object Other extends TypeCategory
  }

  private def categorizeOracle(dt: String): TypeCategory = {
    val u = dt.toUpperCase
    if (
      u.contains("NUMBER") || u.contains("FLOAT") || u.contains("BINARY_FLOAT") ||
      u.contains("BINARY_DOUBLE") || u.contains("INTEGER")
    ) TypeCategory.Numeric
    else if (
      u.contains("CHAR") || u.contains("CLOB") || u.contains("VARCHAR") ||
      u == "LONG" || u.contains("ROWID")
    ) TypeCategory.StringType
    else if (u.contains("DATE") || u.contains("TIMESTAMP") || u.contains("INTERVAL")) TypeCategory.DateType
    else if (u.contains("BLOB") || u.contains("RAW") || u.contains("BFILE")) TypeCategory.Binary
    else TypeCategory.Other
  }

  private def categorizeMySQL(dt: String): TypeCategory = {
    val u = dt.toUpperCase
    if (
      u.contains("DECIMAL") || u.contains("NUMERIC") || u.contains("FLOAT") ||
      u.contains("DOUBLE") || u.contains("REAL") || u.contains("INT") ||
      u.contains("YEAR")
    ) TypeCategory.Numeric
    else if (
      u.contains("CHAR") || u.contains("TEXT") || u.contains("ENUM") ||
      u.contains("SET") || u.contains("JSON")
    ) TypeCategory.StringType
    else if (
      u.contains("DATE") || u.contains("TIME") || u.contains("DATETIME") ||
      u.contains("TIMESTAMP")
    ) TypeCategory.DateType
    else if (u.contains("BLOB") || u.contains("BINARY") || u.contains("VARBINARY")) TypeCategory.Binary
    else if (u.contains("BOOLEAN") || u == "BOOL" || u.startsWith("BIT(1)")) TypeCategory.Boolean
    else TypeCategory.Other
  }

  private def categorizeMSSQL(dt: String): TypeCategory = {
    val u = dt.toUpperCase
    if (
      u.contains("BIGINT") || u.contains("INT") || u.contains("SMALLINT") ||
      u.contains("TINYINT") || u.contains("DECIMAL") || u.contains("NUMERIC") ||
      u.contains("MONEY") || u.contains("SMALLMONEY") || u.contains("FLOAT") ||
      u.contains("REAL")
    ) TypeCategory.Numeric
    else if (
      u.contains("CHAR") || u.contains("TEXT") || u.contains("XML") ||
      u.contains("UNIQUEIDENTIFIER")
    ) TypeCategory.StringType
    else if (
      u.contains("DATE") || u.contains("TIME") || u.contains("DATETIME") ||
      u.contains("SMALLDATETIME")
    ) TypeCategory.DateType
    else if (
      u == "TIMESTAMP" || u.contains("BINARY") || u.contains("VARBINARY") ||
      u.contains("IMAGE") || u.contains("ROWVERSION")
    ) TypeCategory.Binary
    else if (u == "BIT") TypeCategory.Boolean
    else TypeCategory.Other
  }

  private def categorizeSource(dbType: String, dt: String): TypeCategory =
    dbType.toLowerCase.trim match {
      case "oracle" => categorizeOracle(dt)
      case "mysql"  => categorizeMySQL(dt)
      case "mssql"  => categorizeMSSQL(dt)
      case other =>
        throw new IllegalArgumentException(s"Unsupported sourceDbType for schema check: $other")
    }

  private def categorizeSpark(dt: String): TypeCategory = {
    val l = dt.toLowerCase
    if (l.startsWith("decimal") || l.contains("int") || l.contains("long") ||
      l.contains("double") || l.contains("float") || l.contains("short") ||
      l.contains("byte")) TypeCategory.Numeric
    else if (l.contains("string")) TypeCategory.StringType
    else if (l.contains("date") || l.contains("timestamp")) TypeCategory.DateType
    else if (l.contains("binary")) TypeCategory.Binary
    else if (l.contains("boolean")) TypeCategory.Boolean
    else TypeCategory.Other
  }

  def checkSchema(
                   tableName: String,
                   sourceType: String,
                   sourceSchema: Map[String, String],
                   sparkSchema: StructType
                 ): Seq[SchemaFieldDiff] = {

    val sourceFields: Map[String, String] =
      sourceSchema.map { case (k, v) => k.toUpperCase -> v.toUpperCase }

    val sinkFields: Map[String, String] =
      sparkSchema.fields.map { f =>
        f.name.toUpperCase -> f.dataType.simpleString.toLowerCase
      }.toMap

    val allFields = (sourceFields.keySet ++ sinkFields.keySet).toSeq.sorted

    allFields.flatMap { f =>
      (sourceFields.get(f), sinkFields.get(f)) match {
        case (None, _) =>
          Some(SchemaFieldDiff(f, SchemaIssue.MISSING_IN_SOURCE))

        case (_, None) =>
          Some(SchemaFieldDiff(f, SchemaIssue.MISSING_IN_SINK))

        case (Some(srcType), Some(spkType)) =>
          val sourceCategory = categorizeSource(sourceType, srcType)
          val spkCategory = categorizeSpark(spkType)

          if (sourceCategory != spkCategory)
            Some(SchemaFieldDiff(f, SchemaIssue.MISMATCH))
          else
            None
      }
    }
  }

  def checkCount(source: DataFrame, sink: DataFrame): (Long, Long) = {
    require(source != null, "source DataFrame is null")
    require(sink != null, "sink DataFrame is null")
    (source.count(), sink.count())
  }

  def checkChecksum(
                     source: DataFrame,
                     sink: DataFrame,
                     bucketCount: Int = AppConfig.CHECKSUM_BUCKET_COUNT
                   ): Seq[Int] = {
    require(bucketCount > 0, s"bucketCount must be positive, got $bucketCount")

    val sourceBuckets = DataPrep.checksumByBucket(source, bucketCount).alias("s")
    val sinkBuckets = DataPrep.checksumByBucket(sink, bucketCount).alias("k")

    val joined = sourceBuckets.join(sinkBuckets, Seq("bucket_id"), "full_outer")
    val filtered =
      col("s.bucket_id").isNull ||
        col("k.bucket_id").isNull ||
        col("s.row_count") =!= col("k.row_count") ||
        col("s.bucket_hash") =!= col("k.bucket_hash")

    joined
      .filter(filtered)
      .select(coalesce(col("s.bucket_id"), col("k.bucket_id")).alias("bucket_id"))
      .collect()
      .flatMap { r =>
        if (r.isNullAt(0)) None else Some(r.getInt(0))
      }
      .toSeq
      .sorted
  }

  def checkRowDiff(
                    sourceDf: DataFrame,
                    sinkDf: DataFrame,
                    diffBuckets: Seq[Int],
                    bucketCount: Int = AppConfig.CHECKSUM_BUCKET_COUNT,
                    sampleSize: Int = AppConfig.ROW_DIFF_SAMPLE_SIZE
                  ): RowDiffResult = {
    require(bucketCount > 0, s"bucketCount must be positive, got $bucketCount")
    require(sampleSize >= 0, s"sampleSize must be non-negative, got $sampleSize")

    if (diffBuckets.isEmpty) {
      return RowDiffResult(0L, 0L, 0L, Array.empty, Array.empty)
    }

    val bucketArr = diffBuckets.toArray

    def withBucket(df: DataFrame): DataFrame =
      df.withColumn(
        "bucket_id",
        pmod(xxhash64(col("row_hash")), lit(bucketCount)).cast(IntegerType)
      ).filter(col("bucket_id").isin(bucketArr: _*))

    val sourceFiltered = withBucket(sourceDf)
    val sinkFiltered = withBucket(sinkDf)

    val sourceGrouped = sourceFiltered
      .groupBy("row_hash")
      .agg(count(lit(1)).cast(LongType).alias("src_count"))

    val sinkGrouped = sinkFiltered
      .groupBy("row_hash")
      .agg(count(lit(1)).cast(LongType).alias("sink_count"))

    val joined = sourceGrouped
      .join(sinkGrouped, Seq("row_hash"), "full_outer")
      .select(
        col("row_hash"),
        coalesce(col("src_count"), lit(0L)).alias("src_count"),
        coalesce(col("sink_count"), lit(0L)).alias("sink_count")
      )
      .filter(col("src_count") =!= col("sink_count"))
      .persist(StorageLevel.MEMORY_AND_DISK)

    try {
      val onlyInSourceCount = joined.filter(col("sink_count") === 0L).count()
      val onlyInSinkCount = joined.filter(col("src_count") === 0L).count()
      val countMismatchCount =
        joined.filter(col("src_count") > 0L && col("sink_count") > 0L).count()

      val (srcSample, snkSample): (Seq[String], Seq[String]) =
        if (sampleSize == 0) {
          (Array.empty, Array.empty)
        } else {
          val onlySrcHashes = joined.filter(col("sink_count") === 0L).select("row_hash")
          val onlySinkHashes = joined.filter(col("src_count") === 0L).select("row_hash")

          val srcRows = sourceFiltered
            .join(broadcast(onlySrcHashes), Seq("row_hash"), "left_semi")
            .drop("bucket_id")
            .limit(sampleSize)
            .collect()

          val sinkRows = sinkFiltered
            .join(broadcast(onlySinkHashes), Seq("row_hash"), "left_semi")
            .drop("bucket_id")
            .limit(sampleSize)
            .collect()

          (srcRows.map(_.mkString("|")).toSeq, sinkRows.map(_.mkString("|")).toSeq)
        }

      RowDiffResult(
        onlyInSourceCount = onlyInSourceCount,
        onlyInSinkCount = onlyInSinkCount,
        countMismatchCount = countMismatchCount,
        sourceSample = srcSample,
        sinkSample = snkSample
      )
    } finally {
      joined.unpersist()
    }
  }
}
