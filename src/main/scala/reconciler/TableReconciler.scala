package reconciler

import config.{AppConfig, DatabaseConfig}
import model.{CheckStatus, TableReconResult}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import reader.{HdfsMetadataReader, OracleMetadataReader}
import utils.DataPrep

import scala.collection.mutable.ListBuffer

class TableReconciler(
                       dbConfig: DatabaseConfig,
                       sourceReader: OracleMetadataReader,
                       sinkReader: HdfsMetadataReader,
                       runId: String,
                       runTime: String
                     )(implicit spark: SparkSession) {

  def reconcile(tableName: String): Seq[TableReconResult] = {
    val results = ListBuffer[TableReconResult]()

    def makeResult(
                    checkType: String,
                    status: CheckStatus,
                    source: String = "",
                    sink: String = "",
                    detail: String = "",
                    ms: Long = 0L
                  ) =
      TableReconResult(runId, runTime, dbConfig.name, tableName,
        checkType, status.toString, source, sink, detail, ms)

    println(s"[INFO] Reconciling table: ${dbConfig.name}.$tableName")

    val t0 = System.currentTimeMillis()
    val sourceRawOpt = sourceReader.readTable(tableName)
    if (sourceRawOpt.isEmpty) {
      results += makeResult("READ_SOURCE", CheckStatus.ERROR, detail = s"Failed to read source table: $tableName")
      return results
    }
    val sourceRaw = sourceRawOpt.get

    val sinkRawOpt = sinkReader.readTable(tableName)
    if (sinkRawOpt.isEmpty) {
      results += makeResult("READ_SINK", CheckStatus.ERROR, detail = s"Failed to read sink table: $tableName")
      return results
    }
    val sinkRaw = sinkRawOpt.get

    val t1 = System.currentTimeMillis()
    val oracleSchema = sourceReader.getTableSchema(tableName)
    val hdfsSchema = sinkRaw.schema
    val schemaDiffs = ReconChecks.checkSchema(
      tableName = tableName,
      oracleSchema = oracleSchema,
      sparkSchema = hdfsSchema
    )
    if (schemaDiffs.nonEmpty) {
      val detail = schemaDiffs.map(d => s"${d.fieldName}:${d.issue}").mkString(";")
      results += makeResult("SCHEMA", CheckStatus.MISMATCH, detail = detail, ms = System.currentTimeMillis() - t1)
    } else {
      results += makeResult("SCHEMA", CheckStatus.OK, ms = System.currentTimeMillis() - t1)
    }

    val sourceDf = DataPrep.prepare(sourceRaw).persist(StorageLevel.MEMORY_AND_DISK)
    val sinkDf = DataPrep.prepare(sinkRaw).persist(StorageLevel.MEMORY_AND_DISK)

    try {
      val t2 = System.currentTimeMillis()
      val (sourceCount, sinkCount) = ReconChecks.checkCount(sourceDf, sinkDf)
      val countStatus = if (sourceCount == sinkCount) CheckStatus.OK else CheckStatus.MISMATCH
      results += makeResult(
        "COUNT", countStatus,
        source = sourceCount.toString,
        sink = sinkCount.toString,
        detail = if (countStatus == CheckStatus.MISMATCH) s"Mismatch ${Math.abs(sourceCount - sinkCount)} rows" else "",
        ms = System.currentTimeMillis() - t2
      )
      println(s"  COUNT: src=$sourceCount sink=$sinkCount → $countStatus")

      val t3 = System.currentTimeMillis()
      val diffBuckets = ReconChecks.checkChecksum(sourceDf, sinkDf)
      val checksumStatus = if (diffBuckets.isEmpty) CheckStatus.OK else CheckStatus.MISMATCH
      results += makeResult(
        "CHECKSUM", checksumStatus,
        source = AppConfig.CHECKSUM_BUCKET_COUNT.toString,
        sink = (AppConfig.CHECKSUM_BUCKET_COUNT - diffBuckets.size).toString,
        detail = if (diffBuckets.nonEmpty) s"${diffBuckets.size} buckets lệch: ${diffBuckets.take(20).mkString(",")}" else "",
        ms = System.currentTimeMillis() - t3
      )
      println(s"  CHECKSUM: total=${AppConfig.CHECKSUM_BUCKET_COUNT} diff=${diffBuckets.size} → $checksumStatus")

      if (diffBuckets.nonEmpty) {
        val t4 = System.currentTimeMillis()
        val rowDiff = ReconChecks.checkRowDiff(sourceDf, sinkDf, diffBuckets)
        val onlySrcCount = rowDiff.onlyInSourceCount
        val onlySinkCount = rowDiff.onlyInSinkCount
        val srcSample = rowDiff.sourceSample
        val sinkSample = rowDiff.sinkSample

        val detail = Seq(
          s"only_in_source=$onlySrcCount",
          s"only_in_sink=$onlySinkCount",
          s"count_mismatch=${rowDiff.countMismatchCount}",
          s"sample_hashes_src=${srcSample.take(5).map(_.getAs[String]("row_hash")).mkString(",")}",
          s"sample_hashes_sink=${sinkSample.take(5).map(_.getAs[String]("row_hash")).mkString(",")}"
        ).mkString("|")

        results += makeResult(
          "ROW_DIFF",
          if (onlySrcCount == 0 && onlySinkCount == 0) CheckStatus.OK else CheckStatus.MISMATCH,
          source = onlySrcCount.toString,
          sink = onlySinkCount.toString,
          detail = detail,
          ms = System.currentTimeMillis() - t4
        )
        println(s"  ROW_DIFF: onlySrc=$onlySrcCount onlySink=$onlySinkCount")
      } else {
        results += makeResult("ROW_DIFF", CheckStatus.OK, detail = "No checksum differences, skipping row diff check")
      }
    } finally {
      sourceDf.unpersist()
      sinkDf.unpersist()
    }

    val totalMs = System.currentTimeMillis() - t0
    println(s"[INFO] Finished reconciling table: ${dbConfig.name}.$tableName in ${totalMs}ms")
    results
  }

}
