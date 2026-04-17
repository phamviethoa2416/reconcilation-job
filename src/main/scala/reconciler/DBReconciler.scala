package reconciler

import config.{AppConfig, DatabaseConfig}
import model.{CheckStatus, TableReconResult}
import org.apache.spark.sql.SparkSession
import reader.{HdfsMetadataReader, OracleMetadataReader}
import utils.CheckpointManager
import writer.ResultWriter

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

class DBReconciler(
                    dbConfig: DatabaseConfig,
                    runId: String,
                    runTime: String,
                    checkpointMgr: CheckpointManager,
                    resultWriter: ResultWriter
                  )(implicit spark: SparkSession) {
  private val sourceReader = new OracleMetadataReader(dbConfig)
  private val sinkReader = new HdfsMetadataReader(dbConfig)
  private val tableReconciler = new TableReconciler(
    dbConfig, sourceReader, sinkReader, runId, runTime
  )

  def run(): Unit = {
    val sourceTables = sourceReader.listTables().map(_.toUpperCase()).toSet
    val sinkTables = sinkReader.listTables().map(_.toUpperCase()).toSet

    val missingInSink = sourceTables.diff(sinkTables)
    val extraInSink = sinkTables.diff(sourceTables)
    val commonTables = sourceTables.intersect(sinkTables).toSeq.sorted

    println(s"Source: ${sourceTables.size} tables, Sink: ${sinkTables.size} tables")
    println(s"Common tables: ${commonTables.size}")
    println(s"Missing in Sink: ${missingInSink.size} tables")
    println(s"Extra in Sink: ${extraInSink.size} tables")

    val catalogResults = Seq(
      TableReconResult(runId, runTime, dbConfig.name, "_CATALOG_",
        "TABLE_COUNT", if (missingInSink.isEmpty && extraInSink.isEmpty) CheckStatus.OK.toString else CheckStatus.MISMATCH.toString,
        sourceTables.size.toString, sinkTables.size.toString,
        s"missing_in_sink=${missingInSink.take(50).mkString(",")}|extra_in_sink=${extraInSink.take(50).mkString(",")}",
        0L)
    )
    resultWriter.write(catalogResults)

    commonTables
      .filterNot(t => checkpointMgr.isDone(dbConfig.name, t))
      .grouped(AppConfig.TABLE_BATCH_SIZE)
      .zipWithIndex
      .foreach { case (batch, batchIdx) =>
        println(s"\n  [BATCH $batchIdx] ${batch.size} bảng: ${batch.take(5).mkString(",")}...")

        val batchResults = batch.flatMap { tableName =>
          val result = Try {
            val f = Future { tableReconciler.reconcile(tableName) }
            Await.result(f, AppConfig.TABLE_TIMEOUT_MINUTES.minutes)
          } match {
            case Success(res) =>
              checkpointMgr.markDone(dbConfig.name, tableName)
              res
            case Failure(e) =>
              println(s"  [TIMEOUT/ERROR] $tableName: ${e.getMessage}")
              Seq(TableReconResult(
                runId, runTime, dbConfig.name, tableName,
                "TIMEOUT", CheckStatus.ERROR.toString, "", "",
                e.getMessage.take(500), 0L
              ))
          }
          result
        }

        resultWriter.write(batchResults)

        if (batchIdx > 0 && batchIdx % 5 == 0) {
          println(s" Pausing for ${AppConfig.TABLE_BATCH_PAUSE_MS} ms to avoid overload...")
          Thread.sleep(AppConfig.TABLE_BATCH_PAUSE_MS)
        }
      }

    println(s"[DB] Complete: ${dbConfig.name}")
  }
}
