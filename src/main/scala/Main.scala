import config.{AppConfig, DatabaseConfig}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count}
import reconciler.DBReconciler
import utils.CheckpointManager
import writer.ResultWriter

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import scala.util.{Failure, Success, Try}

object Main {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("DW-Reconciliation-Job")
      .config("spark.sql.shuffle.partitions", "200")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.network.timeout", "800s")
      .config("spark.executor.heartbeatInterval", "60s")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val now     = LocalDateTime.now(ZoneId.of("Asia/Ho_Chi_Minh"))
    val runTime = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val runId   = now.format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))

    val databases: Seq[DatabaseConfig] = Seq(
      DatabaseConfig(
        name = "ERP",
        jdbcUrl = "jdbc:oracle:thin:@//10.21.22",
        user = "gg_admin",
        password = "gg_oracle",
        hdfsBasePath = "/raw_data"
      ),
      DatabaseConfig(
        name = "CMIS",
        jdbcUrl = "jdbc:oracle:thin:@//10.21.22",
        user = "gg_admin",
        password = "gg_oracle",
        hdfsBasePath = "/raw_data"
      )
    )


    val checkpointManager = new CheckpointManager(runId)
    val resultWriter = new ResultWriter(runId)

    databases.foreach { dbConfig =>
      Try {
        val dbReconciler = new DBReconciler(
          dbConfig = dbConfig,
          runId = runId,
          runTime = runTime,
          checkpointMgr = checkpointManager,
          resultWriter = resultWriter
        )

        dbReconciler.run()
      } match {
        case Success(_) => println(s"Reconciliation for database ${dbConfig.name} completed successfully.")
        case scala.util.Failure(exception) => println(s"Reconciliation for database ${dbConfig.name} failed with exception: ${exception.getMessage}")
      }
    }

    printSummary(runId, runTime)
    spark.stop()
  }

  private def printSummary(runId: String, runTime: String)(implicit spark: SparkSession): Unit = {
    Try {
      val results = spark.read.parquet(AppConfig.RESULT_HDFS_PATH)
        .filter(col("run_id") === runId)

      println(s"\n══ Result $runId ══")
      results
        .groupBy("db_name", "check_type", "status")
        .agg(count("*").alias("cnt"))
        .orderBy("db_name", "check_type", "status")
        .show(200, truncate = false)

      val mismatches = results.filter(col("status") === "MISMATCH")
      val mismatchCount = mismatches.count()
      if (mismatchCount > 0) {
        println(s"⚠️  Tổng số MISMATCH: $mismatchCount")
        mismatches.select("db_name","table_name","check_type","source_value","sink_value","detail")
          .show(50, truncate = false)
      } else {
        println("✅ Tất cả dữ liệu khớp!")
      }
    } match {
      case Failure(e) => println(s"[WARN] Không đọc được kết quả để summary: ${e.getMessage}")
      case _ =>
    }
  }
}