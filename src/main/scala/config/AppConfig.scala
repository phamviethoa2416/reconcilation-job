package config

object AppConfig {
  val CHECKPOINT_HDFS_PATH: String = "hdfs://path/to/checkpoints"
  val RESULT_HDFS_PATH: String = "hdfs://path/to/results"

  val TABLE_BATCH_SIZE    = 20
  val TABLE_BATCH_PAUSE_MS = 2000L
  val TABLE_TIMEOUT_MINUTES = 60

  val CHECKSUM_BUCKET_COUNT: Int = 1000
  val ROW_DIFF_SAMPLE_SIZE: Int = 100

  val JDBC_NUM_PARTITIONS: Int = 1
  val JDBC_FETCH_SIZE: Int = 5000
}