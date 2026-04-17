package model

import org.apache.spark.sql.Row

case class RowDiffResult(
                          onlyInSourceCount: Long,
                          onlyInSinkCount: Long,
                          countMismatchCount: Long,
                          sourceSample: Array[Row],
                          sinkSample: Array[Row]
                        )