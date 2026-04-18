package model

case class RowDiffResult(
                          onlyInSourceCount: Long,
                          onlyInSinkCount: Long,
                          countMismatchCount: Long,
                          sourceSample: Seq[String],
                          sinkSample: Seq[String]
                        )