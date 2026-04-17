package model

case class TableReconResult(
                             runId: String,
                             runTime: String,
                             dbName: String,
                             tableName: String,
                             checkType: String,
                             status: String,
                             sourceValue: String,
                             sinkValue: String,
                             detail: String,
                             durationMs: Long
                           )