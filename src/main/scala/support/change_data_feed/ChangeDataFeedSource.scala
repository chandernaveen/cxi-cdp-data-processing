package com.cxi.cdp.data_processing
package support.change_data_feed

import org.apache.spark.sql.SparkSession

trait ChangeDataFeedSource {

    protected def cdfService: ChangeDataFeedService

    def queryChangeData(consumerId: String)(implicit spark: SparkSession): ChangeDataQueryResult

    def markProcessed(result: ChangeDataQueryResult)(implicit spark: SparkSession): Unit = {
        val cdfTrackerUpdates = result.tableMetadataSeq
            .map(m => CdfTrackerRow(result.consumerId, m.table, m.latestAvailableVersion))
        cdfService.setLatestProcessedVersion(cdfTrackerUpdates: _*)
    }

}

object ChangeDataFeedSource {

    class SingleTable(protected val cdfService: ChangeDataFeedService, table: String) extends ChangeDataFeedSource {
        def queryChangeData(consumerId: String)(implicit spark: SparkSession): ChangeDataQueryResult = {
            cdfService.queryChangeData(consumerId, table)
        }
    }

    class SimpleUnion(protected val cdfService: ChangeDataFeedService, tables: Seq[String]) extends ChangeDataFeedSource {
        override def queryChangeData(consumerId: String)(implicit spark: SparkSession): ChangeDataQueryResult = {
            val queryResults = tables.map(table => cdfService.queryChangeData(consumerId, table))
            queryResults.reduce((acc, result) => {
                    val combinedTableMetadataSeq = acc.tableMetadataSeq ++ result.tableMetadataSeq
                    val combinedChangeData = (acc.changeData, result.changeData) match {
                        case (Some(a), Some(b)) => Some(a.union(b))
                        case _ => acc.changeData.orElse(result.changeData)
                    }

                    acc.copy(tableMetadataSeq = combinedTableMetadataSeq, changeData = combinedChangeData)
            })
        }
    }

}
