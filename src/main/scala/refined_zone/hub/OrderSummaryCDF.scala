package com.cxi.cdp.data_processing
package refined_zone.hub

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Provides the unified change data feed for order summary tables from different POSs. */
class OrderSummaryCDF(
                         val cdfTrackerTable: String,
                         val orderSummarySquareTable: String
                     ) {

    private val cdfService = new ChangeDataFeedService(cdfTrackerTable)

    def queryChangeData(consumerId: String)(implicit spark: SparkSession): Option[OrderSummaryCDF.Result] = {
        // Now we have only square, but in the future we should consolidate change data from several POSs

        val squareChangeData = cdfService.queryChangeData(consumerId, orderSummarySquareTable)

        squareChangeData.changeData.map({ changeDataDF =>
            val squareCdfTrackerUpdate = ChangeDataFeedService.CDFTrackerRow(
                consumerId = consumerId,
                table = orderSummarySquareTable,
                latestProcessedVersion = squareChangeData.latestAvailableVersion)

            OrderSummaryCDF.Result(changeDataDF, Seq(squareCdfTrackerUpdate))
        })
    }

    def markProcessed(result: OrderSummaryCDF.Result)(implicit spark: SparkSession): Unit = {
        cdfService.setLatestProcessedVersion(result.cdfTrackerUpdates: _*)
    }
}

object OrderSummaryCDF {

    case class Result(
                         changeData: DataFrame,
                         cdfTrackerUpdates: Seq[ChangeDataFeedService.CDFTrackerRow]
                     )

}
