package com.cxi.cdp.data_processing
package curated_zone.signal_framework.transactional_insights.pre_aggr.service

import com.cxi.cdp.data_processing.curated_zone.model.signal.transactional_insights._
import com.cxi.cdp.data_processing.curated_zone.model.signal.SignalDomain

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

private[pre_aggr] object MetricsService {

    final val AllSignalDomains = Seq(TenderTypeMetric, OrderMetric, TimeOfDayMetric, ChannelMetric)

    final val AllMetricColumns = MetricsServiceHelper.getMetricColumns(AllSignalDomains: _*)

    def addOrderMetrics(orderSummary: DataFrame): DataFrame = {
        orderSummary
            .withColumn(
                MetricsServiceHelper.metricColumnName(OrderMetric.signalDomainName, OrderMetric.TotalOrders.signalName),
                lit(1L)
            )
            .withColumn(
                MetricsServiceHelper.metricColumnName(OrderMetric.signalDomainName, OrderMetric.TotalAmount.signalName),
                MetricsServiceHelper.dollarsToCentsUdf(col("ord_pay_total"))
            )
    }

    def addTimeOfDayMetrics(orderSummary: DataFrame): DataFrame = {
        orderSummary.convertSignalValuesToMetrics(
            signalColumn = MetricsServiceHelper.hourToTimeOfDayUdf(hour(col("ord_timestamp"))),
            signalDomain = TimeOfDayMetric
        )
    }

    def addChannelMetrics(orderSummary: DataFrame): DataFrame = {
        orderSummary.convertSignalValuesToMetrics(
            signalColumn =
                MetricsServiceHelper.channelMetricUdf(col("ord_originate_channel_id"), col("ord_target_channel_id")),
            signalDomain = ChannelMetric
        )
    }

    def addTenderTypeMetrics(orderSummary: DataFrame, orderTenders: DataFrame): DataFrame = {
        val orderTenderTypeWithMetrics = orderTenders
            .filter(col("tender_type").isNotNull)
            .convertSignalValuesToMetrics(
                signalColumn = MetricsServiceHelper.extractTenderTypeMetricUdf(col("tender_type")),
                signalDomain = TenderTypeMetric
            )

        val orderIdToTenderId = orderSummary.select(col("ord_id"), explode(col("tender_ids")).as("tender_id"))

        val metricColumns = MetricsServiceHelper.getMetricColumns(TenderTypeMetric)
        val metricAggregations = metricColumns.map(columnName => sum(columnName).as(columnName))

        val tenderTypeMetricsByOrderId = orderIdToTenderId
            .join(orderTenderTypeWithMetrics, Seq("tender_id"))
            .groupBy(col("ord_id"))
            .agg(metricAggregations.head, metricAggregations.drop(1): _*)

        orderSummary
            .join(tenderTypeMetricsByOrderId, Seq("ord_id"), "left_outer")
            .na
            .fill(0L, metricColumns)
    }

    implicit class DataFrameMetricsOps(df: DataFrame) {

        /** Creates a new metric column for every signal from the signal domain and sets it to 1 if `signalColumn`
          * has value that's equal to that particular supported signal, and to 0 otherwise.
          *
          * Implicit class is used for convenience.
          */
        def convertSignalValuesToMetrics(signalColumn: Column, signalDomain: SignalDomain[_]): DataFrame = {
            signalDomain.signalNames
                .foldLeft(df)({ case (df, signalName) =>
                    val columnName = MetricsServiceHelper.metricColumnName(signalDomain.signalDomainName, signalName)
                    df.withColumn(columnName, when(signalColumn === lit(signalName), lit(1L)).otherwise(lit(0L)))
                })
        }
    }

}
