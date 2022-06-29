package com.cxi.cdp.data_processing
package curated_zone.signal_framework.transactional_insights.pre_aggr.service

import com.cxi.cdp.data_processing.curated_zone.model.signal.SignalDomain
import com.cxi.cdp.data_processing.curated_zone.signal_framework.transactional_insights.pre_aggr.model._
import com.cxi.cdp.data_processing.refined_zone.hub.model.CxiIdentity.{CxiIdentityId, CxiIdentityIds}

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, concat, explode, lit}

private[pre_aggr] object PreAggrTransactionalInsightsService {

    import curated_zone.signal_framework.transactional_insights.pre_aggr.service.MetricsService._

    /** Calculates pre-aggregated metrics for order date / customer360 ID / cxi partner ID / location ID
      * from order summary with pre-aggregated transactional insights metrics and customer360 ID mapping.
      *
      * `GROUP BY GROUPING SETS` is used to calculate total metrics per location ID and for all locations
      * in one operation. Unfortunately this API is only supported via Spark SQL.
      */
    def getCustomer360IdWithMetrics(
        orderSummaryWithMetrics: DataFrame,
        customer360IdToQualifiedIdentity: DataFrame,
        signalDomains: Seq[SignalDomain[_]] = AllSignalDomains
    ): DataFrame = {
        val aggregationsSQL = MetricsServiceHelper
            .getMetricColumns(signalDomains: _*)
            .map(column => s"SUM($column) AS $column")
            .mkString(", ")

        val orderSummaryWithMetricsAndCustomer360Id = orderSummaryWithMetrics
            .select(col("*"), explode(col(CxiIdentityIds)).as("id"))
            .withColumn("qualified_identity", concat(col("id.identity_type"), lit(":"), col(s"id.$CxiIdentityId")))
            .join(customer360IdToQualifiedIdentity, Seq("qualified_identity"), "inner")
            .dropDuplicates("ord_id", "cxi_partner_id", "location_id")

        val tempViewName = "orderSummaryWithMetricsAndCustomer360Id"
        orderSummaryWithMetricsAndCustomer360Id.createOrReplaceTempView(tempViewName)

        orderSummaryWithMetricsAndCustomer360Id.sqlContext.sql(s"""
            SELECT
                customer_360_id,
                cxi_partner_id,
                ord_date,
                COALESCE(location_id, '${PreAggrTransactionalInsightsRecord.AllLocationsAlias}') AS location_id,
                $aggregationsSQL
            FROM $tempViewName
            GROUP BY GROUPING SETS (
                (customer_360_id, cxi_partner_id, ord_date, location_id),
                (customer_360_id, cxi_partner_id, ord_date)
            )
            """)
    }

    def getOrderSummaryWithMetrics(orderSummary: DataFrame, orderTenders: DataFrame): DataFrame = {
        val transformations: Seq[DataFrame => DataFrame] = Seq(
            addTenderTypeMetrics(_, orderTenders),
            addOrderMetrics _,
            addTimeOfDayMetrics _,
            addChannelMetrics _
        )

        transformations.foldLeft(orderSummary)({ case (df, f) => f(df) })
    }

    def getCustomer360IdToQualifiedIdentity(posCustomer360: DataFrame): DataFrame = {
        posCustomer360
            .select(col("customer_360_id"), explode(col("identities")).as("type" :: "ids" :: Nil))
            .withColumn("id", explode(col("ids")))
            .select(col("customer_360_id"), concat(col("type"), lit(":"), col("id")).as("qualified_identity"))
    }

    def transformToFinalRecord(
        customer360IdWithMetrics: DataFrame,
        signalDomains: Seq[SignalDomain[_]] = AllSignalDomains
    )(implicit spark: SparkSession): Dataset[PreAggrTransactionalInsightsRecord] = {
        import spark.implicits._

        customer360IdWithMetrics.flatMap(row => {
            for {
                signalDomain <- signalDomains
                signalName <- signalDomain.signalNames
                metricColumn = MetricsServiceHelper.metricColumnName(signalDomain.signalDomainName, signalName)
                metricValue = row.getAs[Long](metricColumn)
                nonZeroMetricValue <- if (metricValue == 0) None else Some(metricValue)
            } yield PreAggrTransactionalInsightsRecord(
                ord_date = row.getAs[java.sql.Date]("ord_date"),
                customer_360_id = row.getAs[String]("customer_360_id"),
                cxi_partner_id = row.getAs[String]("cxi_partner_id"),
                location_id = row.getAs[String]("location_id"),
                signal_domain = signalDomain.signalDomainName,
                signal_name = signalName,
                signal_value = nonZeroMetricValue
            )
        })
    }

}
