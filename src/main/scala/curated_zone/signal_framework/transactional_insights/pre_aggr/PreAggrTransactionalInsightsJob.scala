package com.cxi.cdp.data_processing
package curated_zone.signal_framework.transactional_insights.pre_aggr

import com.cxi.cdp.data_processing.curated_zone.signal_framework.transactional_insights.pre_aggr.model._
import com.cxi.cdp.data_processing.curated_zone.signal_framework.transactional_insights.pre_aggr.service._
import com.cxi.cdp.data_processing.refined_zone.hub.ChangeDataFeedViews
import com.cxi.cdp.data_processing.support.change_data_feed.ChangeDataFeedSource
import com.cxi.cdp.data_processing.support.utils.ContractUtils
import com.cxi.cdp.data_processing.support.SparkSessionFactory

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

import java.nio.file.Paths

/** This job pre-aggregates Transactional Insight metrics for each order date, customer360, partner, and location.
  *
  * It works in two modes:
  * 1. Incremental (default)
  *    - get orders changed since the last run
  *    - extract order dates of the above orders
  *    - recalculate metrics for these order dates
  * 2. Full reprocess
  *    - recalculate metrics for all orders
  */
object PreAggrTransactionalInsightsJob {

    private val logger = Logger.getLogger(this.getClass.getName)

    final val CdfConsumerId = "pre_aggregate_transaction_insights_job"

    private final val ordDateColumnName = "ord_date"
    private final val ordDateColumn = col(ordDateColumnName)

    type FinalizeOnSuccessFn = Function0[Unit]

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val cliArgs = CliArgs.parse(args)
        logger.info(s"Parsed args: $cliArgs")

        run(cliArgs)(SparkSessionFactory.getSparkSession())
    }

    def run(cliArgs: CliArgs)(implicit spark: SparkSession): Unit = {
        val contract: ContractUtils = new ContractUtils(Paths.get("/mnt/" + cliArgs.contractPath))

        getProcessingContext(contract, cliArgs.fullReprocess) match {
            case None => logger.info("Nothing to process")
            case Some(processingContext) => process(processingContext)
        }
    }

    private case class ProcessingContext(
        orderSummary: DataFrame,
        orderTenders: DataFrame,
        customer360: DataFrame,
        destTable: String,
        maybeOrderDates: Option[Set[String]],
        finalizeOnSuccessFn: FinalizeOnSuccessFn
    )

    /** Returns processing context based on the job mode.
      * 1. for full reprocess mode, orderSummary will have all orders, and maybeOrderDates will be empty
      * 2. for incremental mode
      *    a. if there were order summary changes since the last run, maybeOrderDates will have order dates
      *    for the orders that were changed, and orderSummary will have all orders for these dates
      *    b. if here were no changes, processing context is not returned
      */
    private def getProcessingContext(contract: ContractUtils, fullReprocess: Boolean)(implicit
        spark: SparkSession
    ): Option[ProcessingContext] = {
        val orderSummaryCdf = getOrderSummaryCdf(contract)
        val orderTenders = spark.table(getOrderTenderTable(contract))
        val customer360 = spark.table(getCustomer360Table(contract))
        val destTable = getDestTable(contract)

        if (fullReprocess) {
            val cdfResult = orderSummaryCdf.queryAllData(CdfConsumerId)
            cdfResult.data.map(orderSummary => {
                ProcessingContext(
                    orderSummary = getOrderSummaryWithNonNullKeyFields(orderSummary),
                    orderTenders = orderTenders,
                    customer360 = customer360,
                    destTable = destTable,
                    maybeOrderDates = None,
                    finalizeOnSuccessFn = () => orderSummaryCdf.markProcessed(cdfResult)
                )
            })
        } else {
            val cdfResult = orderSummaryCdf.queryChangeData(CdfConsumerId)
            for {
                orderSummaryChangeData <- cdfResult.data
                orderDates = getOrderDatesToProcess(orderSummaryChangeData)
                _ = logger.info(s"Order dates to reprocess: $orderDates")
                if orderDates.nonEmpty
                orderSummaryFull <- orderSummaryCdf.queryAllData(CdfConsumerId).data
            } yield {
                val orderSummaryForDates = orderSummaryFull.filter(ordDateColumn.isInCollection(orderDates))
                ProcessingContext(
                    orderSummary = getOrderSummaryWithNonNullKeyFields(orderSummaryForDates),
                    orderTenders = orderTenders,
                    customer360 = customer360,
                    destTable = destTable,
                    maybeOrderDates = Some(orderDates),
                    finalizeOnSuccessFn = () => orderSummaryCdf.markProcessed(cdfResult)
                )
            }
        }
    }

    private def getOrderSummaryWithNonNullKeyFields(orderSummary: DataFrame): DataFrame = {
        val nonNullColumns = Seq(
            ordDateColumnName,
            "cxi_partner_id",
            "location_id",
            "cxi_identity_ids"
        )
        orderSummary.na.drop(nonNullColumns)
    }

    private def getOrderDatesToProcess(orderSummaryChangeData: DataFrame): Set[String] = {
        orderSummaryChangeData
            .select(ordDateColumn)
            .filter(ordDateColumn.isNotNull)
            .distinct
            .collect
            .map(_.getAs[java.sql.Date](ordDateColumnName).toString)
            .toSet
    }

    private def process(processingContext: ProcessingContext)(implicit spark: SparkSession): Unit = {
        import PreAggrTransactionalInsightsService._

        val orderSummaryWithMetrics =
            getOrderSummaryWithMetrics(processingContext.orderSummary, processingContext.orderTenders)
        val customer360IdToQualifiedIdentity = getCustomer360IdToQualifiedIdentity(processingContext.customer360)
        val customer360IdWithMetrics =
            getCustomer360IdWithMetrics(orderSummaryWithMetrics, customer360IdToQualifiedIdentity)

        val preAggrRecords = transformToFinalRecord(customer360IdWithMetrics)
        write(preAggrRecords, processingContext.destTable, processingContext.maybeOrderDates)

        logger.info("Processing completed. Finalizing...")
        processingContext.finalizeOnSuccessFn()
    }

    private def getOrderSummaryCdf(contract: ContractUtils): ChangeDataFeedSource = {
        val orderSummaryTables = contract.prop[Seq[String]]("datalake.order_summary_tables")
        ChangeDataFeedViews.orderSummary(getCdfTrackerTable(contract), orderSummaryTables)
    }

    private def getCdfTrackerTable(contract: ContractUtils): String = {
        val db = contract.prop[String]("datalake.data_services.db_name")
        val table = contract.prop[String]("datalake.data_services.cdf_tracker_table")
        s"$db.$table"
    }

    private def getOrderTenderTable(contract: ContractUtils): String = {
        val db = contract.prop[String]("datalake.refined_hub.db_name")
        val table = contract.prop[String]("datalake.refined_hub.order_tender_table")
        s"$db.$table"
    }

    private def getCustomer360Table(contract: ContractUtils): String = {
        val db = contract.prop[String]("datalake.curated_hub.db_name")
        val table = contract.prop[String]("datalake.curated_hub.customer_360_table")
        s"$db.$table"
    }

    private def getDestTable(contract: ContractUtils): String = {
        val db = contract.prop[String]("datalake.curated_hub.db_name")
        val table = contract.prop[String]("datalake.curated_hub.pre_aggr_transactional_insights_table")
        s"$db.$table"
    }

    private def write(
        ds: Dataset[PreAggrTransactionalInsightsRecord],
        destTable: String,
        maybeOrderDates: Option[Set[String]]
    ): Unit = {
        val writeOptions = maybeOrderDates match {
            case None => Map.empty[String, String]
            case Some(orderDates) =>
                val orderDatesSqlArray = orderDates.mkString("('", "', '", "')")
                Map("replaceWhere" -> s"ord_date in $orderDatesSqlArray")
        }

        ds.write
            .format("delta")
            .partitionBy("ord_date")
            .mode(SaveMode.Overwrite)
            .options(writeOptions)
            .saveAsTable(destTable)
    }

}
