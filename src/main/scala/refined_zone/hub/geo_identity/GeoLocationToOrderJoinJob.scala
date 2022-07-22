package com.cxi.cdp.data_processing
package refined_zone.hub.geo_identity

import refined_zone.hub.ChangeDataFeedViews
import support.change_data_feed.{ChangeDataFeedService, ChangeDataFeedSource}
import support.utils.ContractUtils
import support.SparkSessionFactory

import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.nio.file.Paths
import java.time.LocalDate

/** This Spark job joins GeoLocation data with order_summary data.
  *
  * Join happens if:
  * - geo location event and order happened at the same location
  * - geo location event and order happened within a configured interval of time (i.e. within 5 minutes)
  *
  * This job works in two modes:
  * 1. incremental processing - determine order dates and geo location dates to reprocess based on the Change Data Feed
  * 2. full reprocess - reprocess all data from the start.
  */
object GeoLocationToOrderJoinJob {

    final val CdfConsumerId = "geo_location_to_order_join_job"

    private val logger = Logger.getLogger(this.getClass.getName)

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val cliArgs = CliArgs.parse(args)
        logger.info(s"Parsed args: $cliArgs")

        val spark = SparkSessionFactory.getSparkSession()
        run(cliArgs)(spark)
    }

    def run(cliArgs: CliArgs)(implicit spark: SparkSession): Unit = {
        val contract: ContractUtils = new ContractUtils(Paths.get(cliArgs.contractPath))

        val cdfTrackerTable = getCdfTrackerTable(contract)
        val orderSummaryTables = getOrderSummaryTables(contract)
        val geoLocationTable = getGeoLocationTable(contract)
        val geoLocationToOrderTable = getGeoLocationToOrderTable(contract)

        val timestampJoinInterval = getTimestampJoinInterval(contract)

        val cdfService = new ChangeDataFeedService(cdfTrackerTable)
        val orderSummaryCdf = ChangeDataFeedViews.orderSummary(cdfTrackerTable, orderSummaryTables)
        val geoLocationCdf = new ChangeDataFeedSource.SingleTable(cdfService, geoLocationTable)

        if (cliArgs.fullReprocess) {
            fullReprocess(orderSummaryCdf, geoLocationCdf, timestampJoinInterval, geoLocationToOrderTable)
        } else {
            incrementalProcess(orderSummaryCdf, geoLocationCdf, timestampJoinInterval, geoLocationToOrderTable)
        }
    }

    private def fullReprocess(
        orderSummaryCdf: ChangeDataFeedSource,
        geoLocationCdf: ChangeDataFeedSource,
        timestampJoinInterval: String,
        geoLocationToOrderTable: String
    )(implicit spark: SparkSession): Unit = {
        val orderSummaryChangeDataResult = orderSummaryCdf.queryAllData(CdfConsumerId)
        val geoLocationChangeDataResult = geoLocationCdf.queryAllData(CdfConsumerId)

        val commitCdfOffsets = () => {
            orderSummaryCdf.markProcessed(orderSummaryChangeDataResult)
            geoLocationCdf.markProcessed(geoLocationChangeDataResult)
        }

        val processingContext = ProcessingContext(
            orderSummary = orderSummaryChangeDataResult.data.getOrElse(spark.emptyDataFrame),
            geoLocation = geoLocationChangeDataResult.data.getOrElse(spark.emptyDataFrame),
            timestampJoinInterval = timestampJoinInterval,
            geoLocationToOrderTable = geoLocationToOrderTable,
            replaceWhereCondition = None, // overwrite everything
            onSuccess = commitCdfOffsets
        )

        process(processingContext)
    }

    /** Determines order_summary order dates and geo_location geo_location_dates that have to be reprocessed
      * based on the order dates and geo location dates that have changed since the last time this job was executed.
      * 1. To get the final order dates:
      *    - get the order dates that have changed
      *    - get the geo location dates that have changed plus and minus one day each, as geo data for one
      *      particular day can be joined with orders for the previous and the following days
      *      E.g. geo_timestamp 2022-06-10 00:01:10 can be joined with ord_timestamp 2022-06-09 23:58:30,
      *      and geo_timestamp 2022-06-10 23:58:30 can be joined with ord_timestamp 2022-06-11 00:01:00.
      * 2. To get the final geo location dates, get the final order dates plus and minus one day each, as orders
      *    for one particular day can be joined with geo data for the previous and the following days.
      */
    private[geo_identity] def getFinalOrderAndGeoLocationDates(
        changedOrderDates: Set[LocalDate],
        changedGeoLocationDates: Set[LocalDate]
    ): (Set[LocalDate], Set[LocalDate]) = {
        def addPrevAndNextDays(dates: Set[LocalDate]): Set[LocalDate] = {
            dates.flatMap(date => Set(date.minusDays(1L), date, date.plusDays(1L)))
        }

        val finalOrderDates = changedOrderDates ++ addPrevAndNextDays(changedGeoLocationDates)
        val finalGeoLocationDates = addPrevAndNextDays(finalOrderDates)

        (finalOrderDates, finalGeoLocationDates)
    }

    private def incrementalProcess(
        orderSummaryCdf: ChangeDataFeedSource,
        geoLocationCdf: ChangeDataFeedSource,
        timestampJoinInterval: String,
        geoLocationToOrderTable: String
    )(implicit spark: SparkSession): Unit = {
        val orderSummaryChangeDataResult = orderSummaryCdf.queryChangeData(CdfConsumerId)
        val changedOrderDates = orderSummaryChangeDataResult.data
            .map(orderSummary => getOrderDatesToProcess(orderSummary))
            .getOrElse(Set.empty)

        val geoLocationChangeDataResult = geoLocationCdf.queryChangeData(CdfConsumerId)
        val changedGeoLocationDates =
            geoLocationChangeDataResult.data.map(getGeoLocationDatesToProcess).getOrElse(Set.empty)

        val (finalOrderDates, finalGeoLocationDates) =
            getFinalOrderAndGeoLocationDates(changedOrderDates, changedGeoLocationDates)

        if (finalOrderDates.isEmpty) {
            logger.info("No updates found since the last run")
        } else {
            val orderSummary = orderSummaryCdf
                .queryAllData(CdfConsumerId)
                .data
                .getOrElse(spark.emptyDataFrame)
                .filter(col("ord_date").isInCollection(finalOrderDates))

            val geoLocation = geoLocationCdf
                .queryAllData(CdfConsumerId)
                .data
                .getOrElse(spark.emptyDataFrame)
                .filter(col("geo_date").isInCollection(finalGeoLocationDates))

            val commitCdfOffsets = () => {
                orderSummaryCdf.markProcessed(orderSummaryChangeDataResult)
                geoLocationCdf.markProcessed(geoLocationChangeDataResult)
            }

            val processingContext = ProcessingContext(
                orderSummary = orderSummary,
                geoLocation = geoLocation,
                timestampJoinInterval = timestampJoinInterval,
                geoLocationToOrderTable = geoLocationToOrderTable,
                replaceWhereCondition = Some(getReplaceWhereConditionForOrderDates(finalOrderDates)),
                onSuccess = commitCdfOffsets
            )

            process(processingContext)
        }
    }

    private case class ProcessingContext(
        orderSummary: DataFrame,
        geoLocation: DataFrame,
        timestampJoinInterval: String,
        geoLocationToOrderTable: String,
        replaceWhereCondition: Option[String],
        onSuccess: () => Unit
    )

    private def process(processingContext: ProcessingContext)(implicit spark: SparkSession): Unit = {
        val geoLocationToOrder = joinGeoLocationWithOrder(
            processingContext.orderSummary,
            processingContext.geoLocation,
            processingContext.timestampJoinInterval
        )

        val geoLocationToOrderWithDeviceScore = addDeviceScore(geoLocationToOrder)

        writeGeoLocationToOrder(
            geoLocationToOrderWithDeviceScore,
            processingContext.geoLocationToOrderTable,
            processingContext.replaceWhereCondition
        )

        logger.info("Committing CDF offsets")
        processingContext.onSuccess()
    }

    private def getOrderDatesToProcess(orderSummaryChangeData: DataFrame): Set[LocalDate] = {
        extractDistinctDates(orderSummaryChangeData, col("ord_date"))
    }

    private def getGeoLocationDatesToProcess(geoLocationChangeData: DataFrame): Set[LocalDate] = {
        extractDistinctDates(geoLocationChangeData, col("geo_date"))
    }

    private def extractDistinctDates(df: DataFrame, dateColumn: Column): Set[LocalDate] = {
        df
            .select(dateColumn)
            .filter(dateColumn.isNotNull)
            .distinct
            .collect
            .map(_.getAs[java.sql.Date](0).toLocalDate)
            .toSet
    }

    private[geo_identity] def getReplaceWhereConditionForOrderDates(orderDates: Iterable[LocalDate]): String = {
        "`ord_date` IN " + orderDates.map(orderDate => s"'$orderDate'").mkString("(", ", ", ")")
    }

    private[geo_identity] def addDeviceScore(
        ordersWithGeoLocation: DataFrame
    )(implicit spark: SparkSession): DataFrame = {
        import spark.implicits._

        val windowOrder = Window.partitionBy("ord_id", "cxi_partner_id")
        ordersWithGeoLocation
            .withColumn("total_score", lit(1.0))
            .withColumn("device_count", count("*") over windowOrder)
            .withColumn("device_score", $"total_score" / $"device_count")
            .drop("total_score", "device_count")
    }

    private[geo_identity] def joinGeoLocationWithOrder(
        orderSummary: DataFrame,
        geoLocation: DataFrame,
        timestampJoinInterval: String
    )(implicit spark: SparkSession): DataFrame = {
        import spark.implicits._

        orderSummary
            .withColumn("min_geo_timestamp", $"ord_timestamp" - expr(timestampJoinInterval))
            .withColumn("max_geo_timestamp", $"ord_timestamp" + expr(timestampJoinInterval))
            .withColumn("min_geo_date", to_date($"min_geo_timestamp"))
            .withColumn("max_geo_date", to_date($"max_geo_timestamp"))
            .as("ord")
            .join(
                geoLocation.as("geo"),
                $"geo.location_id" === $"ord.location_id" &&
                    $"geo.cxi_partner_id" === $"ord.cxi_partner_id" &&
                    $"geo.geo_timestamp".between($"min_geo_timestamp", $"max_geo_timestamp") &&
                    $"geo.geo_date".between($"min_geo_date", $"max_geo_date"),
                "inner"
            )
            .select(
                $"ord.cxi_partner_id".as("cxi_partner_id"),
                $"ord.location_id".as("location_id"),
                $"ord_id",
                $"ord_date",
                $"cxi_identity_ids",
                $"maid",
                $"maid_type"
            )
            .dropDuplicates("cxi_partner_id", "location_id", "ord_id", "maid_type", "maid")
    }

    private def getCdfTrackerTable(contract: ContractUtils): String = {
        val db = contract.prop[String]("schema.data_services.db_name")
        val table = contract.prop[String]("schema.data_services.cdf_tracker_table")
        s"$db.$table"
    }

    private def getOrderSummaryTables(contract: ContractUtils): Seq[String] = {
        contract.prop[Seq[String]]("schema.order_summary_tables")
    }

    private def getGeoLocationTable(contract: ContractUtils): String = {
        val db = contract.prop[String]("schema.refined_hub.db_name")
        val table = contract.prop[String]("schema.refined_hub.geo_location_table")
        s"$db.$table"
    }

    private def getGeoLocationToOrderTable(contract: ContractUtils): String = {
        val db = contract.prop[String]("schema.refined_hub.db_name")
        val table = contract.prop[String]("schema.refined_hub.geo_location_to_order_table")
        s"$db.$table"
    }

    private def getTimestampJoinInterval(contract: ContractUtils): String = {
        contract.prop[String]("jobs.databricks.geo_location_to_order_join_job.job_config.time_interval")
    }

    private[geo_identity] def writeGeoLocationToOrder(
        geoLocationToOrder: DataFrame,
        targetTable: String,
        replaceWhereCondition: Option[String]
    )(implicit spark: SparkSession): Unit = {
        val writeOptions: Map[String, String] = replaceWhereCondition
            .map(replaceWhere => Map("replaceWhere" -> replaceWhere))
            .getOrElse(Map.empty)

        geoLocationToOrder.write
            .format("delta")
            .mode(SaveMode.Overwrite)
            .options(writeOptions)
            .saveAsTable(targetTable)
    }

    case class CliArgs(contractPath: String, fullReprocess: Boolean = false)

    object CliArgs {

        private val initOptions = CliArgs(contractPath = null)

        private def optionsParser = new scopt.OptionParser[CliArgs]("GeoLocation To Order Join Job") {

            opt[String]("contract-path")
                .action((contractPath, c) => c.copy(contractPath = contractPath))
                .text("path to a contract for this job")
                .required

            opt[Boolean]("full-reprocess")
                .action((fullReprocess, c) => c.copy(fullReprocess = fullReprocess))
                .text("if true, remove current indentity relationships and reprocess them from the beginning")

        }

        def parse(args: Seq[String]): CliArgs = {
            optionsParser
                .parse(args, initOptions)
                .getOrElse(throw new IllegalArgumentException("Could not parse arguments"))
        }

    }

}
