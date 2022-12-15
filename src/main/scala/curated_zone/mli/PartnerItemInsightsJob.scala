package com.cxi.cdp.data_processing
package curated_zone.mli
import refined_zone.hub.model.{LocationType, OrderStateType}
import refined_zone.hub.ChangeDataFeedViews
import support.utils.mongodb.MongoDbConfigUtils
import support.utils.mongodb.MongoDbConfigUtils.MongoSparkConnectorClass
import support.utils.ContractUtils
import support.SparkSessionFactory

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import java.nio.file.Paths

object PartnerItemInsightsJob {

    private val logger = Logger.getLogger(this.getClass.getName)

    final val consumer_id = "partner_item_insights_job"

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val cliArgs = CliArgs.parse(args)
        logger.info(s"Parsed args: $cliArgs")

        run(cliArgs)(SparkSessionFactory.getSparkSession())
    }

    def run(cliArgs: CliArgs)(implicit spark: SparkSession): Unit = {
        val contract: ContractUtils = new ContractUtils(Paths.get("/mnt/" + cliArgs.contractPath))

        val dataServicesDb = contract.prop[String]("schema.data_services.db_name")
        val cdfTrackerTable = contract.prop[String]("schema.data_services.cdf_tracker_table")

        val orderSummaryTables = contract.prop[Seq[String]]("schema.order_summary_tables")

        val orderSummaryCdf = ChangeDataFeedViews.orderSummary(s"$dataServicesDb.$cdfTrackerTable", orderSummaryTables)

        val orderSummaryChangeDataResult =
            if (cliArgs.fullReprocess) {
                orderSummaryCdf.queryAllData(consumer_id)
            } else {
                orderSummaryCdf.queryChangeData(consumer_id)
            }

        orderSummaryChangeDataResult.data match {
            case None => logger.info("No updates found since the last run")
            case Some(changeData) =>
                val orderDates = getOrderDatesToProcess(changeData)
                if (orderDates.isEmpty) {
                    logger.info(s"No order dates to process")
                } else {
                    logger.info(s"Order dates to process: $orderDates")
                    process(contract, orderDates, cliArgs.fullReprocess)
                }

                logger.info(s"Update CDF tracker: ${orderSummaryChangeDataResult.tableMetadataSeq}")
                orderSummaryCdf.markProcessed(orderSummaryChangeDataResult)
        }
    }

    def process(contract: ContractUtils, orderDates: Set[String], fullReprocess: Boolean)(implicit
        spark: SparkSession
    ): Unit = {
        val refinedHubDb = contract.prop[String]("schema.refined_hub.db_name")
        val orderSummaryTable = contract.prop[String]("schema.refined_hub.order_summary_table")
        val locationTable = contract.prop[String]("schema.refined_hub.location_table")
        val itemTable = contract.prop[String]("schema.refined_hub.item_table")

        val curatedHubDb = contract.prop[String]("schema.curated_hub.db_name")
        val customer360Table = contract.prop[String]("schema.curated_hub.customer_360_table")

        val curatedcxiInsightDb = contract.prop[String]("schema.curated_cxi_insights.db_name")
        val partnerItemInsightsTable = contract.prop[String]("schema.curated_cxi_insights.partner_item_insights_table")

        val mongoDbConfig = MongoDbConfigUtils.getMongoDbConfig(spark, contract)
        val mongoDb = contract.prop[String]("mongo.db")
        val partnerItemInsightsMongoCollection = contract.prop[String]("mongo.partner_item_insights_collection")

        val orderSummaryDf = readOrderSummary(orderDates, s"$refinedHubDb.$orderSummaryTable")
        val locationDf = readLocation(s"$refinedHubDb.$locationTable")
        val itemDf = readItem(s"$refinedHubDb.$itemTable")
        val customer360Df = readCustomer360WithExplode(s"$curatedHubDb.$customer360Table")

        val orderSummaryTransformed = transformOrderSummary(orderSummaryDf, locationDf, itemDf, customer360Df)

        val partnerItemInsights = computePartnerItemInsights(orderSummaryTransformed)

        writeToDatalakePartnerItemInsights(
            partnerItemInsights,
            s"$curatedcxiInsightDb.$partnerItemInsightsTable",
            fullReprocess
        )

        writeToMongoPartnerItemInsights(
            partnerItemInsights,
            mongoDbConfig.uri,
            mongoDb,
            partnerItemInsightsMongoCollection,
            fullReprocess
        )
    }

    def getOrderDatesToProcess(orderSummaryChangeData: DataFrame): Set[String] = {
        val ordDateColumnName = "ord_date"
        val ordDateColumn = col(ordDateColumnName)

        orderSummaryChangeData
            .select(ordDateColumn)
            .filter(ordDateColumn.isNotNull)
            .distinct
            .collect
            .map(_.getAs[java.sql.Date](ordDateColumnName).toString)
            .toSet
    }

    def readOrderSummary(orderDates: Set[String], orderSummaryTable: String)(implicit
        spark: SparkSession
    ): DataFrame = {
        spark
            .table(orderSummaryTable)
            .filter(col("ord_state_id") === OrderStateType.Completed.code && col("ord_date").isInCollection(orderDates))
            .select(
                "ord_id",
                "ord_date",
                "cxi_partner_id",
                "location_id",
                "item_id",
                "item_quantity",
                "item_total",
                "cxi_identity_ids"
            )
    }

    def readLocation(locationTable: String)(implicit spark: SparkSession): DataFrame = {
        broadcast(
            spark
                .table(locationTable)
                .select("location_id", "cxi_partner_id", "location_nm", "region", "state_code", "city")
        )
    }

    def readItem(itemTable: String)(implicit spark: SparkSession): DataFrame = {
        val excludeItemNames = Set("Small", "Large", "Medium", "Spicy", "Plain", "null", "")
        val excludeItemNamesRegex =
            "^([Aa][Dd][Dd]|[Nn][Oo][Tt]?|[Ss][Ii][Dd][Ee]|[Ss][Uu][Bb]|[Ee][Xx][Tt][Rr][Aa]|[Ll][Ii][Gg][Hh][Tt]) |^[0-9]+ [Oo][Zz]$"

        spark
            .table(itemTable)
            .filter(
                (col("item_type") !== "variation") && col("item_nm").isNotNull && !col("item_nm").isInCollection(
                    excludeItemNames
                ) && !col("item_nm").rlike(excludeItemNamesRegex)
            )
            .select("item_id", "cxi_partner_id", "item_nm")
            .dropDuplicates()
    }

    def readCustomer360WithExplode(customer360Table: String)(implicit spark: SparkSession): DataFrame = {
        spark
            .table(customer360Table)
            .where(col("active_flag") === true)
    }

    def transformOrderSummary(
        orderSummaryDf: DataFrame,
        locationDf: DataFrame,
        itemDf: DataFrame,
        customer360Df: DataFrame
    ): DataFrame = {

        val customer360ExplodeDf = customer360Df
            .select(col("customer_360_id"), explode(col("identities")).as("type" :: "ids" :: Nil))
            .withColumn("id", explode(col("ids")))
            .select(col("customer_360_id"), concat(col("type"), lit(":"), col("id")).as("qualified_identity"))

        orderSummaryDf
            .join(locationDf, Seq("cxi_partner_id", "location_id"), "left")
            .join(itemDf, Seq("cxi_partner_id", "item_id"), "inner")
            .withColumn("id", explode(col("cxi_identity_ids")))
            .withColumn("qualified_identity", concat(col("id.identity_type"), lit(":"), col(s"id.cxi_identity_id")))
            .join(customer360ExplodeDf, Seq("qualified_identity"), "inner") // Inner??
            .dropDuplicates("ord_id", "ord_date", "cxi_partner_id", "location_id", "item_nm")
            .select(
                "ord_id",
                "ord_date",
                "cxi_partner_id",
                "region",
                "state_code",
                "city",
                "location_id",
                "location_nm",
                "item_nm",
                "item_quantity",
                "item_total",
                "customer_360_id"
            )
    }

    def computePartnerItemInsights(orderSummary: DataFrame): DataFrame = {
        orderSummary
            .groupBy(
                "ord_date",
                "cxi_partner_id",
                "region",
                "state_code",
                "city",
                "location_id",
                "location_nm",
                "item_nm"
            )
            .agg(
                countDistinct("ord_id") as "transaction_quantity",
                sum("item_quantity") as "item_quantity",
                sum("item_total") as "item_total",
                collect_set("customer_360_id") as "customer_360_ids"
            )
            .withColumnRenamed("item_nm", "pos_item_nm")
    }

    def writeToDatalakePartnerItemInsights(
        partnerItemInsights: DataFrame,
        destTable: String,
        fullReprocess: Boolean = false
    ): Unit = {
        if (fullReprocess) {
            partnerItemInsights.sqlContext.sql(s"DELETE FROM $destTable")
        }
        val srcTable = "newPartnerItemInsight"

        partnerItemInsights.createOrReplaceTempView(srcTable)

        partnerItemInsights.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.ord_date <=> $srcTable.ord_date
               | AND $destTable.cxi_partner_id <=> $srcTable.cxi_partner_id
               | AND $destTable.location_id <=> $srcTable.location_id
               | AND $destTable.item_nm <=> $srcTable.item_nm
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

    def writeToMongoPartnerItemInsights(
        partnerItemInsights: DataFrame,
        mongoDbUri: String,
        dbName: String,
        collectionName: String,
        fullReprocess: Boolean = false
    ): Unit = {
        val saveMode = if (fullReprocess) SaveMode.Overwrite else SaveMode.Append

        // either insert or update a document in Mongo based on these fields
        val shardKey = """{"ord_date": 1, "cxi_partner_id": 1, "location_id": 1, "item_nm": 1}"""

        partnerItemInsights.write
            .format(MongoSparkConnectorClass)
            .mode(saveMode)
            .option("database", dbName)
            .option("collection", collectionName)
            .option("uri", mongoDbUri)
            .option("replaceDocument", "true")
            .option("shardKey", shardKey)
            .save()
    }

    case class CliArgs(contractPath: String, fullReprocess: Boolean)
    object CliArgs {

        private val initOptions = CliArgs(contractPath = null, fullReprocess = false)

        private def optionsParser = new scopt.OptionParser[CliArgs]("Menu Item Insight Job") {

            opt[String]("contract-path")
                .action((contractPath, c) => c.copy(contractPath = contractPath))
                .text("path to a contract for this job")
                .required

            opt[Boolean]("full-reprocess")
                .action((fullReprocess, c) => c.copy(fullReprocess = fullReprocess))
                .text("if true, reprocess MLI fully from the beginning")
                .optional

        }

        def parse(args: Seq[String]): CliArgs = {
            optionsParser
                .parse(args, initOptions)
                .getOrElse(throw new IllegalArgumentException("Could not parse arguments"))
        }

    }

}
