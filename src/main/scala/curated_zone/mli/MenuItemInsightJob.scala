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

import java.nio.file.Paths
object MenuItemInsightJob {

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
        val curatedInsightDb = contract.prop[String]("schema.curated_cxi_insights.db_name")
        val curatedHubDb = contract.prop[String]("schema.curated_hub.db_name")

        val orderSummaryTable = contract.prop[String]("schema.refined_hub.order_summary_table")
        val locationTable = contract.prop[String]("schema.refined_hub.location_table")
        val itemTable = contract.prop[String]("schema.refined_hub.item_table")
        val customer360table = contract.prop[String]("schema.curated_hub.customer_360_table")

        val partnerItemInsightsTable = contract.prop[String]("schema.curated_cxi_insight.partner_item_insights_table")

        val mongoDbConfig = MongoDbConfigUtils.getMongoDbConfig(spark, contract)
        val mongoDbName = contract.prop[String]("mongo.db")
        val partnerItemInsightsMongoCollectionName = contract.prop[String]("mongo.partner_item_insights_collection")

        val orderSummary: DataFrame =
            readOrderSummary(
                orderDates,
                s"$refinedHubDb.$orderSummaryTable",
                s"$refinedHubDb.$locationTable",
                s"$refinedHubDb.$itemTable"
            )

        val customer360: DataFrame =
            readCustomer360(
                s"$curatedHubDb.$customer360table"
            )

        val partnerItemInsights: DataFrame = computePartnerItemInsights(orderSummary, customer360)

        writeToDatalakePartnerItemInsights(
            partnerItemInsights,
            s"$curatedInsightDb.$partnerItemInsightsTable",
            fullReprocess
        )
        writeToMongoPartnerItemInsights(
            partnerItemInsights,
            mongoDbConfig.uri,
            mongoDbName,
            partnerItemInsightsMongoCollectionName,
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

    def readOrderSummary(orderDates: Set[String], orderSummaryTable: String, locationTable: String, itemTable: String)(
        implicit spark: SparkSession
    ): DataFrame = {

        import spark.implicits._

        val orderSummaryDF = spark.table(orderSummaryTable)

        val locationDF = spark.table(locationTable)

        val itemTableDF = spark.table(itemTable).filter("a") /// ###chnage the filter, consult with Puspendra

        orderSummaryDF
            .filter($"ord_state_id" === OrderStateType.Completed.code && $"ord_date".isInCollection(orderDates))
            .join(broadcast(locationDF), usingColumns = Seq("cxi_partner_id", "location_id"), "inner")
            .join(broadcast(itemTableDF), usingColumns = Seq("cxi_partner_id", "item_id"), "inner")
            .select(
                orderSummaryDF("ord_id"),
                orderSummaryDF("cxi_partner_id"),
                orderSummaryDF("item_id"),
                locationDF("region"),
                upper(locationDF("state_code")).as("state_code"),
                initcap(locationDF("city")).as("city"),
                col("location_id"),
                col("location_nm"),
                orderSummaryDF("ord_date"),
                orderSummaryDF("item_quantity"),
                orderSummaryDF("item_total"),
                itemTableDF("item_nm").as("pos_item_nm"),
                orderSummaryDF("cxi_identity_ids").as("cxi_identity_ids"),
                explode(col("cxi_identity_ids")).as("id"),
                concat(col(s"id.identity_type"), lit(":"), col(s"id.cxi_identity_id")).as("qualified_identity")
            )
            .dropDuplicates("ord_id", "cxi_partner_id", "location_id")
            .drop("id", "cxi_identity_ids", "item_id")
        /*
        spark.sql(
            "select item_id, cxi_partner_id, item_nm from pos_item where item_type != 'variation' and " +
                "item_nm is not null and item_nm not in ('Small', 'Large', 'Medium', 'Spicy', 'Plain', 'null', '') " +
                "and item_nm not rlike '^([Aa][Dd][Dd]|[Nn][Oo][Tt]?|[Ss][Ii]" +
                "[Dd][Ee]|[Ss][Uu][Bb]|[Ee][Xx][Tt][Rr][Aa]|[Ll][Ii][Gg][Hh][Tt]) |^[0-9]+ [Oo][Zz]$'"
        )*/

    }

    def readCustomer360(
        customer360Table: String
    )(implicit spark: SparkSession): DataFrame = {

        import spark.implicits._

        val customer360DF = spark
            .table(customer360Table)
            .filter($"active_flag" === 1)
            .select(col("customer_360_id"), explode(col("identities")).as("type" :: "ids" :: Nil))
            .withColumn("id", explode(col("ids")))
            .select(col("customer_360_id"), concat(col("type"), lit(":"), col("id")).as("qualified_identity"))
        customer360DF

    }

    private def computePartnerItemInsights(orderSummary: DataFrame, customer360: DataFrame): DataFrame = {
        import orderSummary.sparkSession.implicits._

        orderSummary
            .join(customer360, Seq("qualified_identity"), "inner")
            .drop("qualified_identity")
            .groupBy(
                "ord_date",
                "cxi_partner_id",
                "location_type",
                "location_nm",
                "region",
                "state_code",
                "city",
                "pos_item_nm"
            )
            .agg(

                sum("item_quantity") as "item_quantity",
                sum("item_total") as "item_total",
                collect_set("customer_360_id") as "customer_360_ids"
            )
            .select(
                $"cxi_partner_id",
                $"location_type",
                $"region",
                $"state",
                $"city",
                $"location_id",
                $"location_nm",
                $"date",
                $"item_total",
                $"transaction_amount",
                $"transaction_quantity"
            )
    }

    def writeToDatalakePartnerItemInsights(
        partnerItemInsights: DataFrame,
        destTable: String,
        fullReprocess: Boolean = false
    ): Unit = {
        if (fullReprocess) {
            partnerItemInsights.sqlContext.sql(s"DELETE FROM $destTable")
        }
        val srcTable = "PartnerItemInsight"

        partnerItemInsights.createOrReplaceTempView(srcTable)

        partnerItemInsights.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id <=> $srcTable.cxi_partner_id
               | AND $destTable.ord_date <=> $srcTable.ord_date
               | AND $destTable.location_id <=> $srcTable.location_id
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
        val shardKey = """{"cxi_partner_id": 1, "ord_date": 1 }"""

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
