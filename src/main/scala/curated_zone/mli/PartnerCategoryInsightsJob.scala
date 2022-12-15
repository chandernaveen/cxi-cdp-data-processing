package com.cxi.cdp.data_processing
package curated_zone.mli

import refined_zone.hub.ChangeDataFeedViews
import support.utils.mongodb.MongoDbConfigUtils
import support.utils.mongodb.MongoDbConfigUtils.MongoSparkConnectorClass
import support.utils.ContractUtils
import support.SparkSessionFactory

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.nio.file.Paths

object PartnerCategoryInsightsJob {

    private val logger = Logger.getLogger(this.getClass.getName)
    final val CdfConsumerId = "partner_category_insights_job"

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

        val itemInsightsTables = contract.prop[String]("schema.curated_cxi_insights.partner_item_insights_table")

        val itemInsightsCDF =
            ChangeDataFeedViews.cdfSingleTable(s"$dataServicesDb.$cdfTrackerTable", itemInsightsTables)

        val itemInsightsCdfResult =
            if (cliArgs.fullReprocess) {
                itemInsightsCDF.queryAllData(CdfConsumerId)
            } else {
                itemInsightsCDF.queryChangeData(CdfConsumerId)
            }

        itemInsightsCdfResult.data match {
            case None => logger.info("No updates found since the last run")

            case Some(changeData) =>
                val itemOrderDates = getItemOrderDatesToProcess(changeData)
                if (itemOrderDates.isEmpty) {
                    logger.info(s"No Item-order dates to process")
                } else {
                    logger.info(s"Item Order dates to process: $itemOrderDates")
                    process(contract, itemOrderDates, cliArgs.fullReprocess)
                }

                logger.info(s"Update CDF tracker: ${itemInsightsCdfResult.tableMetadataSeq}")
                itemInsightsCDF.markProcessed(itemInsightsCdfResult)
        }
    }

    def process(contract: ContractUtils, orderDates: Set[String], fullReprocess: Boolean)(implicit
        spark: SparkSession
    ): Unit = {

        val curatedInsightsDb = contract.prop[String]("schema.curated_cxi_insights.db_name")
        val curatedDb = contract.prop[String]("schema.curated_hub.db_name")
        val itemInsightsTable = contract.prop[String]("schema.curated_cxi_insights.partner_item_insights_table")
        val itemUniverseTable = contract.prop[String]("schema.curated_hub.item_universe_table")
        val categoryInsightsTable = contract.prop[String]("schema.curated_cxi_insights.partner_category_insights_table")

        val mongoDbConfig = MongoDbConfigUtils.getMongoDbConfig(spark, contract)
        val mongoDbName = contract.prop[String]("mongo.db")
        val categoryInsightsMongoCollectionName = contract.prop[String]("mongo.partner_category_insights_collection")

        val itemInsightsData: DataFrame =
            readItemInsightsUniv(orderDates, s"$curatedInsightsDb.$itemInsightsTable", s"$curatedDb.$itemUniverseTable")

        val categoryInsightsData: DataFrame =
            computeCategoryInsights(itemInsightsData)

        writeToDataLakeCategoryInsights(
            categoryInsightsData,
            s"$curatedInsightsDb.$categoryInsightsTable",
            fullReprocess
        )
        writeToMongoCategoryInsights(
            categoryInsightsData,
            mongoDbConfig.uri,
            mongoDbName,
            categoryInsightsMongoCollectionName,
            fullReprocess
        )

    }
    def getItemOrderDatesToProcess(itemInsightsChangeData: DataFrame): Set[String] = {
        val ordDateColumnName = "ord_date"
        val ordDateColumn = col(ordDateColumnName)

        itemInsightsChangeData
            .select(ordDateColumn)
            .filter(ordDateColumn.isNotNull)
            .distinct
            .collect
            .map(_.getAs[java.sql.Date](ordDateColumnName).toString)
            .toSet
    }

    def readItemInsightsUniv(orderDates: Set[String], itemInsightsTable: String, itemUniverseTable: String)(implicit
        spark: SparkSession
    ): DataFrame = {
        import spark.implicits._

        val itemInsightsTableDf = spark.table(itemInsightsTable)
        val itemUniverseTableDf = broadcast(spark.table(itemUniverseTable))

        itemInsightsTableDf
            .filter($"ord_date".isInCollection(orderDates))
            .as("ingt")
            .join(
                itemUniverseTableDf.as("univ"),
                $"ingt.cxi_partner_id" === $"univ.cxi_partner_id" && $"ingt.pos_item_nm" === $"univ.pos_item_nm",
                "inner"
            )
            .select(
                $"ingt.ord_date".as("ord_date"),
                $"ingt.cxi_partner_id".as("cxi_partner_id"),
                $"ingt.region".as("region"),
                $"ingt.state_code".as("state_code"),
                $"ingt.city".as("city"),
                $"ingt.location_id".as("location_id"),
                $"ingt.location_nm".as("location_nm"),
                $"ingt.transaction_quantity".as("transaction_quantity"),
                $"ingt.item_quantity".as("item_quantity"),
                $"ingt.item_total".as("item_total"),
                coalesce($"univ.item_category_modified", $"univ.cxi_item_category").as("item_category"),
                coalesce($"univ.item_nm_modified", $"univ.pos_item_nm").as("item_nm")
            )
    }

    def computeCategoryInsights(itemInsightsData: DataFrame): DataFrame = {
        import itemInsightsData.sparkSession.implicits._

        itemInsightsData
            .groupBy(
                "cxi_partner_id",
                "region",
                "state_code",
                "city",
                "location_id",
                "ord_date",
                "location_nm",
                "item_category"
            )
            .agg(
                sum("transaction_quantity") as "transaction_quantity",
                sum("item_quantity") as "item_quantity",
                sum("item_total") as "item_total"
            )
            .select(
                $"ord_date",
                $"cxi_partner_id",
                $"region",
                $"state_code",
                $"city",
                $"location_id",
                $"location_nm",
                $"transaction_quantity",
                $"item_quantity",
                $"item_category",
                $"item_total"
            )
    }

    private def writeToDataLakeCategoryInsights(
        categoryInsightsData: DataFrame,
        destTable: String,
        fullReprocess: Boolean = false
    ): Unit = {
        if (fullReprocess) {
            categoryInsightsData.sqlContext.sql(s"DELETE FROM $destTable")
        }
        val srcTable = "newCategoryInsights"

        categoryInsightsData.createOrReplaceTempView(srcTable)

        // <=> comparison operator is used for null-safe semantics
        // https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-null-semantics.html#null-semantics
        categoryInsightsData.sqlContext.sql(
            s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.ord_date <=> $srcTable.ord_date
               | AND $destTable.cxi_partner_id <=> $srcTable.cxi_partner_id
               | AND $destTable.location_id <=> $srcTable.location_id
               | AND $destTable.item_category <=> $srcTable.item_category
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin
        )
    }

    private def writeToMongoCategoryInsights(
        categoryInsightsData: DataFrame,
        mongoDbUri: String,
        dbName: String,
        collectionName: String,
        fullReprocess: Boolean = false
    ): Unit = {
        val saveMode = if (fullReprocess) SaveMode.Overwrite else SaveMode.Append

        // either insert or update a document in Mongo based on these fields
        val shardKey = """{"cxi_partner_id": 1, "ord_date": 1 }"""
        categoryInsightsData.write // Change
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

        private def optionsParser = new scopt.OptionParser[CliArgs]("Total Market Insight Job") {

            opt[String]("contract-path")
                .action((contractPath, c) => c.copy(contractPath = contractPath))
                .text("path to a contract for this job")
                .required

            opt[Boolean]("full-reprocess")
                .action((fullReprocess, c) => c.copy(fullReprocess = fullReprocess))
                .text("if true, reprocess TMI fully from the beginning")
                .optional

        }

        def parse(args: Seq[String]): CliArgs = {
            optionsParser
                .parse(args, initOptions)
                .getOrElse(throw new IllegalArgumentException("Could not parse arguments"))
        }

    }

}
