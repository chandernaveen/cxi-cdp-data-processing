package com.cxi.cdp.data_processing
package curated_zone.tmi

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

object TotalMarketInsightsJob {

    private val logger = Logger.getLogger(this.getClass.getName)

    final val CdfConsumerId = "total_market_insights_job"

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
                orderSummaryCdf.queryAllData(CdfConsumerId)
            } else {
                orderSummaryCdf.queryChangeData(CdfConsumerId)
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

        val curatedDb = contract.prop[String]("schema.curated.db_name")
        val partnerMarketInsightsTable = contract.prop[String]("schema.curated.partner_market_insights_table")
        val totalMarketInsightsTable = contract.prop[String]("schema.curated.total_market_insights_table")

        val mongoDbConfig = MongoDbConfigUtils.getMongoDbConfig(spark, contract)
        val mongoDbName = contract.prop[String]("mongo.db")
        val partnerMarketInsightsMongoCollectionName = contract.prop[String]("mongo.partner_market_insights_collection")
        val totalMarketInsightsMongoCollectionName = contract.prop[String]("mongo.total_market_insights_collection")

        val orderSummary: DataFrame =
            readOrderSummary(orderDates, s"$refinedHubDb.$orderSummaryTable", s"$refinedHubDb.$locationTable")

        val partnerMarketInsights: DataFrame = computePartnerMarketInsights(orderSummary).cache()
        writeToDatalakePartnerMarketInsights(
            partnerMarketInsights,
            s"$curatedDb.$partnerMarketInsightsTable",
            fullReprocess
        )
        writeToMongoPartnerMarketInsights(
            partnerMarketInsights,
            mongoDbConfig.uri,
            mongoDbName,
            partnerMarketInsightsMongoCollectionName,
            fullReprocess
        )

        if (shouldComputeTotalMarketInsights(contract)) {
            val totalMarketInsights: DataFrame = computeTotalMarketInsights(partnerMarketInsights).cache()
            writeToDatalakeTotalMarketInsights(
                totalMarketInsights,
                s"$curatedDb.$totalMarketInsightsTable",
                fullReprocess
            )
            writeToMongoTotalMarketInsights(
                totalMarketInsights,
                mongoDbConfig.uri,
                mongoDbName,
                totalMarketInsightsMongoCollectionName,
                fullReprocess
            )
        } else {
            logger.info("Skip calculation of total market insights based on a contract")
        }
    }

    private def shouldComputeTotalMarketInsights(contract: ContractUtils): Boolean = {
        contract.propOrElse[Boolean](
            "jobs.databricks.total_market_insights_job.job_config.compute_total_market_insights",
            true
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

    def readOrderSummary(orderDates: Set[String], orderSummaryTable: String, locationTable: String)(implicit
        spark: SparkSession
    ): DataFrame = {
        import spark.implicits._

        val orderSummaryDF = spark.table(orderSummaryTable)
        val locationDF = spark.table(locationTable)

        val getLocationTypeUdf = udf(getLocationType _)

        orderSummaryDF
            .filter($"ord_state_id" === OrderStateType.Completed.code && $"ord_date".isInCollection(orderDates))
            .join(locationDF, usingColumns = Seq("cxi_partner_id", "location_id"))
            .select(
                orderSummaryDF("cxi_partner_id"),
                getLocationTypeUdf(locationDF("location_type")).as("location_type"),
                locationDF("region"),
                upper(locationDF("state_code")).as("state"),
                initcap(locationDF("city")).as("city"),
                col("location_id"),
                col("location_nm"),
                orderSummaryDF("ord_date"),
                orderSummaryDF("ord_pay_total"),
                orderSummaryDF("ord_id")
            )
    }

    def getLocationType(locationTypeCode: Int): String = {
        LocationType.withValueOpt(locationTypeCode).getOrElse(LocationType.Unknown).name
    }

    def computePartnerMarketInsights(orderSummary: DataFrame): DataFrame = {
        import orderSummary.sparkSession.implicits._

        orderSummary
            .dropDuplicates("cxi_partner_id", "location_id", "ord_date", "ord_id")
            .groupBy("cxi_partner_id", "location_type", "region", "state", "city", "location_id", "ord_date")
            .agg(
                min("location_nm") as "location_nm",
                sum("ord_pay_total") as "transaction_amount",
                count("ord_id").cast(IntegerType) as "transaction_quantity"
            )
            .withColumnRenamed("ord_date", "date")
            .select(
                $"cxi_partner_id",
                $"location_type",
                $"region",
                $"state",
                $"city",
                $"location_id",
                $"location_nm",
                $"date",
                $"transaction_amount",
                $"transaction_quantity"
            )
    }

    def writeToDatalakePartnerMarketInsights(
        partnerMarketInsights: DataFrame,
        destTable: String,
        fullReprocess: Boolean = false
    ): Unit = {
        if (fullReprocess) {
            partnerMarketInsights.sqlContext.sql(s"DELETE FROM $destTable")
        }
        val srcTable = "newPartnerMarketInsight"

        partnerMarketInsights.createOrReplaceTempView(srcTable)

        // <=> comparison operator is used for null-safe semantics
        // https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-null-semantics.html#null-semantics
        partnerMarketInsights.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id <=> $srcTable.cxi_partner_id
               | AND $destTable.date <=> $srcTable.date
               | AND $destTable.location_id <=> $srcTable.location_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

    def writeToMongoPartnerMarketInsights(
        partnerMarketInsights: DataFrame,
        mongoDbUri: String,
        dbName: String,
        collectionName: String,
        fullReprocess: Boolean = false
    ): Unit = {
        val saveMode = if (fullReprocess) SaveMode.Overwrite else SaveMode.Append

        // either insert or update a document in Mongo based on these fields
        val shardKey = """{"cxi_partner_id": 1, "date": 1, "location_id": 1}"""

        partnerMarketInsights.write
            .format(MongoSparkConnectorClass)
            .mode(saveMode)
            .option("database", dbName)
            .option("collection", collectionName)
            .option("uri", mongoDbUri)
            .option("replaceDocument", "true")
            .option("shardKey", shardKey)
            .save()
    }

    def computeTotalMarketInsights(partnerMarketInsights: DataFrame): DataFrame = {
        partnerMarketInsights
            .groupBy("location_type", "region", "state", "city", "date")
            .agg(
                sum("transaction_amount").as("transaction_amount"),
                sum("transaction_quantity").as("transaction_quantity")
            )
            .select("location_type", "region", "state", "city", "date", "transaction_amount", "transaction_quantity")
    }

    def writeToDatalakeTotalMarketInsights(
        totalMarketInsights: DataFrame,
        destTable: String,
        fullReprocess: Boolean = false
    ): Unit = {
        if (fullReprocess) {
            totalMarketInsights.sqlContext.sql(s"DELETE FROM $destTable")
        }

        val srcTable = "newTotalMarketInsight"
        totalMarketInsights.createOrReplaceTempView(srcTable)

        // <=> comparison operator is used for null-safe semantics
        // https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-null-semantics.html#null-semantics
        totalMarketInsights.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.location_type <=> $srcTable.location_type
               | AND $destTable.date <=> $srcTable.date
               | AND $destTable.region <=> $srcTable.region
               | AND $destTable.state <=> $srcTable.state
               | AND $destTable.city <=> $srcTable.city
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

    def writeToMongoTotalMarketInsights(
        totalMarketInsights: DataFrame,
        mongoDbUri: String,
        dbName: String,
        collectionName: String,
        fullReprocess: Boolean = false
    ): Unit = {
        // either insert or update a document in Mongo based on these fields
        val shardKey = """{"date": 1, "location_type": 1, "region": 1, "state": 1, "city": 1}"""

        val saveMode = if (fullReprocess) SaveMode.Overwrite else SaveMode.Append

        totalMarketInsights.write
            .format(MongoSparkConnectorClass)
            .mode(saveMode)
            .option("database", dbName)
            .option("collection", collectionName)
            .option("uri", mongoDbUri)
            .option("replaceDocument", "true")
            .option("shardKey", shardKey) // allow updates based on these fields
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
