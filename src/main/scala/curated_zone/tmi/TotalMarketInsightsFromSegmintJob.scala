package com.cxi.cdp.data_processing
package curated_zone.tmi

import support.utils.mongodb.MongoDbConfigUtils
import support.utils.ContractUtils
import support.SparkSessionFactory

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.Paths

/** This job populates total market insights from data provided by Segmint.
  *
  * This is required for beta until we have more partners.
  */
object TotalMarketInsightsFromSegmintJob {
    private val logger = Logger.getLogger(this.getClass.getName)

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val cliArgs = CliArgs.parse(args)
        logger.info(s"Parsed args: $cliArgs")

        run(cliArgs)(SparkSessionFactory.getSparkSession())
    }

    def run(cliArgs: CliArgs)(implicit spark: SparkSession): Unit = {
        import TotalMarketInsightsJob.{writeToDatalakeTotalMarketInsights, writeToMongoTotalMarketInsights}

        val contract: ContractUtils = new ContractUtils(Paths.get("/mnt/" + cliArgs.contractPath))

        val refinedSegmintDb = contract.prop[String]("schema.refined_segmint.db_name")
        val refinedSegmintTable = contract.prop[String]("schema.refined_segmint.segmint_table")

        val curatedDb = contract.prop[String]("schema.curated.db_name")
        val totalMarketInsightsTable = contract.prop[String]("schema.curated.total_market_insights_table")

        val mongoDbName = contract.prop[String]("mongo.db")
        val totalMarketInsightsMongoCollectionName = contract.prop[String]("mongo.total_market_insights_collection")

        val mongoDbConfig = MongoDbConfigUtils.getMongoDbConfig(spark, contract)

        val totalMarketInsightsSource: DataFrame =
            readTotalMarketInsights(s"$refinedSegmintDb.$refinedSegmintTable")

        val totalMarketInsights: DataFrame = transformTotalMarketInsights(totalMarketInsightsSource).cache()

        writeToDatalakeTotalMarketInsights(
            totalMarketInsights,
            s"$curatedDb.$totalMarketInsightsTable",
            cliArgs.overwrite
        )
        writeToMongoTotalMarketInsights(
            totalMarketInsights,
            mongoDbConfig.uri,
            mongoDbName,
            totalMarketInsightsMongoCollectionName,
            cliArgs.overwrite
        )
    }

    def readTotalMarketInsights(refinedSegmintTable: String)(implicit spark: SparkSession): DataFrame = {
        spark.sqlContext.sql(
            s"""
                SELECT
                    cuisine_category AS location_type, region, state,
                    city, date, transaction_amount, transaction_quantity
                FROM ${refinedSegmintTable}
            """
        )
    }

    def transformTotalMarketInsights(refinedSegmintTable: DataFrame)(implicit spark: SparkSession): DataFrame = {
        refinedSegmintTable.createOrReplaceTempView("sourceData")
        spark.sqlContext.sql(
            s"""
                SELECT
                    location_type, region, state, city, date,
                    SUM(transaction_amount)  AS transaction_amount,
                    SUM(transaction_quantity) AS transaction_quantity
                FROM sourceData
                group by 1, 2, 3, 4, 5
            """
        )
    }

    case class CliArgs(contractPath: String, overwrite: Boolean = false)

    object CliArgs {

        private val initOptions = CliArgs(contractPath = null)

        private def optionsParser = new scopt.OptionParser[CliArgs]("Total Market Insight From Segmint Job") {

            opt[String]("contract-path")
                .action((contractPath, c) => c.copy(contractPath = contractPath))
                .text("path to a contract for this job")
                .required

            opt[Boolean]("overwrite")
                .action((overwrite, c) => c.copy(overwrite = overwrite))
                .text("if true, remove data from DataLake/MongoDB before import")
                .optional
        }

        def parse(args: Seq[String]): CliArgs = {
            optionsParser
                .parse(args, initOptions)
                .getOrElse(throw new IllegalArgumentException("Could not parse arguments"))
        }
    }

}
