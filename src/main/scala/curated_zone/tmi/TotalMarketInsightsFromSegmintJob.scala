package com.cxi.cdp.data_processing
package curated_zone.tmi

import support.SparkSessionFactory
import support.utils.ContractUtils
import support.utils.mongodb.MongoDbConfigUtils

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

        val refinedHubDb = contract.prop[String]("schema.refined_hub.db_name")
        val postalCodeTable = contract.prop[String]("schema.refined_hub.postal_code_table")

        val curatedDb = contract.prop[String]("schema.curated.db_name")
        val totalMarketInsightsTable = contract.prop[String]("schema.curated.total_market_insights_table")

        val mongoDbConfig = MongoDbConfigUtils.getMongoDbConfig(spark, contract)

        val totalMarketInsights: DataFrame =
            readTotalMarketInsights("/mnt/" + cliArgs.dataPath, s"$refinedHubDb.$postalCodeTable").cache()

        writeToDatalakeTotalMarketInsights(totalMarketInsights, s"$curatedDb.$totalMarketInsightsTable", cliArgs.overwrite)
        writeToMongoTotalMarketInsights(totalMarketInsights, mongoDbConfig.uri, contract, cliArgs.overwrite)
    }

    def readTotalMarketInsights(dataPath: String, postalCodeTable: String)(implicit spark: SparkSession): DataFrame = {
        // we want to use records from 2020 instead of 2021 as there are more of them and they are of better quality
        // also shift months by 1 to match our real partner data for demo
        spark.sqlContext.sql(
            s"""
                SELECT * FROM (
                    SELECT
                        CASE WHEN d.location_type = 'FOOD AND DINING' THEN 'Restaurant' ELSE 'Unknown' END AS location_type,
                        COALESCE(p.region, 'Unknown') as region,
                        UPPER(COALESCE(p.state_code, d.state)) AS state,
                        INITCAP(COALESCE(
                          p.city,
                          CASE WHEN d.city_name = '' THEN null ELSE d.city_name END
                        )) AS city,
                        ADD_MONTHS(TO_DATE(d.ord_date, "MMyyyy"), 13) AS date,
                        CAST(SUM(d.total_amount) AS Decimal(9,2)) AS transaction_amount,
                        SUM(d.total_quantity) AS transaction_quantity
                    FROM delta.`$dataPath` d
                    LEFT JOIN $postalCodeTable p
                        ON d.postal_code = p.postal_code
                    GROUP BY 1, 2, 3, 4, 5
                ) t
                WHERE YEAR(date) = 2021
            """
        )
    }

    case class CliArgs(contractPath: String, dataPath: String, overwrite: Boolean = false)

    object CliArgs {

        private val initOptions = CliArgs(contractPath = null, dataPath = null)

        private def optionsParser = new scopt.OptionParser[CliArgs]("Total Market Insight From Segmint Job") {

            opt[String]("contract-path")
                .action((contractPath, c) => c.copy(contractPath = contractPath))
                .text("path to a contract for this job")
                .required

            opt[String]("data-path")
                .action((dataPath, c) => c.copy(dataPath = dataPath))
                .text("path to Segmint data")
                .required

            opt[Boolean]("overwrite")
                .action((overwrite, c) => c.copy(overwrite = overwrite))
                .text("if true, remove data from DataLake/MongoDB before import")
                .optional
        }

        def parse(args: Seq[String]): CliArgs = {
            optionsParser.parse(args, initOptions)
                .getOrElse(throw new IllegalArgumentException("Could not parse arguments"))
        }
    }

}
