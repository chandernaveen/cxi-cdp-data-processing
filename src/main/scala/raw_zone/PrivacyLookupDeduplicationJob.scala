package com.cxi.cdp.data_processing
package raw_zone

import support.{SparkSessionFactory, WorkspaceConfigReader}
import support.change_data_feed.{ChangeDataFeedService, ChangeDataFeedSource}
import support.crypto_shredding.PrivacyFunctions.inAuthorizedContext
import support.utils.ContractUtils

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import scopt.OptionParser

import java.nio.file.Paths

object PrivacyLookupDeduplicationJob {
    final val CdfConsumerId = "privacy_lookup_deduplication_job"

    private val logger = Logger.getLogger(this.getClass.getName)

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val cliArgs = CliArgs.parse(args)
        logger.info(s"Parsed args: $cliArgs")

        run(cliArgs)(SparkSessionFactory.getSparkSession())
    }

    def run(cliArgs: CliArgs)(implicit spark: SparkSession): Unit = {
        val contract: ContractUtils = new ContractUtils(Paths.get("/mnt/" + cliArgs.contractPath))

        val cdfTrackerTable = getCdfTrackerTable(contract)
        val privacyIntermediateTable = getPrivacyIntermediateTable(contract)
        val privacyFinalTable = getPrivacyTable(contract)

        val workspaceConfigPath: String = contract.prop[String]("databricks_workspace_config")

        inAuthorizedContext(spark, WorkspaceConfigReader.readWorkspaceConfig(spark, workspaceConfigPath)) {
            val cdfService = new ChangeDataFeedService(cdfTrackerTable)
            val singleTable = new ChangeDataFeedSource.SingleTable(cdfService, privacyIntermediateTable)
            val changeDataResult =
                if (cliArgs.fullReprocess) {
                    singleTable.queryAllData(CdfConsumerId)
                } else {
                    cdfService.queryChangeData(CdfConsumerId, privacyIntermediateTable)
                }
            changeDataResult.data match {
                case None => logger.info("No updates found since the last run")

                case Some(changeData) =>
                    val feedDateToRunIdPairsToProcess = getFeedDateToRunIdPairsToProcess(changeData)
                    if (feedDateToRunIdPairsToProcess.isEmpty) {
                        logger.info(s"No feed date and run id pairs to process")
                    } else {
                        logger.info(s"Feed date and run id pairs to process: $feedDateToRunIdPairsToProcess")
                        process(privacyIntermediateTable, privacyFinalTable, feedDateToRunIdPairsToProcess)
                    }
                    logger.info(s"Update CDF tracker: ${changeDataResult.tableMetadataSeq}")
                    singleTable.markProcessed(changeDataResult)
            }
        }
    }

    def process(
        privacyIntermediateTableName: String,
        privacyFinalTableName: String,
        feedDateToRunIdPairsToProcess: Set[FeedDateToRunIdChangedData]
    )(implicit spark: SparkSession): Unit = {
        val privacyLookupIntermediateTable =
            readPrivacyLookupIntermediateTable(privacyIntermediateTableName, feedDateToRunIdPairsToProcess)
        val transformedPrivacyLookupIntermediateDf = transform(privacyLookupIntermediateTable)
        writePrivacyLookup(transformedPrivacyLookupIntermediateDf, privacyFinalTableName)
    }

    def readPrivacyLookupIntermediateTable(
        table: String,
        feedDateToRunIdPairsToProcess: Set[FeedDateToRunIdChangedData]
    )(implicit spark: SparkSession): DataFrame = {
        val filterCondition = feedDateToRunIdPairsToProcess
            .map(data => s"(feed_date='${data.feedDate}' AND run_id='${data.runId}')")
            .mkString(" OR ")

        spark
            .table(table)
            .select("cxi_source", "identity_type", "hashed_value", "original_value", "feed_date")
            .filter(filterCondition)
    }

    def transform(privacyLookupIntermediateTable: DataFrame): DataFrame = {
        privacyLookupIntermediateTable
            .dropDuplicates(Seq("cxi_source", "identity_type", "hashed_value"))
            .select("cxi_source", "identity_type", "hashed_value", "original_value", "feed_date")
    }

    def writePrivacyLookup(transformedPrivacyLookup: DataFrame, destTable: String): Unit = {
        val srcTable = "newPrivacyLookup"

        transformedPrivacyLookup.createOrReplaceTempView(srcTable)

        // <=> comparison operator is used for null-safe semantics
        // https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-null-semantics.html#null-semantics
        transformedPrivacyLookup.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_source <=> $srcTable.cxi_source
               | AND $destTable.identity_type <=> $srcTable.identity_type
               | AND $destTable.hashed_value <=> $srcTable.hashed_value
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

    private def getCdfTrackerTable(contract: ContractUtils): String = {
        val db = contract.prop[String]("datalake.data_services.db_name")
        val table = contract.prop[String]("datalake.data_services.cdf_tracker_table")
        s"$db.$table"
    }

    private def getPrivacyIntermediateTable(contract: ContractUtils): String = {
        val db = contract.prop[String]("datalake.crypto.db_name")
        val table = contract.prop[String]("datalake.crypto.lookup_intermediate_table")
        s"$db.$table"
    }

    private def getPrivacyTable(contract: ContractUtils): String = {
        val db = contract.prop[String]("datalake.crypto.db_name")
        val table = contract.prop[String]("datalake.crypto.lookup_table")
        s"$db.$table"
    }

    def getFeedDateToRunIdPairsToProcess(
        privacyIntermediateTableChangeData: DataFrame
    ): Set[FeedDateToRunIdChangedData] = {
        val feedDateColumnName = "feed_date"
        val runIdColumnName = "run_id"
        val feedDateColumn = col(feedDateColumnName)
        val runIdColumn = col(runIdColumnName)

        privacyIntermediateTableChangeData
            .select(feedDateColumn, runIdColumn)
            .filter(feedDateColumn.isNotNull and runIdColumn.isNotNull)
            .distinct
            .collect
            .map(r =>
                FeedDateToRunIdChangedData(
                    r.getAs[java.sql.Date](feedDateColumnName).toString,
                    r.getAs[String](runIdColumnName)
                )
            )
            .toSet
    }

    case class FeedDateToRunIdChangedData(feedDate: String, runId: String)

    case class CliArgs(contractPath: String, fullReprocess: Boolean = false)

    object CliArgs {

        private val initOptions = CliArgs(contractPath = null)

        private def optionsParser: OptionParser[CliArgs] =
            new scopt.OptionParser[CliArgs]("Privacy Deduplication Job") {

                opt[String]("contract-path")
                    .action((contractPath, c) => c.copy(contractPath = contractPath))
                    .text("path to a contract for this job")
                    .required

                opt[Boolean]("full-reprocess")
                    .action((fullReprocess, c) => c.copy(fullReprocess = fullReprocess))
                    .text("if true, read all data from intermediate table")
            }

        def parse(args: Seq[String]): CliArgs = {
            optionsParser
                .parse(args, initOptions)
                .getOrElse(throw new IllegalArgumentException("Could not parse arguments"))
        }

    }
}
