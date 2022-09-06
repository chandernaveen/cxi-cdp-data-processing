package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink

import refined_zone.pos_parbrink.config.ProcessorConfig
import refined_zone.pos_parbrink.processor._
import refined_zone.pos_parbrink.processor.identity.PosIdentityProcessor
import support.crypto_shredding.config.CryptoShreddingConfig
import support.normalization.DateNormalization
import support.utils.ContractUtils
import support.SparkSessionFactory

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

import java.nio.file.Paths

object RawRefinedParbrinkPartnerJob {

    private val logger = Logger.getLogger(this.getClass.getName)

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val cliArgs = CliArgs.parse(args)
        logger.info(s"Parsed args: $cliArgs")

        run(cliArgs)(SparkSessionFactory.getSparkSession())
    }

    def run(cliArgs: CliArgs)(implicit spark: SparkSession): Unit = {

        val contract: ContractUtils = new ContractUtils(Paths.get("/mnt/" + cliArgs.contractPath))

        val cxiPartnerId = contract.prop[String]("partner.cxiPartnerId")
        val srcDbName = contract.prop[String](getSchemaRawPath("db_name"))
        val srcTable = contract.prop[String](getSchemaRawPath("all_record_types_table"))
        val refinedDbName = contract.prop[String](getSchemaRefinedPath("db_name"))

        val processorCommonConfig = ProcessorConfig(
            contract,
            DateNormalization.parseToLocalDate(cliArgs.feedDate),
            cxiPartnerId,
            cliArgs.runId,
            srcDbName,
            srcTable
        )

        val cryptoShreddingConfig = CryptoShreddingConfig(
            cxiSource = processorCommonConfig.cxiPartnerId,
            lookupDestDbName = contract.prop[String]("schema.crypto.db_name"),
            lookupDestTableName = contract.prop[String]("schema.crypto.lookup_table"),
            workspaceConfigPath = contract.prop[String]("databricks_workspace_config"),
            date = processorCommonConfig.date,
            runId = processorCommonConfig.runId
        )

        LocationsProcessor.process(spark, processorCommonConfig, refinedDbName)
        CategoriesProcessor.process(spark, processorCommonConfig, refinedDbName)
        MenuItemsProcessor.process(spark, processorCommonConfig, refinedDbName)
        val customers = CustomersProcessor.process(spark, processorCommonConfig, cryptoShreddingConfig, refinedDbName)
        OrderTendersProcessor.process(spark, processorCommonConfig, refinedDbName)
        HouseAccountsProcessor.process(spark, processorCommonConfig, refinedDbName)
        val payments = PaymentsProcessor.process(spark, processorCommonConfig, refinedDbName)
        val identitiesByOrder = PosIdentityProcessor.process(
            spark,
            processorCommonConfig,
            cryptoShreddingConfig,
            payments,
            customers
        )
        OrderSummaryProcessor.process(
            spark,
            identitiesByOrder,
            processorCommonConfig,
            cryptoShreddingConfig,
            refinedDbName
        )
    }

    def getSchemaRawPath(relativePath: String): String = s"schema.raw.$relativePath"

    def getSchemaRefinedPath(relativePath: String): String = s"schema.refined.$relativePath"

    def getSchemaRefinedHubPath(relativePath: String): String = s"schema.refined_hub.$relativePath"
}

case class CliArgs(contractPath: String = null, feedDate: String = null, runId: String = null)

object CliArgs {

    private val initOptions = CliArgs()

    private def optionsParser: OptionParser[CliArgs] =
        new scopt.OptionParser[CliArgs]("POS Parbrink Raw to Refined Job") {

            opt[String]("contract-path")
                .action((contractPath, c) => c.copy(contractPath = contractPath))
                .text("path to a contract for this job")
                .required

            opt[String]("feedDate")
                .action((feedDate, c) => c.copy(feedDate = feedDate))
                .text("processing date")
                .required()

            opt[String]("runId")
                .action((runId, c) => c.copy(runId = runId))
                .text("unique run id for this job")
                .required()
        }

    def parse(args: Seq[String]): CliArgs = {
        optionsParser
            .parse(args, initOptions)
            .getOrElse(throw new IllegalArgumentException("Could not parse arguments"))
    }

}
