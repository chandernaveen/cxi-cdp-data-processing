package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink

import refined_zone.pos_parbrink.config.ProcessorConfig
import refined_zone.pos_parbrink.processor._
import refined_zone.pos_parbrink.processor.identity.PosIdentityProcessor
import support.audit.{AuditLogs, LogContext}
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
    val baseAuditPropName = "jobs.databricks.raw_to_refined_job.job_config.audit"

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val cliArgs = CliArgs.parse(args)
        logger.info(s"Parsed args: $cliArgs")

        run(cliArgs)(SparkSessionFactory.getSparkSession())
    }

    // scalastyle:off  method.length
    def run(cliArgs: CliArgs)(implicit spark: SparkSession): Unit = {
        val contract: ContractUtils = new ContractUtils(Paths.get("/mnt/" + cliArgs.contractPath))
        val cxiPartnerId = contract.prop[String]("partner.cxiPartnerId")
        val srcDbName = contract.prop[String](getSchemaRawPath("db_name"))
        val srcTable = contract.prop[String](getSchemaRawPath("all_record_types_table"))
        val refinedDbName = contract.prop[String](getSchemaRefinedPath("db_name"))
        val processStartTime = java.time.LocalDateTime.now.toString
        val processorCommonConfig =
            ProcessorConfig(
                contract,
                DateNormalization.parseToLocalDate(cliArgs.feedDate),
                cxiPartnerId,
                cliArgs.runId,
                cliArgs.refinedFullProcess,
                srcDbName,
                srcTable
            )
        val cryptoShreddingConfig =
            CryptoShreddingConfig(
                cxiSource = processorCommonConfig.cxiPartnerId,
                lookupDestDbName = contract.prop[String]("schema.crypto.db_name"),
                lookupDestTableName = contract.prop[String]("schema.crypto.lookup_table"),
                workspaceConfigPath = contract.prop[String]("databricks_workspace_config"),
                date = processorCommonConfig.date,
                runId = processorCommonConfig.runId
            )
        try {
            LocationsProcessor.process(spark, processorCommonConfig, refinedDbName)
            CategoriesProcessor.process(spark, processorCommonConfig, refinedDbName)
            MenuItemsProcessor.process(spark, processorCommonConfig, refinedDbName)
            val customers =
                CustomersProcessor.process(spark, processorCommonConfig, cryptoShreddingConfig, refinedDbName)
            OrderTendersProcessor.process(spark, processorCommonConfig, refinedDbName)
            HouseAccountsProcessor.process(spark, processorCommonConfig, refinedDbName)
            val payments = PaymentsProcessor.process(spark, processorCommonConfig, refinedDbName)
            val identitiesByOrder =
                PosIdentityProcessor.process(spark, processorCommonConfig, cryptoShreddingConfig, payments, customers)
            OrderSummaryProcessor.process(
                spark,
                identitiesByOrder,
                processorCommonConfig,
                cryptoShreddingConfig,
                refinedDbName
            )
            val logContext =
                getLogContext(config = processorCommonConfig, writeStatus = "0", errorMessage = "", processStartTime)
            AuditLogs.write(logContext)(spark)
        } catch {
            case e: Exception =>
                val logContext =
                    getLogContext(
                        config = processorCommonConfig,
                        writeStatus = "1",
                        errorMessage = e.toString,
                        processStartTime
                    )
                AuditLogs.write(logContext)(spark)
                logger.error(
                    s"Failed to process refined Zone for ${logContext.entity}.${logContext.subEntity}. Due to error: ${e.toString}"
                )
                throw e
        }
    }
    private def getLogContext(
        config: ProcessorConfig,
        writeStatus: String,
        errorMessage: String,
        processStartTime: String,
        processEndTime: String = java.time.LocalDateTime.now.toString
    ): LogContext = {
        val processName = config.contract.propOrElse(getauditPath("processName"), "parbrink_refined")
        val sourceEntity = config.contract.propOrElse(getauditPath("sourceEntity"), "parbrink")
        val subEntity = config.contract.propOrElse(getauditPath("subEntity"), "")
        val logTable = config.contract.propOrElse(getauditPath("logTable"), "logs.application_audit_logs")

        LogContext(
            logTable = logTable,
            processName = processName,
            entity = sourceEntity,
            subEntity = subEntity,
            runID = 1,
            dpYear = config.date.getYear,
            dpMonth = config.date.getMonthValue,
            dpDay = config.date.getDayOfMonth,
            dpHour = 0,
            processStartTime = processStartTime,
            processEndTime = processEndTime,
            writeStatus = writeStatus,
            errorMessage = errorMessage
        )
    }

    def getSchemaRawPath(relativePath: String): String = s"schema.raw.$relativePath"
    def getSchemaRefinedPath(relativePath: String): String = s"schema.refined.$relativePath"
    def getSchemaRefinedHubPath(relativePath: String): String = s"schema.refined_hub.$relativePath"

    def getauditPath(relativePath: String): String = s"$baseAuditPropName.$relativePath"
}

case class CliArgs(
    contractPath: String = null,
    feedDate: String = null,
    runId: String = null,
    refinedFullProcess: String = "false"
)

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

            opt[String]("refinedFullProcess")
                .action((refinedFullProcess, c) => c.copy(refinedFullProcess = refinedFullProcess))
                .text("FullProcess Flag for this job")
                .required()
        }

    def parse(args: Seq[String]): CliArgs = {
        optionsParser
            .parse(args, initOptions)
            .getOrElse(throw new IllegalArgumentException("Could not parse arguments"))
    }

}
