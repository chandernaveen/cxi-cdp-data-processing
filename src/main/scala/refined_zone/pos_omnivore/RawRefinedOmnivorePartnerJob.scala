package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore

import refined_zone.pos_omnivore.config.ProcessorConfig
import support.audit.{AuditLogs, LogContext}
import support.normalization.DateNormalization
import support.utils.ContractUtils
import support.SparkSessionFactory

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.nio.file.Paths

object RawRefinedOmnivorePartnerJob {

    private val logger = Logger.getLogger(this.getClass.getName)
    val baseAuditPropName = "jobs.databricks.raw_to_refined_job.job_config.audit"

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val contractPath = args(0)
        val date = args(1)
        val runId = args(2)
        val spark = SparkSessionFactory.getSparkSession()
        run(spark, contractPath, date, runId)
    }

    def run(spark: SparkSession, contractPath: String, date: String, runId: String): Unit = {
        val contract: ContractUtils = new ContractUtils(Paths.get(contractPath))

        val cxiPartnerId = contract.prop[String]("partner.cxiPartnerId")
        val srcDbName = contract.prop[String](getSchemaRawPath("db_name"))
        val srcTable = contract.prop[String](getSchemaRawPath("all_record_types_table"))
        val destDbName = contract.prop[String](getSchemaRefinedPath("db_name"))
        val processStartTime = java.time.LocalDateTime.now.toString
        val processorCommonConfig = ProcessorConfig(
            contract,
            DateNormalization.parseToLocalDate(date),
            cxiPartnerId,
            runId,
            srcDbName,
            srcTable
        )
        try {

            LocationProcessor.process(spark, processorCommonConfig, destDbName)

            CategoriesProcessor.process(spark, processorCommonConfig, destDbName)

            ItemsProcessor.process(spark, processorCommonConfig, destDbName)

            OrderTendersProcessor.process(spark, processorCommonConfig, destDbName)

            PaymentsProcessor.process(spark, processorCommonConfig, destDbName)

            val posIdentityIdsByOrder = PosIdentityProcessor.process(spark, processorCommonConfig)

            OrderSummaryProcessor.process(spark, processorCommonConfig, destDbName, posIdentityIdsByOrder)

            val logContext =
                getLogContext(config = processorCommonConfig, writeStatus = "0", errorMessage = "", processStartTime)
            AuditLogs.write(logContext)(spark)
        } catch {
            case e: Exception =>
                // If run failed write Audit log table to indicate data processing failure
                val logContext = getLogContext(
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
        val processName = config.contract.propOrElse(getauditPath("processName"), "omnivore_refined")
        val sourceEntity = config.contract.propOrElse(getauditPath("sourceEntity"), "omnivore")
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
