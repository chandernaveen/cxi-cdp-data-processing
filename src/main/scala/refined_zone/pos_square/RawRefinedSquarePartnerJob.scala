package com.cxi.cdp.data_processing
package refined_zone.pos_square

import com.cxi.cdp.data_processing.support.audit.{AuditLogs, LogContext}
import refined_zone.pos_square.config.ProcessorConfig
import support.normalization.udf.DateNormalizationUdfs.parseToSqlDateIsoFormat
import support.normalization.udf.TimestampNormalizationUdfs.parseToTimestampIsoDateTime
import support.normalization.DateNormalization
import support.utils.ContractUtils
import support.SparkSessionFactory

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.nio.file.Paths

object RawRefinedSquarePartnerJob {

    private val logger = Logger.getLogger(this.getClass.getName)
    val baseAuditPropName = "jobs.databricks.raw_to_refined_job.job_config.audit"

    /** Utilize IsoDateTimeConverter as POS Square uses RFC 3339 format for timestamps
      * https://developer.squareup.com/docs/build-basics/common-data-types/working-with-dates
      */
    private[pos_square] final val parsePosSquareTimestamp = parseToTimestampIsoDateTime

    /** Utilize standard ISO date formatter as POS Square uses ISO 8601 format (YYYY-MM-DD) for dates
      * https://developer.squareup.com/docs/build-basics/common-data-types/working-with-dates
      */
    private[pos_square] final val parsePosSquareDate = parseToSqlDateIsoFormat

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
            LocationsProcessor.process(spark, processorCommonConfig, destDbName)
            CategoriesProcessor.process(spark, processorCommonConfig, destDbName)
            val menuItemTable = contract.prop[String](getSchemaRefinedPath("item_table"))
            MenuItemsProcessor.process(
                spark,
                cxiPartnerId,
                date,
                s"$srcDbName.$srcTable",
                s"$destDbName.$menuItemTable"
            )
            CustomersProcessor.process(spark, processorCommonConfig, destDbName)
            val payments = PaymentsProcessor.process(spark, processorCommonConfig, destDbName)
            val cxiIdentityIdsByOrder = CxiIdentityProcessor.process(spark, processorCommonConfig, payments)

            OrderTaxesProcessor.process(spark, processorCommonConfig, destDbName)
            OrderTendersProcessor.process(spark, processorCommonConfig, destDbName)
            OrderSummaryProcessor.process(spark, processorCommonConfig, destDbName, cxiIdentityIdsByOrder)
            val logContext =
                getLogContext(config = processorCommonConfig, writeStatus = "0", errorMessage = "", processStartTime)
            AuditLogs.write(logContext)(spark)
        } catch {
            case e: Exception =>
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
        val processName = config.contract.propOrElse(getauditPath("processName"), "square_refined")
        val sourceEntity = config.contract.propOrElse(getauditPath("sourceEntity"), "sqaure")
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
