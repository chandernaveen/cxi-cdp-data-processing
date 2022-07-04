package com.cxi.cdp.data_processing
package refined_zone.pos_square

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

        val processorCommonConfig = ProcessorConfig(
            contract,
            DateNormalization.parseToLocalDate(date),
            cxiPartnerId,
            runId,
            srcDbName,
            srcTable
        )

        LocationsProcessor.process(spark, processorCommonConfig, destDbName)
        CategoriesProcessor.process(spark, processorCommonConfig, destDbName)
        val menuItemTable = contract.prop[String](getSchemaRefinedPath("item_table"))
        MenuItemsProcessor.process(spark, cxiPartnerId, date, s"$srcDbName.$srcTable", s"$destDbName.$menuItemTable")
        CustomersProcessor.process(spark, processorCommonConfig, destDbName)
        val payments = PaymentsProcessor.process(spark, processorCommonConfig, destDbName)
        val cxiIdentityIdsByOrder = CxiIdentityProcessor.process(spark, processorCommonConfig, payments)

        OrderTaxesProcessor.process(spark, processorCommonConfig, destDbName)
        OrderTendersProcessor.process(spark, processorCommonConfig, destDbName)
        OrderSummaryProcessor.process(spark, processorCommonConfig, destDbName, cxiIdentityIdsByOrder)
    }

    def getSchemaRawPath(relativePath: String): String = s"schema.raw.$relativePath"
    def getSchemaRefinedPath(relativePath: String): String = s"schema.refined.$relativePath"
    def getSchemaRefinedHubPath(relativePath: String): String = s"schema.refined_hub.$relativePath"

}
