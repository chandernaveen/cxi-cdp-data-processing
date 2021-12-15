package com.cxi.cdp.data_processing
package refined_zone.pos_square

import support.SparkSessionFactory
import support.packages.utils.ContractUtils

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.nio.file.Paths

object RawRefinedSquarePartnerJob {
    private val logger = Logger.getLogger(this.getClass.getName)

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val contractPath = args(0)
        val date = args(1)
        val spark = SparkSessionFactory.getSparkSession()
        run(spark, contractPath, date)
    }

    def run(spark: SparkSession, contractPath: String, date: String): Unit = {
        val contract: ContractUtils = new ContractUtils(Paths.get(contractPath))

        val cxiPartnerId = contract.prop[String]("partner.cxiPartnerId")
        val srcDbName = contract.prop[String](getSchemaRawPath("db_name"))
        val srcTable = contract.prop[String](getSchemaRawPath("all_record_types_table"))
        val destDbName = contract.prop[String](getSchemaRefinedPath("db_name"))
        val refinedHubDestDbName = contract.prop[String](getSchemaRefinedHubPath("db_name"))

        LocationsProcessor.process(spark, contract, date, cxiPartnerId, srcDbName, srcTable, destDbName)
        CategoriesProcessor.process(spark, contract, date, cxiPartnerId, srcDbName, srcTable, destDbName)
        MenuItemsProcessor.process(spark, contract, date, cxiPartnerId, srcDbName, srcTable, destDbName)
        CustomersProcessor.process(spark, contract, date, cxiPartnerId, srcDbName, srcTable, destDbName)
        val payments = PaymentsProcessor.process(spark, contract, date, cxiPartnerId, srcDbName, srcTable, destDbName)
        val cxiCustomerIdsByOrder = CxiCustomersProcessor.process(spark, contract, date, cxiPartnerId, srcDbName, srcTable, refinedHubDestDbName, payments)
        OrderTaxesProcessor.process(spark, contract, date, cxiPartnerId, srcDbName, srcTable, destDbName)
        OrderTenderTypesProcessor.process(spark, contract, date, cxiPartnerId, srcDbName, srcTable, destDbName)
        OrderSummaryProcessor.process(spark, contract, date, cxiPartnerId, srcDbName, srcTable, destDbName, cxiCustomerIdsByOrder)
    }

    def getSchemaRawPath(relativePath: String): String = s"schema.raw.$relativePath"
    def getSchemaRefinedPath(relativePath: String): String = s"schema.refined.$relativePath"
    def getSchemaRefinedHubPath(relativePath: String): String = s"schema.refined_hub.$relativePath"

}