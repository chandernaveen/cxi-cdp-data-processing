package com.cxi.cdp.data_processing
package refined_zone.pos_square

import refined_zone.pos_square.RawRefinedSquarePartnerJob.getSchemaRefinedPath
import support.packages.utils.ContractUtils

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object CustomersProcessor {
    def process(spark: SparkSession,
                contract: ContractUtils,
                date: String,
                cxiPartnerId: String,
                srcDbName: String,
                srcTable: String,
                destDbName: String): Unit = {

        val customersTable = contract.prop[String](getSchemaRefinedPath("customer_table"))

        val customers = readCustomers(spark, date, s"$srcDbName.$srcTable")

        val transformedCustomers = transformCustomers(customers, cxiPartnerId)

        writeCustomers(transformedCustomers, cxiPartnerId, s"$destDbName.$customersTable")
    }

    def readCustomers(spark: SparkSession, date: String, table: String): DataFrame = {
        spark.sql(
            s"""
               |SELECT
               |get_json_object(record_value, "$$.id") as customer_id,
               |get_json_object(record_value, "$$.email_address") as email_address,
               |get_json_object(record_value, "$$.phone_number") as phone_number,
               |get_json_object(record_value, "$$.given_name") as first_name,
               |get_json_object(record_value, "$$.family_name") as last_name,
               |get_json_object(record_value, "$$.created_at") as created_at,
               |get_json_object(record_value, "$$.version") as version
               |FROM $table
               |WHERE record_type = "customers" AND feed_date = "$date"
               |""".stripMargin)
    }

    def transformCustomers(customers: DataFrame,
                           cxiPartnerId: String): DataFrame = {
        val transformedCustomers = customers
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .dropDuplicates("cxi_partner_id","customer_id")
        transformedCustomers
    }

    def writeCustomers(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newCustomers"

        df.createOrReplaceTempView(srcTable)
        // TODO: consider using SCD type 2 based on version column as well
        df.sqlContext.sql(
            s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id = "$cxiPartnerId" AND $destTable.customer_id = $srcTable.customer_id
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

}