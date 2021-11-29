package com.cxi.cdp.data_processing
package refined_zone.pos_square

import refined_zone.pos_square.RawRefinedSquarePartnerJob.getSchemaRefinedPath
import support.packages.utils.ContractUtils

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

object PaymentsProcessor {
    def process(spark: SparkSession,
                contract: ContractUtils,
                date: String,
                cxiPartnerId: String,
                srcDbName: String,
                srcTable: String,
                destDbName: String): DataFrame = {

        val paymentTable = contract.prop[String](getSchemaRefinedPath("payment_table"))

        val payments = readPayments(spark, date, s"$srcDbName.$srcTable")

        val processedPayments = transformPayments(payments, cxiPartnerId)
            .cache()

        writePayments(processedPayments, cxiPartnerId, s"$destDbName.$paymentTable")
        processedPayments
    }

    def readPayments(spark: SparkSession, date: String, table: String): DataFrame = {
        spark.sql(
            s"""
               |SELECT
               |get_json_object(record_value, "$$.id") as payment_id,
               |get_json_object(record_value, "$$.order_id") as order_id,
               |get_json_object(record_value, "$$.location_id") as location_id,
               |get_json_object(record_value, "$$.status") as status,
               |get_json_object(record_value, "$$.card_details.card.cardholder_name") as name,
               |get_json_object(record_value, "$$.card_details.card.card_brand") as card_brand,
               |get_json_object(record_value, "$$.card_details.card.last_4") as pan,
               |get_json_object(record_value, "$$.card_details.card.bin") as bin,
               |get_json_object(record_value, "$$.card_details.card.exp_month") as exp_month,
               |get_json_object(record_value, "$$.card_details.card.exp_year") as exp_year
               |FROM $table
               |WHERE record_type = "payments" AND feed_date = "$date"
               |""".stripMargin)
    }

    def transformPayments(payments: DataFrame, cxiPartnerId: String): DataFrame = {
        payments
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .dropDuplicates("cxi_partner_id", "location_id", "order_id", "payment_id")
    }

    def writePayments(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newPayments"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(
            s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id <=> "$cxiPartnerId" AND $destTable.location_id <=> $srcTable.location_id AND $destTable.order_id <=> $srcTable.order_id AND $destTable.payment_id <=> $srcTable.payment_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

}
