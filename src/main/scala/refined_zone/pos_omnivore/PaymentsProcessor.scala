package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore

import raw_zone.pos_omnivore.model.Payment
import refined_zone.pos_omnivore.config.ProcessorConfig
import refined_zone.pos_omnivore.RawRefinedOmnivorePartnerJob.getSchemaRefinedPath

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

object PaymentsProcessor {
    def process(spark: SparkSession, config: ProcessorConfig, destDbName: String): Unit = {

        val paymentTable = config.contract.prop[String](getSchemaRefinedPath("payment_table"))

        val payments = readPayments(spark, config.dateRaw, s"${config.srcDbName}.${config.srcTable}")

        val processedPayments = transformPayments(payments, config.cxiPartnerId)
            .cache()

        writePayments(processedPayments, config.cxiPartnerId, s"$destDbName.$paymentTable")
    }

    def readPayments(spark: SparkSession, date: String, table: String): DataFrame = {
        spark.sql(s"""
               |SELECT
               |get_json_object(record_value, "$$._embedded.payments") as payments,
               |get_json_object(record_value, "$$.id") as order_id,
               |get_json_object(record_value, "$$._links.self.href") as location_url
               |FROM $table
               |WHERE record_type = "tickets" AND feed_date = "$date"
               |""".stripMargin)
    }

    def transformPayments(payments: DataFrame, cxiPartnerId: String): DataFrame = {
        val fieldLocation = 5
        payments
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn(
                "PaymentsStruct",
                from_json(col("payments"), DataTypes.createArrayType(Encoders.product[Payment].schema))
            )
            .withColumn("PaymentsArray", explode(col("PaymentsStruct")))
            .withColumn("payment_id", col("paymentsArray.id"))
            .withColumn("status", lit(null))
            .withColumn("name", col("paymentsArray.type"))
            .withColumn("location_id", split(col("location_url"), "/")(fieldLocation))
            .withColumn("card_brand", lit(null))
            .withColumn("pan", lit(null))
            .withColumn("bin", lit(null))
            .withColumn("exp_month", lit(null))
            .withColumn("exp_year", lit(null))
            .dropDuplicates("cxi_partner_id", "location_id", "order_id", "payment_id")
            .select(
                "cxi_partner_id",
                "payment_id",
                "status",
                "name",
                "location_id",
                "order_id",
                "card_brand",
                "pan",
                "bin",
                "exp_month",
                "exp_year"
            )
    }

    def writePayments(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newPayments"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id <=> "$cxiPartnerId" AND $destTable.location_id <=> $srcTable.location_id
               |  AND $destTable.order_id <=> $srcTable.order_id AND $destTable.payment_id <=> $srcTable.payment_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

}
