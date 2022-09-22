package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore

import raw_zone.pos_omnivore.model.Payment
import refined_zone.pos_omnivore.config.ProcessorConfig
import refined_zone.pos_omnivore.model.PosOmnivoreOrderTenderTypes.PosOmnivoreToCxiTenderType
import refined_zone.pos_omnivore.RawRefinedOmnivorePartnerJob.getSchemaRefinedPath
import support.normalization.udf.OrderTenderTypeNormalizationUdfs.normalizeOrderTenderType

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

object OrderTendersProcessor {

    def process(spark: SparkSession, config: ProcessorConfig, destDbName: String): Unit = {

        val orderTenderTable = config.contract.prop[String](getSchemaRefinedPath("order_tender_table"))

        val orderTenders = readOrderTenders(spark, config.dateRaw, s"${config.srcDbName}.${config.srcTable}")

        val processedOrderTenders = transformOrderTenders(orderTenders, config.cxiPartnerId)

        writeOrderTenders(processedOrderTenders, config.cxiPartnerId, s"$destDbName.$orderTenderTable")
    }

    def readOrderTenders(spark: SparkSession, date: String, table: String): DataFrame = {

        spark.sql(s"""
                     |SELECT
                     |get_json_object(record_value, "$$._embedded.payments") as payments,
                     |get_json_object(record_value, "$$._links.self.href") as location_url
                     |FROM $table
               |WHERE record_type = "tickets" AND feed_date = "$date"
               |""".stripMargin)
    }

    def transformOrderTenders(orderTenderTypes: DataFrame, cxiPartnerId: String): DataFrame = {
        val fieldLocation = 5

        orderTenderTypes
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn(
                "paymentsStruct",
                from_json(col("payments"), DataTypes.createArrayType(Encoders.product[Payment].schema))
            )
            .withColumn("PaymentsArray", explode(col("paymentsStruct")))
            .withColumn("tender_id", col("paymentsArray._embedded.tender_type.id"))
            .withColumn("location_id", split(col("location_url"), "/")(fieldLocation))
            .withColumn("tender_nm", col("paymentsArray._embedded.tender_type.name"))
            .withColumn("tender_type", normalizeOrderTenderType(PosOmnivoreToCxiTenderType)(col("tender_nm")))
            .drop("PaymentArray", "payments")
            .dropDuplicates("cxi_partner_id", "location_id", "tender_id")
            .select(
                "tender_id",
                "cxi_partner_id",
                "location_id",
                "tender_nm",
                "tender_type"
            )
    }

    def writeOrderTenders(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newOrderTenders"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id <=> "$cxiPartnerId" AND $destTable.location_id <=> $srcTable.location_id
               |AND $destTable.tender_id <=> $srcTable.tender_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }
}
