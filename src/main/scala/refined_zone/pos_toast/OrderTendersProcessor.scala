package com.cxi.cdp.data_processing
package refined_zone.pos_toast

import refined_zone.pos_toast.config.ProcessorConfig
import refined_zone.pos_toast.model.PosToastOrderTenderTypes.PosToastToCxiTenderType
import refined_zone.pos_toast.RawRefinedToastPartnerJob.getSchemaRefinedPath
import support.normalization.udf.OrderTenderTypeNormalizationUdfs.normalizeOrderTenderType

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, explode, from_json, lit}

object OrderTendersProcessor {
    def process(spark: SparkSession, config: ProcessorConfig, destDbName: String): Unit = {

        val orderTenderTable = config.contract.prop[String](getSchemaRefinedPath("order_tender_table"))

        val orderTenders = readOrderTenders(spark, config.dateRaw, s"${config.srcDbName}.${config.srcTable}")

        val processedOrderTenders = transformOrderTenders(orderTenders, config.cxiPartnerId)

        writeOrderTenders(processedOrderTenders, config.cxiPartnerId, s"$destDbName.$orderTenderTable")
    }

    def readOrderTenders(spark: SparkSession, date: String, table: String): DataFrame = {
        import spark.implicits._

        val rawOrdersAsStrings = spark
            .table(table)
            .filter($"record_type" === "orders" && $"feed_date" === date)
            .select("record_value", "location_id")

        val ordersRecordTypeDf = spark.read.json(rawOrdersAsStrings.select("record_value").as[String](Encoders.STRING))

        rawOrdersAsStrings
            .withColumn("record_value", from_json(col("record_value"), ordersRecordTypeDf.schema))
            .select(
                $"location_id",
                explode($"record_value.checks").as("check")
            )
            .withColumn("payment", explode($"check.payments"))
            .select($"location_id", $"payment.guid".as("tender_id"), $"payment.type".as("payment_type"))
    }

    def transformOrderTenders(orderTenderTypes: DataFrame, cxiPartnerId: String): DataFrame = {
        orderTenderTypes
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("tender_nm", lit(null))
            .withColumn("tender_type", normalizeOrderTenderType(PosToastToCxiTenderType)(col("payment_type")))
            .drop("payment_type")
            .dropDuplicates(
                "cxi_partner_id",
                "location_id",
                "tender_id"
            ) // TODO: consider removing tender id, and using tender type as key instead ?
    }

    def writeOrderTenders(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newOrderTenders"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id = "$cxiPartnerId" AND $destTable.location_id = $srcTable.location_id AND $destTable.tender_id = $srcTable.tender_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }
}
