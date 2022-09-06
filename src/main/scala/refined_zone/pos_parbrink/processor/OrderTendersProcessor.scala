package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor

import refined_zone.pos_parbrink.config.ProcessorConfig
import refined_zone.pos_parbrink.model.ParbrinkOrderTenderType.ParbrinkToCxiTenderType
import refined_zone.pos_parbrink.model.ParbrinkRecordType
import refined_zone.pos_parbrink.RawRefinedParbrinkPartnerJob.getSchemaRefinedPath
import support.normalization.udf.OrderTenderTypeNormalizationUdfs.normalizeIntOrderTenderType

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, get_json_object, lit}
import org.apache.spark.sql.types.BooleanType

object OrderTendersProcessor {

    def process(spark: SparkSession, config: ProcessorConfig, destDbName: String): DataFrame = {

        val orderTenderTable = config.contract.prop[String](getSchemaRefinedPath("order_tender_table"))

        val orderTenders = readOrderTenders(spark, config.dateRaw, s"${config.srcDbName}.${config.srcTable}")

        val processedOrderTenders = transformOrderTenders(orderTenders, config.cxiPartnerId)

        writeOrderTenders(processedOrderTenders, config.cxiPartnerId, s"$destDbName.$orderTenderTable")

        processedOrderTenders
    }

    def readOrderTenders(spark: SparkSession, date: String, table: String): DataFrame = {
        import spark.implicits._
        val fromRecordValue = path => get_json_object($"record_value", path)

        spark
            .table(table)
            .filter($"record_type" === ParbrinkRecordType.OrderTenders.value && $"feed_date" === date)
            .filter(col("record_value").isNotNull)
            .select(
                col("location_id"),
                fromRecordValue("$.Id").as("tender_id"),
                fromRecordValue("$.Name").as("tender_nm"),
                fromRecordValue("$.TenderType").as("tender_type")
            )
    }

    def transformOrderTenders(orderTenderTypes: DataFrame, cxiPartnerId: String): DataFrame = {
        orderTenderTypes
            .dropDuplicates("tender_id", "location_id")
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("tender_type", normalizeIntOrderTenderType(ParbrinkToCxiTenderType)(col("tender_type")))
    }

    def writeOrderTenders(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newOrderTenders"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id = "$cxiPartnerId"
               |    AND $destTable.location_id = $srcTable.location_id
               |    AND $destTable.tender_id = $srcTable.tender_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }
}
