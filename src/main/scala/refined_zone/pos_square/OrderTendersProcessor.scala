package com.cxi.cdp.data_processing
package refined_zone.pos_square

import raw_zone.pos_square.model.Tender
import refined_zone.pos_square.config.ProcessorConfig
import refined_zone.pos_square.model.PosSquareOrderTenderTypes.PosSquareToCxiTenderType
import refined_zone.pos_square.RawRefinedSquarePartnerJob.getSchemaRefinedPath
import support.normalization.udf.OrderTenderTypeNormalizationUdfs.normalizeOrderTenderType

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, explode, from_json, lit}
import org.apache.spark.sql.types.DataTypes

object OrderTendersProcessor {
    def process(spark: SparkSession, config: ProcessorConfig, destDbName: String): Unit = {

        val orderTenderTable = config.contract.prop[String](getSchemaRefinedPath("order_tender_table"))

        val orderTenders = readOrderTenders(spark, config.dateRaw, config.srcDbName, config.srcTable)

        val processedOrderTenders = transformOrderTenders(orderTenders, config.cxiPartnerId)

        writeOrderTenders(processedOrderTenders, config.cxiPartnerId, s"$destDbName.$orderTenderTable")
    }

    def readOrderTenders(spark: SparkSession, date: String, dbName: String, table: String): DataFrame = {
        spark.sql(s"""
               |SELECT
               |get_json_object(record_value, "$$.tenders") as tenders,
               |get_json_object(record_value, "$$.location_id") as location_id
               |FROM $dbName.$table
               |WHERE record_type = "orders" AND feed_date = "$date"
               |""".stripMargin)
    }

    def transformOrderTenders(orderTenderTypes: DataFrame, cxiPartnerId: String): DataFrame = {
        orderTenderTypes
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn(
                "tenders",
                from_json(col("tenders"), DataTypes.createArrayType(Encoders.product[Tender].schema))
            )
            .withColumn("tender", explode(col("tenders")))
            .withColumn("tender_id", col("tender.id"))
            .withColumn("tender_nm", lit(null))
            .withColumn("tender_type", normalizeOrderTenderType(PosSquareToCxiTenderType)(col("tender.type")))
            .drop("tenders", "tender")
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
