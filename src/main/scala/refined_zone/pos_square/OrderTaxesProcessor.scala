package com.cxi.cdp.data_processing
package refined_zone.pos_square

import raw_zone.pos_square.model.Tax
import refined_zone.pos_square.RawRefinedSquarePartnerJob.getSchemaRefinedPath
import refined_zone.pos_square.config.ProcessorConfig

import org.apache.spark.sql.functions.{col, explode, from_json, lit}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

object OrderTaxesProcessor {
    def process(spark: SparkSession,config: ProcessorConfig, destDbName: String): Unit = {

        val orderTaxTable = config.contract.prop[String](getSchemaRefinedPath("order_tax_table"))

        val orderTaxes = readOrderTaxes(spark, config.date, config.srcDbName, config.srcTable)

        val processedOrderTaxes = transformOrderTaxes(orderTaxes, config.cxiPartnerId)

        writeOrderTaxes(processedOrderTaxes, config.cxiPartnerId, s"$destDbName.$orderTaxTable")
    }

    def readOrderTaxes(spark: SparkSession, date: String, dbName: String, table: String): DataFrame = {
        spark.sql(
            s"""
               |SELECT
               |get_json_object(record_value, "$$.taxes") as taxes,
               |get_json_object(record_value, "$$.location_id") as location_id
               |FROM $dbName.$table
               |WHERE record_type = "orders" AND get_json_object(record_value, "$$.state")= "COMPLETED" AND feed_date="$date"
               |""".stripMargin)
    }

    def transformOrderTaxes(orderTaxes: DataFrame, cxiPartnerId: String): DataFrame = {
        orderTaxes
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("taxes", from_json(col("taxes"), DataTypes.createArrayType(Encoders.product[Tax].schema)))
            .withColumn("tax", explode(col("taxes")))
            .withColumn("tax_id", col("tax.uid"))
            .withColumn("tax_nm", col("tax.name"))
            .withColumn("tax_rate", col("tax.percentage"))
            .withColumn("tax_type", col("tax.type"))
            .drop("taxes", "tax")
            .dropDuplicates("cxi_partner_id", "location_id", "tax_id") // TODO: should we use tax_nm+tax_rate+tax_type as composite key & remove tax_id?
    }

    def writeOrderTaxes(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newOrderTaxes"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(
            s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id = "$cxiPartnerId" AND $destTable.location_id = $srcTable.location_id AND $destTable.tax_id = $srcTable.tax_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

}
