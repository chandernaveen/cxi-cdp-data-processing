package com.cxi.cdp.data_processing
package refined_zone.pos_square

import raw_zone.pos_square.model.Variation
import refined_zone.pos_square.RawRefinedSquarePartnerJob.getSchemaRefinedPath
import refined_zone.pos_square.config.ProcessorConfig

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

object MenuItemsProcessor {
    def process(spark: SparkSession, config: ProcessorConfig, destDbName: String): Unit = {

        val menuItemTable = config.contract.prop[String](getSchemaRefinedPath("item_table"))

        val menuItems = readMenuItems(spark, config.date, config.srcDbName, config.srcTable)
        val itemsVariations = readMenuItemsVariations(spark, config.date, config.srcDbName, config.srcTable)

        val processedMenuItems = transformMenuItems(menuItems, itemsVariations, config.cxiPartnerId)

        writeMenuItems(processedMenuItems, config.cxiPartnerId, s"$destDbName.$menuItemTable")
    }

    def readMenuItems(spark: SparkSession, date: String, dbName: String, table: String): DataFrame = {
        spark.sql(
            s"""
               |SELECT
               |get_json_object(record_value, "$$.id") as item_id,
               |get_json_object(record_value, "$$.item_data.name") as item_nm,
               |get_json_object(record_value, "$$.item_data.description") as item_desc,
               |get_json_object(record_value, "$$.type") as item_type,
               |get_json_object(record_value, "$$.item_data.category_id") as category_array,
               |get_json_object(record_value, "$$.item_data.variations") as variations
               |FROM $dbName.$table
               |WHERE record_type = "objects" AND get_json_object(record_value, "$$.type")="ITEM" AND feed_date = "$date"
               |""".stripMargin)
    }

    def readMenuItemsVariations(spark: SparkSession, date: String, dbName: String, table: String): DataFrame = {
        spark.sql(
            s"""
               |SELECT
               |get_json_object(record_value, "$$.id") as item_id,
               |get_json_object(record_value, "$$.item_data.variations") as variations
               |FROM $dbName.$table
               |WHERE record_type = "objects" AND get_json_object(record_value, "$$.type")="ITEM" AND feed_date = "$date"
               |""".stripMargin)
    }

    def transformMenuItems(menuItems: DataFrame, itemsVariations: DataFrame, cxiPartnerId: String): DataFrame = {
        val transformedMenuItems = menuItems
            .withColumn("variations", from_json(col("variations"), DataTypes.createArrayType(Encoders.product[Variation].schema)))
            .withColumn("variation_array", col("variations.id"))
            .withColumn("item_type", lit("food"))
            .drop("variations")

        val transformedMenuItemsVariations = itemsVariations
            .withColumn("variations", from_json(col("variations"), DataTypes.createArrayType(Encoders.product[Variation].schema)))
            .withColumn("variation", explode(col("variations")))
            .withColumn("item_nm", col("variation.item_variation_data.name"))
            .withColumn("item_desc", lit(null))
            .withColumn("item_type", when(lower(col("variation.type")) === "item", "food")
                .otherwise(
                    when(lower(col("variation.type")) === "item_variation", "variation")
                        .otherwise("unknown")))
            .withColumn("category_array", lit(null))
            .withColumn("variation_array", lit(null))
            .drop("variations", "variation")

        val allItems = transformedMenuItems.unionAll(transformedMenuItemsVariations)
        allItems
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("main_category_name", lit(null)) //TODO: We have the Category DF above, we should join and populate this
            .withColumn("item_plu", lit(null))
            .withColumn("item_barcode", lit(null))
            .dropDuplicates("cxi_partner_id", "item_id", "item_type")
    }

    def writeMenuItems(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newMenuItems"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(
            s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id = "$cxiPartnerId" AND $destTable.item_id = $srcTable.item_id AND $destTable.item_type = $srcTable.item_type
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }
}
