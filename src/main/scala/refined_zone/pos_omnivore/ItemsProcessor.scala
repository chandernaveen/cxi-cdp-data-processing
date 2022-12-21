package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore

import raw_zone.pos_omnivore.model.TicketItem
import refined_zone.pos_omnivore.config.ProcessorConfig
import refined_zone.pos_omnivore.RawRefinedOmnivorePartnerJob.getSchemaRefinedPath

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

object ItemsProcessor {

    def process(
        spark: SparkSession,
        config: ProcessorConfig,
        destDbName: String
    ): Unit = {

        val itemTable = config.contract.prop[String](getSchemaRefinedPath("item_table"))

        val items = readItems(spark, config.dateRaw, s"${config.srcDbName}.${config.srcTable}")

        val processedItem =
            transformItems(items, config.cxiPartnerId)

        writeItems(processedItem, config.cxiPartnerId, s"$destDbName.$itemTable")
    }

    def readItems(spark: SparkSession, date: String, table: String): DataFrame = {

        spark.sql(s"""
                     |SELECT
                     |get_json_object(record_value, "$$._embedded.items") as items
                     |FROM $table
                     |WHERE record_type="tickets" AND feed_date="$date"
                     |""".stripMargin)
    }

    def transformItems(Items: DataFrame, cxiPartnerId: String): DataFrame = {
        Items
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn(
                "itemsArray",
                from_json(col("items"), DataTypes.createArrayType(Encoders.product[TicketItem].schema))
            )
            .withColumn("item", explode(col("itemsArray")))
            .withColumn(
                "itemsEmbedded",
                col("item._embedded")
            )
            .withColumn(
                "modifiers",
                col("itemsEmbedded.modifiers.id")
            )
            .withColumn("variation_array", col("modifiers"))
            .withColumn(
                "item_type",
                when(col("variation_array").isNull || size(col("variation_array")) === 0, lit("FOOD")).otherwise(
                    lit("VARIATION")
                )
            )
            .withColumn("item_id", col("item.id"))
            .withColumn("item_nm", col("item.name"))
            .withColumn("category_array", col("itemsEmbedded.menu_item._embedded.menu_categories.name"))
            .withColumn("category_array_str", concat_ws(",", col("category_array")))
            .withColumn(
                "main_category_name",
                when(upper(col("category_array_str")).contains("FOOD"), lit("FOOD")).otherwise(
                    when(upper(col("category_array_str")).contains("DRINKS"), lit("DRINKS"))
                        .otherwise(array_min(col("category_array")))
                )
            )
            .withColumn("item_desc", lit(null))
            .withColumn("item_plu", lit(null))
            .withColumn("item_barcode", concat_ws(",", col("itemsEmbedded.menu_item.barcodes")))
            .dropDuplicates("cxi_partner_id", "item_id")
            .select(
                "cxi_partner_id",
                "item_id",
                "item_nm",
                "category_array",
                "item_desc",
                "item_type",
                "main_category_name",
                "item_plu",
                "item_barcode",
                "variation_array"
            )
    }

    def writeItems(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newItem"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
                             |MERGE INTO $destTable
                             |USING $srcTable
                             |ON $destTable.cxi_partner_id <=> "$cxiPartnerId"
                             |  AND $destTable.item_id <=> $srcTable.item_id
                             |WHEN MATCHED
                             |  THEN UPDATE SET *
                             |WHEN NOT MATCHED
                             |  THEN INSERT *
                             |""".stripMargin)
    }
}
