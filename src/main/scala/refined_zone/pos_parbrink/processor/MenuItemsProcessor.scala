package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor

import refined_zone.pos_parbrink.config.ProcessorConfig
import refined_zone.pos_parbrink.model.{MenuItemType, ParbrinkRecordType}
import refined_zone.pos_parbrink.model.ParbrinkRawModels.{IncludedModifier, Item}
import refined_zone.pos_parbrink.RawRefinedParbrinkPartnerJob.getSchemaRefinedPath

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

object MenuItemsProcessor {

    def process(spark: SparkSession, config: ProcessorConfig, refinedDbName: String): Unit = {

        val menuItemsTable = config.contract.prop[String](getSchemaRefinedPath("item_table"))

        val categories = readCategories(spark, config.dateRaw, s"${config.srcDbName}.${config.srcTable}")
        val transformedCategories = transformCategories(categories)

        val menuItems = readMenuItems(spark, config.dateRaw, s"${config.srcDbName}.${config.srcTable}")
        val processedMenuItems = transformMenuItems(menuItems, transformedCategories, config.cxiPartnerId)

        writeMenuItems(processedMenuItems, config.cxiPartnerId, s"$refinedDbName.$menuItemsTable")
    }

    def readCategories(spark: SparkSession, date: String, table: String): DataFrame = {
        import spark.implicits._
        val fromRecordValue = path => get_json_object($"record_value", path)

        spark
            .table(table)
            .filter($"record_type" === ParbrinkRecordType.Categories.value && $"feed_date" === date)
            .filter(fromRecordValue("$.IsDeleted") === false)
            .select(
                col("location_id"),
                fromRecordValue("$.Id").as("cat_id"),
                fromRecordValue("$.Items").as("item_array")
            )
    }

    def transformCategories(categories: DataFrame): DataFrame = {
        categories
            .withColumn(
                "items",
                from_json(
                    col("item_array"),
                    DataTypes.createArrayType(Encoders.product[Item].schema)
                )
            )
            .select(
                col("location_id"),
                col("cat_id"),
                explode(col("items.ItemId")).as("item_id")
            )
            .groupBy("item_id", "location_id")
            .agg(collect_set("cat_id").as("category_array"))
    }

    def readMenuItems(spark: SparkSession, date: String, srcTable: String): DataFrame = {
        import spark.implicits._
        val fromRecordValue = path => get_json_object($"record_value", path)

        spark
            .table(srcTable)
            .filter($"record_type" === ParbrinkRecordType.MenuItems.value && $"feed_date" === date)
            .filter(col("record_value").isNotNull)
            .filter(fromRecordValue("$.IsDeleted") === false)
            .select(
                col("location_id"),
                fromRecordValue("$.Id").as("item_id"),
                fromRecordValue("$.Name").as("item_nm"),
                fromRecordValue("$.ItemType").as("item_type"),
                fromRecordValue("$.PLU").as("item_plu"),
                fromRecordValue("$.IncludedModifiers").as("included_modifiers")
            )
    }

    def transformMenuItems(menuItems: DataFrame, transformedCategories: DataFrame, cxiPartnerId: String): DataFrame = {
        menuItems
            .dropDuplicates("location_id", "item_id")
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("item_desc", lit(null))
            .withColumn("item_barcode", lit(null))
            .withColumn("item_type", menuItemTypeByCode(col("item_type")))
            .withColumn(
                "variations",
                from_json(
                    col("included_modifiers"),
                    DataTypes.createArrayType(Encoders.product[IncludedModifier].schema)
                )
            )
            .withColumn("variation_array", col("variations.ItemId"))
            .drop("variations", "included_modifiers")
            .join(transformedCategories, Seq("item_id", "location_id"), "left_outer")
            .withColumn("main_category_name", array_max(col("category_array")))
    }

    def writeMenuItems(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newMenuItems"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id = "$cxiPartnerId"
               |    AND $destTable.location_id = $srcTable.location_id
               |    AND $destTable.item_id = $srcTable.item_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)

    }

    def menuItemTypeByCode: UserDefinedFunction = {
        udf((code: Int) => MenuItemType.withValueOpt(code).map(_.name))
    }

}
