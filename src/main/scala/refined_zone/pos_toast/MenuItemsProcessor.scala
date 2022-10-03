package com.cxi.cdp.data_processing
package refined_zone.pos_toast

import refined_zone.pos_square.RawRefinedSquarePartnerJob.getSchemaRefinedPath
import refined_zone.pos_toast.config.ProcessorConfig

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

object MenuItemsProcessor {
    def process(
        spark: SparkSession,
        config: ProcessorConfig,
        destDbName: String
    ): Unit = {
        val menuItemTable = config.contract.prop[String](getSchemaRefinedPath("item_table"))

        val menuItems = readMenuItems(spark, config.dateRaw, s"${config.srcDbName}.${config.srcTable}")
        val menuGroups = readMenuGroups(spark, config.dateRaw, s"${config.srcDbName}.${config.srcTable}")
        val menuOptionGroups = readMenuOptionGroups(spark, config.dateRaw, s"${config.srcDbName}.${config.srcTable}")
        val processedMenuItems = transformMenuItems(menuItems, menuGroups, menuOptionGroups, config.cxiPartnerId)
        writeMenuItems(processedMenuItems, config.cxiPartnerId, s"$destDbName.$menuItemTable")
    }

    def readMenuItems(spark: SparkSession, date: String, table: String): DataFrame = {
        import spark.implicits._

        val rawMenuItems = spark
            .table(table)
            .filter($"record_type" === "menu-items" && $"feed_date" === date)
            .select("record_value")
            .as[String]

        spark.read
            .json(rawMenuItems.select("record_value").as[String](Encoders.STRING))
            .select(
                $"guid".as("item_id"),
                $"name".as("item_nm"),
                $"plu".as("item_plu"),
                $"optionGroups.guid".as("variation_array")
            )
    }

    def readMenuGroups(spark: SparkSession, date: String, table: String): DataFrame = {
        import spark.implicits._

        val guidSchema = new StructType()
            .add("guid", StringType)

        val menuItemSchema = new StructType()
            .add("name", StringType)
            .add("items", ArrayType(guidSchema))
            .add("optionGroups", ArrayType(guidSchema))
            .add("subgroups", ArrayType(guidSchema))

        val rawMenuItems = spark
            .table(table)
            .filter($"record_type" === "menu-groups" && $"feed_date" === date)
            .select("record_value")
            .as[String]

        spark.read
            .schema(menuItemSchema)
            .json(rawMenuItems.select("record_value").as[String](Encoders.STRING))
            .select(
                $"optionGroups.guid".as("variation_array"),
                $"subgroups.guid".as("category_array"),
                $"items.guid".as("items_ids"),
                $"name".as("main_category_name")
            )
    }

    def readMenuOptionGroups(spark: SparkSession, date: String, table: String): DataFrame = {
        import spark.implicits._

        val rawMenuOptionGroups = spark
            .table(table)
            .filter($"record_type" === "menu-option-groups" && $"feed_date" === date)
            .select("record_value")
            .as[String]

        spark.read
            .json(rawMenuOptionGroups.select("record_value").as[String](Encoders.STRING))
            .select(
                $"guid".as("item_id"),
                $"name".as("item_nm")
            )
    }

    def transformMenuItems(
        menuItems: DataFrame,
        menuGroups: DataFrame,
        menuOptionGroups: DataFrame,
        cxiPartnerId: String
    ): DataFrame = {
        import menuItems.sparkSession.implicits._

        val transformedMenuGroups = menuGroups
            .select(
                explode(col("items_ids")).as("item_id"),
                col("variation_array"),
                col("category_array"),
                col("main_category_name")
            )
            .as("menu_groups")

        val transformedMenuItems = menuItems
            .withColumn("item_type", lit("food"))

        val transformedMenuItemsVariations = menuOptionGroups
            .withColumn("item_type", lit("variation"))
            .withColumn("item_plu", lit(null))
            .withColumn("variation_array", lit(null))

        val allItems = transformedMenuItems.unionByName(transformedMenuItemsVariations)

        allItems
            .as("all_menu_items")
            .join(transformedMenuGroups, $"all_menu_items.item_id" === $"menu_groups.item_id", "left")
            .select(
                $"all_menu_items.item_id".as("item_id"),
                $"all_menu_items.item_nm".as("item_nm"),
                lit(null).as("item_desc"),
                $"all_menu_items.item_type".as("item_type"),
                $"menu_groups.category_array".as("category_array"),
                $"menu_groups.main_category_name".as("main_category_name"),
                coalesce($"all_menu_items.variation_array", $"menu_groups.variation_array").as("variation_array"),
                $"all_menu_items.item_plu".as("item_plu"),
                lit(null).as("item_barcode")
            )
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .dropDuplicates("cxi_partner_id", "item_id", "item_type")
    }

    def writeMenuItems(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newMenuItems"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
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
