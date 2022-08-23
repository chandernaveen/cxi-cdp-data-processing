package com.cxi.cdp.data_processing
package refined_zone.pos_toast

import refined_zone.pos_toast.config.ProcessorConfig
import refined_zone.pos_toast.RawRefinedToastPartnerJob.getSchemaRefinedPath

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode, lit}

object CategoriesProcessor {
    def process(spark: SparkSession, config: ProcessorConfig, destDbName: String): Unit = {

        val categoryTable = config.contract.prop[String](getSchemaRefinedPath("category_table"))

        val categories = readCategories(spark, config.dateRaw, s"${config.srcDbName}.${config.srcTable}")

        val processedCategories = transformCategories(categories, config.cxiPartnerId)

        writeCategories(processedCategories, config.cxiPartnerId, s"$destDbName.$categoryTable")
    }

    def readCategories(spark: SparkSession, date: String, table: String): DataFrame = {
        import spark.implicits._

        val rawJsonCategories = spark
            .table(table)
            .filter($"record_type" === "menus" && $"feed_date" === date)
            .select("record_value")
            .as[String]

        spark.read
            .json(rawJsonCategories)
            .select(col("restaurantGuid"), explode(col("menus")).as("menu"))
            .withColumn("menuGroup", explode(col("menu.menuGroups")))
            .withColumn("menuItem", explode(col("menuGroup.menuItems")))
            .withColumn("salesCategory", col("menuItem.salesCategory"))
            .select(
                col("salesCategory.guid").as("cat_id"),
                col("salesCategory.name").as("cat_nm"),
                col("restaurantGuid").as("location_id")
            )
    }

    def transformCategories(categories: DataFrame, cxiPartnerId: String): DataFrame = {
        categories
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("cat_desc", lit(null))
            .dropDuplicates("cxi_partner_id", "cat_id", "location_id")
    }

    def writeCategories(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newCategories"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id <=> "$cxiPartnerId" AND $destTable.location_id <=> $srcTable.location_id AND $destTable.cat_id <=> $srcTable.cat_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

}
