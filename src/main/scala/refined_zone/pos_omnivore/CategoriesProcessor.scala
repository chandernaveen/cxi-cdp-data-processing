package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore

import raw_zone.pos_omnivore.model.TicketItem
import refined_zone.pos_omnivore.config.ProcessorConfig
import refined_zone.pos_omnivore.RawRefinedOmnivorePartnerJob.getSchemaRefinedPath

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

object CategoriesProcessor {
    def process(spark: SparkSession, config: ProcessorConfig, destDbName: String): Unit = {

        val categoryTable = config.contract.prop[String](getSchemaRefinedPath("category_table"))

        val categories = readCategories(spark, config.dateRaw, s"${config.srcDbName}.${config.srcTable}")

        val processedCategories = transformCategories(categories, config.cxiPartnerId)

        writeCategories(processedCategories, config.cxiPartnerId, s"$destDbName.$categoryTable")
    }

    def readCategories(spark: SparkSession, date: String, table: String): DataFrame = {
        spark.sql(s"""
               |SELECT
               |get_json_object(record_value, "$$._embedded.items") as items,
               |get_json_object(record_value, "$$._links.self.href") as location_url
               |FROM $table
               |WHERE record_type = "tickets" AND feed_date = "$date"
               |""".stripMargin)
    }

    def transformCategories(categories: DataFrame, cxiPartnerId: String): DataFrame = {
        val fieldLocation: Int = 5
        categories
            .withColumn(
                "itemsStruct",
                from_json(col("items"), DataTypes.createArrayType(Encoders.product[TicketItem].schema))
            )
            .withColumn("item", explode(col("itemsStruct")))
            .withColumn("menu_categories", explode(col("item._embedded.menu_item._embedded.menu_categories")))
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("cat_desc", lit(null))
            .withColumn("cat_id", col("menu_categories.id"))
            .withColumn("cat_nm", col("menu_categories.name"))
            .withColumn("location_id", split(col("location_url"), "/")(fieldLocation))
            .drop("location_url", "items", "item", "itemsStruct", "menu_categories")
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
