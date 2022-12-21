package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor

import refined_zone.pos_parbrink.config.ProcessorConfig
import refined_zone.pos_parbrink.model.ParbrinkRecordType
import refined_zone.pos_parbrink.RawRefinedParbrinkPartnerJob.getSchemaRefinedPath

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, get_json_object, lit, when}

object CategoriesProcessor {

    def process(spark: SparkSession, config: ProcessorConfig, refinedDbName: String): Unit = {

        val categoryTable = config.contract.prop[String](getSchemaRefinedPath("category_table"))

        val categories =
            readCategories(spark, config.dateRaw, config.refinedFullProcess, s"${config.srcDbName}.${config.srcTable}")

        val processedCategories = transformCategories(categories, config.cxiPartnerId)

        writeCategories(processedCategories, config.cxiPartnerId, s"$refinedDbName.$categoryTable")
    }

    def readCategories(spark: SparkSession, date: String, refinedFullProcess: String, table: String): DataFrame = {
        import spark.implicits._
        val fromRecordValue = path => get_json_object($"record_value", path)

        spark
            .table(table)
            .filter(
                $"record_type" === ParbrinkRecordType.Categories.value && $"feed_date" === when(
                    lit(refinedFullProcess).equalTo("true"),
                    $"feed_date"
                ).otherwise(date)
            )
            .filter(col("record_value").isNotNull)
            .filter(fromRecordValue("$.IsDeleted") === false)
            .select(
                col("location_id"),
                fromRecordValue("$.Id").as("cat_id"),
                fromRecordValue("$.Name").as("cat_nm")
            )
    }

    def transformCategories(categories: DataFrame, cxiPartnerId: String): DataFrame = {
        categories
            .dropDuplicates("cat_id", "location_id")
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("cat_desc", lit(null))
    }

    def writeCategories(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newCategories"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id <=> "$cxiPartnerId"
               |  AND $destTable.location_id <=> $srcTable.location_id
               |  AND $destTable.cat_id <=> $srcTable.cat_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

}
