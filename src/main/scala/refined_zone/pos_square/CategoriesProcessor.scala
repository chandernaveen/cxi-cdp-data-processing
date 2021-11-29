package com.cxi.cdp.data_processing
package refined_zone.pos_square

import refined_zone.pos_square.RawRefinedSquarePartnerJob.getSchemaRefinedPath
import support.packages.utils.ContractUtils

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

object CategoriesProcessor {
    def process(spark: SparkSession,
                contract: ContractUtils,
                date: String,
                cxiPartnerId: String,
                srcDbName: String,
                srcTable: String,
                destDbName: String): Unit = {

        val categoryTable = contract.prop[String](getSchemaRefinedPath("category_table"))

        val categories = readCategories(spark, date, s"$srcDbName.$srcTable")

        val processedCategories = transformCategories(categories, cxiPartnerId)

        writeCategories(processedCategories, cxiPartnerId, s"$destDbName.$categoryTable")
    }

    def readCategories(spark: SparkSession, date: String, table: String): DataFrame = {
        spark.sql(
            s"""
               |SELECT
               |get_json_object(record_value, "$$.id") as cat_id,
               |get_json_object(record_value, "$$.category_data.name") as cat_nm
               |FROM $table
               |WHERE record_type = "objects" AND get_json_object(record_value, "$$.type")="CATEGORY" AND feed_date = "$date"
               |""".stripMargin)
    }

    def transformCategories(categories: DataFrame, cxiPartnerId: String): DataFrame = {
        categories
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("cat_desc", lit(null))
            .withColumn("location_id", lit(null))
            .dropDuplicates("cxi_partner_id", "cat_id", "location_id")
    }

    def writeCategories(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newCategories"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(
            s"""
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
