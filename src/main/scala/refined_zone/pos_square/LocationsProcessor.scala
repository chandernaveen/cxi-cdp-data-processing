package com.cxi.cdp.data_processing
package refined_zone.pos_square

import refined_zone.pos_square.RawRefinedSquarePartnerJob.{getSchemaRefinedHubPath, getSchemaRefinedPath}
import support.packages.utils.ContractUtils

import org.apache.spark.sql.functions.{broadcast, col, lit, upper, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object LocationsProcessor {
    def process(spark: SparkSession,
                contract: ContractUtils,
                date: String,
                cxiPartnerId: String,
                srcDbName: String,
                srcTable: String,
                destDbName: String): Unit = {

        val locationTable = contract.prop[String](getSchemaRefinedPath("location_table"))
        val postalCodeDb = contract.prop[String](getSchemaRefinedHubPath("db_name"))
        val postalCodeTable = contract.prop[String](getSchemaRefinedHubPath("postal_code_table"))

        val locations = readLocations(spark, date, s"$srcDbName.$srcTable")
        val postalCodes = readPostalCodes(spark,s"$postalCodeDb.$postalCodeTable")

        val processedLocations = transformLocations(locations, broadcast(postalCodes), cxiPartnerId)

        writeLocation(processedLocations, cxiPartnerId, s"$destDbName.$locationTable")
    }

    def readLocations(spark: SparkSession, date: String, table: String): DataFrame = {
        spark.sql(
            s"""
               |SELECT
               |get_json_object(record_value, "$$.id") as location_id,
               |get_json_object(record_value, "$$.name") as location_nm,
               |get_json_object(record_value, "$$.type") as location_type,
               |get_json_object(record_value, "$$.status") as active_flg,
               |get_json_object(record_value, "$$.address.address_line_1") as address_1,
               |get_json_object(record_value, "$$.address.postal_code") as zip_code,
               |get_json_object(record_value, "$$.coordinates.latitude") as lat,
               |get_json_object(record_value, "$$.coordinates.longitude") as long,
               |get_json_object(record_value, "$$.phone_number") as phone,
               |get_json_object(record_value, "$$.address.country") as country_code,
               |get_json_object(record_value, "$$.timezone") as timezone,
               |get_json_object(record_value, "$$.currency") as currency,
               |get_json_object(record_value, "$$.created_at") as open_dt,
               |get_json_object(record_value, "$$.website_url") as location_website
               |FROM $table
               |WHERE record_type = "locations" AND feed_date = "$date"
               |""".stripMargin)
    }

    def readPostalCodes(spark: SparkSession, table: String): DataFrame = {
        spark.sql(
            s"""
               |SELECT
               |    postal_code AS zip_code,
               |    city,
               |    state_code,
               |    region
               |FROM $table
               |""".stripMargin)
    }

    def transformLocations(locations: DataFrame, postalCodes: DataFrame, cxiPartnerId: String): DataFrame = {
        locations
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("active_flg", when(col("active_flg") === "ACTIVE", 1).otherwise(0))
            .withColumn("location_type", when(upper(col("location_type")) === "PHYSICAL", 1).otherwise(when(upper(col("location_type")) === "MOBILE", 6).otherwise(0)))
            .withColumn("address_2", lit(null))
            .withColumn("fax", lit(null))
            .withColumn("parent_location_id", lit(null))
            .withColumn("extended_attr", lit(null))
            .dropDuplicates("cxi_partner_id", "location_id")
            .join(postalCodes, locations("zip_code") === postalCodes("zip_code"), "left") // adds city, state_code, region
            .drop(postalCodes("zip_code"))
    }

    def writeLocation(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newLocations"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(
            s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id = "$cxiPartnerId" AND $destTable.location_id = $srcTable.location_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }
}