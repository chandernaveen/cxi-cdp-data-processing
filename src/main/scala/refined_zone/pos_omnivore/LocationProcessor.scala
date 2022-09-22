package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore

import refined_zone.hub.model.LocationType
import refined_zone.pos_omnivore.config.ProcessorConfig
import refined_zone.pos_omnivore.RawRefinedOmnivorePartnerJob.{getSchemaRefinedHubPath, getSchemaRefinedPath}
import support.normalization.udf.LocationNormalizationUdfs.normalizeZipCode
import support.normalization.udf.TimestampNormalizationUdfs.convertToTimestamp

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object LocationProcessor {
    def process(spark: SparkSession, config: ProcessorConfig, destDbName: String): Unit = {

        val locationTable = config.contract.prop[String](getSchemaRefinedPath("location_table"))
        val postalCodeDb = config.contract.prop[String](getSchemaRefinedHubPath("db_name"))
        val postalCodeTable = config.contract.prop[String](getSchemaRefinedHubPath("postal_code_table"))

        val locations = readLocations(spark, config.dateRaw, s"${config.srcDbName}.${config.srcTable}")
        val postalCodes = readPostalCodes(spark, s"$postalCodeDb.$postalCodeTable")

        val processedLocations = transformLocations(locations, broadcast(postalCodes), config.cxiPartnerId)

        writeLocation(processedLocations, config.cxiPartnerId, s"$destDbName.$locationTable")
    }

    def readLocations(spark: SparkSession, date: String, table: String): DataFrame = {
        import spark.implicits._

        spark
            .table(table)
            .filter($"record_type" === "locations" && $"feed_date" === date)
            .select(
                get_json_object($"record_value", "$.id").as("location_id"),
                get_json_object($"record_value", "$.display_name").as("location_nm"),
                get_json_object($"record_value", "$.name").as("location_nm1"),
                get_json_object($"record_value", "$.website").as("location_website"),
                get_json_object($"record_value", "$.type").as("location_type"),
                get_json_object($"record_value", "$.status").as("status"),
                get_json_object($"record_value", "$.address.street1").as("address_1"),
                get_json_object($"record_value", "$.address.street2").as("address_2"),
                get_json_object($"record_value", "$.address.zip").as("zip_code"),
                get_json_object($"record_value", "$.coordinates.latitude").as("lat"),
                get_json_object($"record_value", "$.coordinates.longitude").as("long"),
                get_json_object($"record_value", "$.phone").as("phone"),
                get_json_object($"record_value", "$.address.country").as("country_code"),
                get_json_object($"record_value", "$.timezone").as("timezone"),
                get_json_object($"record_value", "$.currency").as("currency"),
                get_json_object($"record_value", "$.created_at").as("open_dt")
            )
    }

    def readPostalCodes(spark: SparkSession, table: String): DataFrame = {
        spark.sql(s"""
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
            .withColumn("active_flg", when(upper(col("status")) === "ONLINE", "1").otherwise(0))
            .withColumn(
                "location_nm",
                coalesce(col("location_nm"), col("location_nm1"))
            )
            .withColumn("location_type", lit(LocationType.Restaurant.code.toString))
            .withColumn("timezone", when(col("timezone").isNull, lit("UTC")).otherwise(col("timezone")))
            .withColumn("currency", lit("USD"))
            .withColumn("fax", lit(null))
            .withColumn("parent_location_id", lit(null))
            .withColumn("extended_attr", lit(null))
            .withColumn("zip_code", normalizeZipCode(col("zip_code")))
            .withColumn("open_dt", convertToTimestamp(col("open_dt")))
            .dropDuplicates("cxi_partner_id", "location_id")
            .join(postalCodes, Seq("zip_code"), "left")
            .drop(postalCodes("zip_code"))
            .drop("location_nm1", "status")
    }

    def writeLocation(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newLocations"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
                             |MERGE INTO $destTable
                             |USING $srcTable
                             |ON $destTable.cxi_partner_id <=> "$cxiPartnerId" AND $destTable.location_id <=> $srcTable.location_id
                             |WHEN MATCHED
                             |  THEN UPDATE SET *
                             |WHEN NOT MATCHED
                             |  THEN INSERT *
                             |""".stripMargin)
    }
}
