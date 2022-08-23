package com.cxi.cdp.data_processing
package refined_zone.pos_toast

import refined_zone.hub.model.LocationType
import refined_zone.pos_toast.config.ProcessorConfig
import refined_zone.pos_toast.RawRefinedToastPartnerJob.{getSchemaRefinedHubPath, getSchemaRefinedPath}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object LocationsProcessor {
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

        val rawJsonLocations = spark
            .table(table)
            .filter($"record_type" === "restaurants" && $"feed_date" === date)
            .select("record_value")
            .as[String]

        spark.read
            .json(rawJsonLocations)
            .select(
                col("guid").as("location_id"),
                col("location.address1").as("address_1"),
                col("location.address2").as("address_2"),
                col("location.latitude").as("lat"),
                col("location.longitude").as("long"),
                col("location.phone").as("phone"),
                col("location.zipCode").as("zip_code"),
                col("urls.website").as("location_website"),
                col("general.timeZone").as("timezone"),
                col("location.country").as("country_code"),
                concat(col("general.locationName"), lit(","), col("general.name")).as("location_nm")
            )
    }

    def readPostalCodes(spark: SparkSession, table: String): DataFrame = {
        import spark.implicits._
        broadcast(
            spark
                .table(table)
                .select(
                    $"postal_code" as "zip_code",
                    $"city",
                    $"state_code",
                    $"region"
                )
        )
    }

    def transformLocations(locations: DataFrame, postalCodes: DataFrame, cxiPartnerId: String): DataFrame = {
        locations
            .withColumn("fax", lit(null))
            .withColumn("parent_location_id", lit(null))
            .withColumn("extended_attr", lit(null))
            .withColumn("currency", lit("USD"))
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("active_flg", lit("1")) //  // Q: active, with no conditions ?
            .withColumn("location_type", lit(LocationType.Restaurant.code.toString))
            .withColumn("open_dt", lit(null))
            .dropDuplicates("cxi_partner_id", "location_id")
            .join(postalCodes, Seq("zip_code"), "left") // adds city, state_code, region
            .drop(postalCodes("zip_code"))
    }

    def writeLocation(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newLocations"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
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
