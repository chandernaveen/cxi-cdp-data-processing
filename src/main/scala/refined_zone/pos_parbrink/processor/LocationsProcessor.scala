package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor

import refined_zone.hub.model.LocationType
import refined_zone.pos_parbrink.config.ProcessorConfig
import refined_zone.pos_parbrink.model.{ParbrinkRecordType, Timezone}
import refined_zone.pos_parbrink.RawRefinedParbrinkPartnerJob.{getSchemaRefinedHubPath, getSchemaRefinedPath}
import support.normalization.PhoneNumberNormalization

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object LocationsProcessor {

    val CurrencyDefault = "USD"
    val LocationTypeDefault = LocationType.Restaurant.code.toString
    val ActiveFlagDefault = "1" // isActive = true

    def process(spark: SparkSession, config: ProcessorConfig, refinedDbName: String): Unit = {

        val locationTable = config.contract.prop[String](getSchemaRefinedPath("location_table"))

        val refinedHubDb = config.contract.prop[String](getSchemaRefinedHubPath("db_name"))
        val postalCodeTable = config.contract.prop[String](getSchemaRefinedHubPath("postal_code_table"))

        val locations = readLocations(spark, config.dateRaw, s"${config.srcDbName}.${config.srcTable}")

        val processedLocations =
            transformLocations(spark, locations, s"$refinedHubDb.$postalCodeTable", config.cxiPartnerId)

        writeLocations(processedLocations, config.cxiPartnerId, s"$refinedDbName.$locationTable")
    }

    def readLocations(spark: SparkSession, date: String, table: String): DataFrame = {
        import spark.implicits._
        val fromRecordValue = path => get_json_object($"record_value", path)

        spark
            .table(table)
            .filter($"record_type" === ParbrinkRecordType.Locations.value && $"feed_date" === date)
            .filter(col("record_value").isNotNull)
            .select(
                col("location_id"),
                fromRecordValue("$.Name").as("location_nm"),
                fromRecordValue("$.Address1").as("address_1"),
                fromRecordValue("$.Address2").as("address_2"),
                fromRecordValue("$.City").as("city"),
                fromRecordValue("$.State").as("state_code"),
                fromRecordValue("$.Zip").as("zip_code"),
                fromRecordValue("$.Latitude").as("lat"),
                fromRecordValue("$.Longitude").as("long"),
                fromRecordValue("$.Phone").as("phone"),
                fromRecordValue("$.Fax").as("fax"),
                fromRecordValue("$.Country").as("country_code"),
                fromRecordValue("$.TimeZone").as("timezone")
            )
    }

    def transformLocations(
        spark: SparkSession,
        locations: DataFrame,
        postalCodesTableName: String,
        cxiPartnerId: String
    ): DataFrame = {

        val postalCodes = readPostalCodes(spark, postalCodesTableName)

        locations
            .dropDuplicates("location_id")
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("location_type", lit(LocationTypeDefault))
            .withColumn("location_website", lit(null))
            .withColumn("active_flg", lit(ActiveFlagDefault))
            .withColumn("phone", normalizePhoneNumber(col("phone")))
            .withColumn("parent_location_id", lit(null))
            .withColumn("currency", lit(CurrencyDefault))
            .withColumn("open_dt", lit(null))
            .withColumn("extended_attr", lit(null))
            .withColumn("timezone", timezoneByCode(col("timezone")))
            .join(postalCodes, locations("zip_code") === postalCodes("postal_code"), "left")
            .withColumn("state_code", coalesce(col("state_code"), col("postal_codes_state_code")))
            .withColumn("city", coalesce(col("city"), col("postal_codes_city")))
            .drop("postal_code", "postal_codes_state_code", "postal_codes_city")
    }

    def writeLocations(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
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

    def timezoneByCode: UserDefinedFunction = {
        udf((code: Int) => Timezone.withValueOpt(code).map(_.name))
    }

    def normalizePhoneNumber: UserDefinedFunction = {
        udf((value: String) => PhoneNumberNormalization.normalizePhoneNumber(value))
    }

    private def readPostalCodes(spark: SparkSession, table: String): DataFrame = {
        val postalCodes = spark
            .table(table)
            .select(
                col("postal_code"),
                col("region"),
                col("city").as("postal_codes_city"),
                col("state_code").as("postal_codes_state_code")
            )
        broadcast(postalCodes)
    }
}
