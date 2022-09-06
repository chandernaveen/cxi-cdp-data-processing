package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor

import refined_zone.pos_parbrink.config.ProcessorConfig
import refined_zone.pos_parbrink.model.ParbrinkRecordType
import refined_zone.pos_parbrink.RawRefinedParbrinkPartnerJob.getSchemaRefinedPath
import support.normalization.udf.LocationNormalizationUdfs.normalizeZipCode

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, get_json_object, lit}

object HouseAccountsProcessor {

    def process(spark: SparkSession, config: ProcessorConfig, destDbName: String): Unit = {

        val houseAccountTable = config.contract.prop[String](getSchemaRefinedPath("house_account_table"))

        val houseAccounts = readHouseAccounts(spark, config.dateRaw, s"${config.srcDbName}.${config.srcTable}")

        val processedHouseAccounts = transformHouseAccounts(houseAccounts, config.cxiPartnerId)

        writeHouseAccounts(processedHouseAccounts, config.cxiPartnerId, s"$destDbName.$houseAccountTable")
    }

    def readHouseAccounts(spark: SparkSession, date: String, table: String): DataFrame = {
        import spark.implicits._
        val fromRecordValue = path => get_json_object($"record_value", path)

        spark
            .table(table)
            .filter($"record_type" === ParbrinkRecordType.HouseAccounts.value && $"feed_date" === date)
            .filter(col("record_value").isNotNull)
            .filter(fromRecordValue("$.Active") === true)
            .select(
                col("location_id"),
                fromRecordValue("$.Id").as("house_account_id"),
                fromRecordValue("$.AccountNumber").as("account_number"),
                fromRecordValue("$.Address1").as("address_1"),
                fromRecordValue("$.Address2").as("address_2"),
                fromRecordValue("$.Address3").as("address_3"),
                fromRecordValue("$.Address4").as("address_4"),
                fromRecordValue("$.Balance").as("balance"),
                fromRecordValue("$.City").as("city"),
                fromRecordValue("$.EmailAddress").as("email_address"), // already normalized and hashed
                fromRecordValue("$.EnforceLimit").as("is_enforced_limit"),
                fromRecordValue("$.FirstName").as("first_name"),
                fromRecordValue("$.LastName").as("last_name"),
                fromRecordValue("$.Limit").as("limit"),
                fromRecordValue("$.MiddleName").as("middle_name"),
                fromRecordValue("$.Name").as("name"),
                fromRecordValue("$.PhoneNumber").as("phone_number"), // already normalized and hashed
                fromRecordValue("$.State").as("state"),
                fromRecordValue("$.Zip").as("zip")
            )

    }

    def transformHouseAccounts(houseAccounts: DataFrame, cxiPartnerId: String): DataFrame = {
        houseAccounts
            .dropDuplicates("house_account_id", "location_id")
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("zip", normalizeZipCode(col("zip")))
    }

    def writeHouseAccounts(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newHouseAccounts"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id = "$cxiPartnerId"
               |    AND $destTable.location_id = $srcTable.location_id
               |    AND $destTable.house_account_id = $srcTable.house_account_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }
}
