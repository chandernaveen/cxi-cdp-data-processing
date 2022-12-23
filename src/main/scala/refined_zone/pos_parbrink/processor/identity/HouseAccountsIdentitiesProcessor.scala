package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor.identity

import refined_zone.hub.identity.model.IdentityType
import refined_zone.hub.model.CxiIdentity.{CxiIdentityId, Type, Weight}
import refined_zone.pos_parbrink.config.ProcessorConfig
import refined_zone.pos_parbrink.model.ParbrinkRecordType

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, get_json_object, lit, when}

object HouseAccountsIdentitiesProcessor {

    def process(spark: SparkSession, config: ProcessorConfig): DataFrame = {
        val houseAccountCharges =
            readHouseAccountCharges(
                spark,
                config.dateRaw,
                config.refinedFullProcess,
                s"${config.srcDbName}.${config.srcTable}"
            )
        val houseAccounts =
            readHouseAccounts(
                spark,
                config.dateRaw,
                config.refinedFullProcess,
                s"${config.srcDbName}.${config.srcTable}"
            )
        val houseAccountsWithOrder = transformHouseAccounts(houseAccounts, houseAccountCharges)

        computeIdentitiesFromHouseAccounts(houseAccountsWithOrder)
    }

    def computeIdentitiesFromHouseAccounts(houseAccountsWithOrder: DataFrame): DataFrame = {
        val emails = houseAccountsWithOrder
            .filter(col("email_address").isNotNull)
            .select(
                col("order_id"),
                col("location_id"),
                col("email_address").as(CxiIdentityId),
                lit(IdentityType.Email.code).as(Type),
                lit(3).as(Weight)
            )

        val phones = houseAccountsWithOrder
            .filter(col("phone_number").isNotNull)
            .select(
                col("order_id"),
                col("location_id"),
                col("phone_number").as(CxiIdentityId),
                lit(IdentityType.Phone.code).as(Type),
                lit(3).as(Weight)
            )
        emails.unionByName(phones)
    }

    def readHouseAccountCharges(
        spark: SparkSession,
        date: String,
        refinedFullProcess: String,
        table: String
    ): DataFrame = {
        import spark.implicits._
        val fromRecordValue = path => get_json_object($"record_value", path)

        spark
            .table(table)
            .filter(
                $"record_type" === ParbrinkRecordType.HouseAccountCharges.value && $"feed_date" === when(
                    lit(refinedFullProcess).equalTo("true"),
                    $"feed_date"
                ).otherwise(date)
            )
            .filter(col("record_value").isNotNull)
            .select(
                col("location_id"),
                fromRecordValue("$.AccountId").as("house_account_id"),
                fromRecordValue("$.OrderId").as("order_id")
            )
    }

    def readHouseAccounts(spark: SparkSession, date: String, refinedFullProcess: String, table: String): DataFrame = {
        import spark.implicits._
        val fromRecordValue = path => get_json_object($"record_value", path)

        spark
            .table(table)
            .filter(
                $"record_type" === ParbrinkRecordType.HouseAccounts.value && $"feed_date" === when(
                    lit(refinedFullProcess).equalTo("true"),
                    $"feed_date"
                ).otherwise(date)
            )
            .filter(fromRecordValue("$.Active") === true)
            .select(
                col("location_id"),
                fromRecordValue("$.Id").as("house_account_id"),
                fromRecordValue("$.EmailAddress").as("email_address"), // already normalized and hashed
                fromRecordValue("$.PhoneNumber").as("phone_number") // already normalized and hashed
            )
    }

    def transformHouseAccounts(houseAccounts: DataFrame, houseAccountCharges: DataFrame): DataFrame = {
        val houseAccountsWithOrder =
            houseAccounts
                .dropDuplicates("house_account_id", "location_id")
                .join(
                    houseAccountCharges.dropDuplicates("location_id", "house_account_id"),
                    Seq("house_account_id", "location_id"),
                    "left"
                )
                .select("location_id", "email_address", "phone_number", "order_id")

        houseAccountsWithOrder
    }

}
