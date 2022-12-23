package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor.identity

import refined_zone.hub.identity.model.IdentityType
import refined_zone.hub.model.CxiIdentity.{CxiIdentityId, Type, Weight}
import refined_zone.pos_parbrink.config.ProcessorConfig
import refined_zone.pos_parbrink.model.ParbrinkRecordType

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object CustomersIdentitiesProcessor {

    val ParbrinkCustomerIdentityDelimeter = "_"

    def process(spark: SparkSession, config: ProcessorConfig, customers: DataFrame): DataFrame = {
        val orders =
            readOrders(spark, config.dateRaw, config.refinedFullProcess, s"${config.srcDbName}.${config.srcTable}")
        computeIdentitiesFromCustomers(customers, orders, config.cxiPartnerId)
    }

    def computeIdentitiesFromCustomers(customers: DataFrame, orders: DataFrame, cxiPartnerId: String): DataFrame = {
        val customersWithOrders = customers
            .withColumn("phone_number", explode_outer(col("phone_numbers")))
            .join(orders.dropDuplicates(Seq("order_id", "location_id")), Seq("customer_id"), "left_outer")
            .withColumn(
                "parbrink_customer_id",
                concat(lit(cxiPartnerId), lit(ParbrinkCustomerIdentityDelimeter), col("customer_id"))
            )
            .cache()

        val emails = customersWithOrders
            .filter(col("email_address").isNotNull)
            .select(
                col("order_id"),
                col("location_id"),
                col("email_address").as(CxiIdentityId),
                lit(IdentityType.Email.code).as(Type),
                lit(3).as(Weight)
            )

        val phones = customersWithOrders
            .filter(col("phone_number").isNotNull)
            .select(
                col("order_id"),
                col("location_id"),
                col("phone_number").as(CxiIdentityId),
                lit(IdentityType.Phone.code).as(Type),
                lit(3).as(Weight)
            )

        val parbrinkCustomerIds = customersWithOrders
            .filter(col("parbrink_customer_id").isNotNull)
            .select(
                col("order_id"),
                col("location_id"),
                col("parbrink_customer_id").as(CxiIdentityId),
                lit(IdentityType.ParbrinkCustomerId.code).as(Type),
                lit(3).as(Weight)
            )

        emails
            .unionByName(phones)
            .unionByName(parbrinkCustomerIds)
            .dropDuplicates("order_id", "location_id", CxiIdentityId, Type)
    }

    def readOrders(spark: SparkSession, date: String, refinedFullProcess: String, table: String): DataFrame = {
        import spark.implicits._
        val fromRecordValue = path => get_json_object($"record_value", path)

        spark
            .table(table)
            .filter(
                $"record_type" === ParbrinkRecordType.Orders.value && $"feed_date" === when(
                    lit(refinedFullProcess).equalTo("true"),
                    $"feed_date"
                ).otherwise(date)
            )
            .filter(col("record_value").isNotNull)
            .select(
                col("location_id"),
                fromRecordValue("$.Id").as("order_id"),
                fromRecordValue("$.CustomerId").as("customer_id")
            )
    }

}
