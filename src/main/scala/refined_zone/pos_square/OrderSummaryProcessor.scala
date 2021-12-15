package com.cxi.cdp.data_processing
package refined_zone.pos_square

import raw_zone.pos_square.model.{Fulfillment, LineItem, Tender}
import refined_zone.pos_square.RawRefinedSquarePartnerJob.getSchemaRefinedPath
import support.packages.utils.ContractUtils

import org.apache.spark.sql.functions.{col, explode, from_json, lit}
import org.apache.spark.sql.types.{DataTypes, DoubleType, StringType}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

object OrderSummaryProcessor {
    def process(spark: SparkSession,
                contract: ContractUtils,
                date: String,
                cxiPartnerId: String,
                srcDbName: String,
                srcTable: String,
                destDbName: String,
                cxiCustomerIdsByOrder: DataFrame): Unit = {

        val orderSummaryTable = contract.prop[String](getSchemaRefinedPath("order_summary_table"))

        val orderSummary = readOrderSummary(spark, date, srcDbName, srcTable)

        val processedOrderSummary = transformOrderSummary(orderSummary, date, cxiPartnerId, cxiCustomerIdsByOrder)

        writeOrderSummary(processedOrderSummary, cxiPartnerId, s"$destDbName.$orderSummaryTable")
    }

    def readOrderSummary(spark: SparkSession, date: String, dbName: String, table: String): DataFrame = {
        spark.sql(
            s"""
               |SELECT
               |get_json_object(record_value, "$$.id") as ord_id,
               |get_json_object(record_value, "$$.total_money.amount") as ord_total,
               |get_json_object(record_value, "$$.total_discount_money.amount") as discount_amount,
               |get_json_object(record_value, "$$.closed_at") as ord_date,
               |get_json_object(record_value, "$$.closed_at") as ord_timestamp,
               |get_json_object(record_value, "$$.location_id") as location_id,
               |get_json_object(record_value, "$$.state") as ord_state,
               |get_json_object(record_value, "$$.fulfillments") as fulfillments,
               |get_json_object(record_value, "$$.discounts.uid") as discount_id,
               |get_json_object(record_value, "$$.line_items") as line_items,
               |get_json_object(record_value, "$$.total_service_charge_money.amount") as service_charge_amount,
               |get_json_object(record_value, "$$.customer_id") as customer_id,
               |get_json_object(record_value, "$$.tenders") as tender_array
               |FROM $dbName.$table
               |WHERE record_type="orders" AND get_json_object(record_value, "$$.state")="COMPLETED" AND feed_date="$date"
               |""".stripMargin)
    }

    def transformOrderSummary(orderSummary: DataFrame, date: String, cxiPartnerId: String, cxiCustomerIdsByOrder: DataFrame): DataFrame = {
        orderSummary
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("ord_desc", lit(null))
            .withColumn("ord_total", col("ord_total").cast(DoubleType) / 100)
            .withColumn("ord_pay_total", col("ord_total"))
            .withColumn("discount_amount", col("discount_amount").cast(DoubleType) / 100)
            .withColumn("ord_type", lit(null))
            .withColumn("fulfillments", from_json(col("fulfillments"), DataTypes.createArrayType(Encoders.product[Fulfillment].schema)))
            .withColumn("ord_target_channel_id", col("fulfillments.type").cast(StringType))
            .withColumn("ord_originate_channel_id", lit(null))
            .withColumn("line_id", lit(null))
            .withColumn("emp_id", lit(null))
            .withColumn("dsp_qty", lit(null))
            .withColumn("dsp_ttl", lit(null))
            .withColumn("reason_code_id", lit(null))
            .withColumn("service_charge_id", lit(null))
            .withColumn("guest_check_line_item_id", lit(null))
            .withColumn("taxes_amount", lit(null))
            .withColumn("ord_originate_channel_id", lit(null))
            .withColumn("service_charge", lit(null))
            .withColumn("line_items", from_json(col("line_items"), DataTypes.createArrayType(Encoders.product[LineItem].schema)))
            .withColumn("line_item", explode(col("line_items")))
            .withColumn("item_id", col("line_item.catalog_object_id"))
            .withColumn("item_quantity", col("line_item.quantity"))
            .withColumn("item_price_id", lit(null)) // TODO: Could use help on item/price overall
            .withColumn("item_total", col("line_item.total_money.amount").cast(DoubleType) / 100)
            .withColumn("guest_check_line_item_id", col("line_item.uid"))
            .withColumn("taxes_id", col("line_item.applied_taxes.tax_uid"))
            .withColumn("taxes_amount", col("line_item.total_tax_money.amount").cast(DoubleType) / 100)
            .withColumn("service_charge_amount", col("service_charge_amount").cast(DoubleType) / 100)
            .withColumn("ord_sub_total", col("ord_total") - col("taxes_amount") - col("discount_amount") - col("service_charge_amount"))
            .withColumn("tender_array", from_json(col("tender_array"), DataTypes.createArrayType(Encoders.product[Tender].schema)))
            .withColumn("tender", explode(col("tender_array")))
            .withColumn("tender_id", col("tender.id"))
            .withColumn("feed_date", lit(date))
            .select(
                "ord_id", "ord_desc", "ord_total", "ord_date", "ord_timestamp", "discount_amount",
                "cxi_partner_id", "location_id", "ord_state", "ord_type", "ord_originate_channel_id",
                "ord_target_channel_id", "item_quantity", "item_total", "emp_id", "discount_id", "dsp_qty",
                "dsp_ttl", "guest_check_line_item_id", "line_id", "taxes_id", "taxes_amount", "item_id",
                "item_price_id", "reason_code_id", "service_charge_id", "service_charge_amount", "tender_id",
                "ord_pay_total", "ord_sub_total", "feed_date")
            .join(cxiCustomerIdsByOrder, orderSummary("ord_id") === cxiCustomerIdsByOrder("ord_id"), "left") // adds cxi_customer_id_array
            .drop(cxiCustomerIdsByOrder("ord_id"))
            .dropDuplicates("cxi_partner_id", "location_id", "ord_id", "ord_date")
    }

    def writeOrderSummary(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newOrderSummary"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(
            s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id = "$cxiPartnerId" AND $destTable.location_id = $srcTable.location_id AND $destTable.ord_id = $srcTable.ord_id AND $destTable.ord_date = $srcTable.ord_date
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }
}