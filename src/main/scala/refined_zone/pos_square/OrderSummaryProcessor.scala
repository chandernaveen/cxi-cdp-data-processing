package com.cxi.cdp.data_processing
package refined_zone.pos_square

import raw_zone.pos_square.model.{Fulfillment, LineItem, OrderSource, Tender}
import refined_zone.pos_square.config.ProcessorConfig
import refined_zone.pos_square.model.PosSquareOrderStateTypes.PosSquareToCxiOrderStateType
import refined_zone.pos_square.normalization.OrderChannelTypeNormalization
import refined_zone.pos_square.RawRefinedSquarePartnerJob.{getSchemaRefinedPath, parsePosSquareDate}
import support.normalization.udf.MoneyNormalizationUdfs.convertCentsToMoney
import support.normalization.udf.OrderChannelTypeNormalizationUdfs
import support.normalization.udf.OrderStateNormalizationUdfs.normalizeOrderState

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

object OrderSummaryProcessor {
    def process(
        spark: SparkSession,
        config: ProcessorConfig,
        destDbName: String,
        cxiCustomerIdsByOrder: DataFrame
    ): Unit = {

        val orderSummaryTable = config.contract.prop[String](getSchemaRefinedPath("order_summary_table"))

        val orderSummary = readOrderSummary(spark, config.dateRaw, config.srcDbName, config.srcTable)

        val processedOrderSummary =
            transformOrderSummary(orderSummary, config.dateRaw, config.cxiPartnerId, cxiCustomerIdsByOrder)

        writeOrderSummary(processedOrderSummary, config.cxiPartnerId, s"$destDbName.$orderSummaryTable")
    }

    def readOrderSummary(spark: SparkSession, date: String, dbName: String, table: String): DataFrame = {
        spark.sql(s"""
               |SELECT
               |get_json_object(record_value, "$$.source") as ord_source,
               |get_json_object(record_value, "$$.id") as ord_id,
               |get_json_object(record_value, "$$.total_money.amount") as ord_total,
               |get_json_object(record_value, "$$.total_discount_money.amount") as discount_amount,
               |get_json_object(record_value, "$$.created_at") as created_at,
               |get_json_object(record_value, "$$.closed_at") as ord_date,
               |get_json_object(record_value, "$$.closed_at") as ord_timestamp,
               |get_json_object(record_value, "$$.location_id") as location_id,
               |get_json_object(record_value, "$$.state") as ord_state,
               |get_json_object(record_value, "$$.fulfillments") as fulfillments,
               |get_json_object(record_value, "$$.discounts.uid") as discount_id,
               |get_json_object(record_value, "$$.line_items") as line_items,
               |get_json_object(record_value, "$$.total_service_charge_money.amount") as service_charge_amount,
               |get_json_object(record_value, "$$.total_tax_money.amount") as total_taxes_amount,
               |get_json_object(record_value, "$$.total_tip_money.amount") as total_tip_amount,
               |get_json_object(record_value, "$$.customer_id") as customer_id,
               |get_json_object(record_value, "$$.tenders") as tender_array
               |FROM $dbName.$table
               |WHERE record_type="orders" AND feed_date="$date"
               |""".stripMargin)
    }

    // scalastyle:off method.length
    def transformOrderSummary(
        orderSummary: DataFrame,
        date: String,
        cxiPartnerId: String,
        cxiIdentityIdsByOrder: DataFrame
    ): DataFrame = {
        orderSummary
            .withColumn("ord_source", from_json(col("ord_source"), Encoders.product[OrderSource].schema))
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("ord_desc", lit(null))
            .withColumn("ord_total", convertCentsToMoney("ord_total"))
            .withColumn("ord_pay_total", col("ord_total"))
            .withColumn("discount_amount", convertCentsToMoney("discount_amount"))
            .withColumn("ord_type", lit(null))
            .withColumn(
                "fulfillments",
                from_json(col("fulfillments"), DataTypes.createArrayType(Encoders.product[Fulfillment].schema))
            )
            .withColumn(
                "ord_originate_channel_id",
                normalizeOrderOriginateChannelTypeUdf(col("ord_source"))
            )
            .withColumn(
                "ord_target_channel_id",
                normalizeOrderTargetChannelTypeUdf(col("ord_source"), col("fulfillments"))
            )
            .withColumn("line_id", lit(null))
            .withColumn("emp_id", lit(null))
            .withColumn("dsp_qty", lit(null))
            .withColumn("dsp_ttl", lit(null))
            .withColumn("reason_code_id", lit(null))
            .withColumn("service_charge_id", lit(null))
            .withColumn("guest_check_line_item_id", lit(null))
            .withColumn("service_charge", lit(null))
            .withColumn(
                "line_items",
                from_json(col("line_items"), DataTypes.createArrayType(Encoders.product[LineItem].schema))
            )
            .withColumn("line_item", explode(col("line_items")))
            .withColumn("item_id", col("line_item.catalog_object_id"))
            .withColumn("item_quantity", col("line_item.quantity"))
            .withColumn("item_price_id", lit(null)) // TODO: Could use help on item/price overall
            .withColumn("item_total", convertCentsToMoney("line_item.total_money.amount"))
            .withColumn("guest_check_line_item_id", col("line_item.uid"))
            .withColumn("taxes_id", col("line_item.applied_taxes.tax_uid"))
            .withColumn("taxes_amount", convertCentsToMoney("line_item.total_tax_money.amount"))
            .withColumn("service_charge_amount", convertCentsToMoney("service_charge_amount"))
            .withColumn("total_taxes_amount", convertCentsToMoney("total_taxes_amount"))
            .withColumn("total_tip_amount", convertCentsToMoney("total_tip_amount"))
            .withColumn(
                "ord_sub_total",
                col("ord_total") - (col("total_taxes_amount") + col("total_tip_amount") + col("service_charge_amount"))
            )
            .withColumn(
                "tender_array",
                from_json(col("tender_array"), DataTypes.createArrayType(Encoders.product[Tender].schema))
            )
            .withColumn("tender_ids", col("tender_array.id"))
            .withColumn("feed_date", parsePosSquareDate(lit(date)))
            .withColumn("ord_date", coalesce(col("ord_date"), col("created_at")))
            .withColumn("ord_timestamp", coalesce(col("ord_timestamp"), col("created_at")))
            .withColumn("ord_state_id", normalizeOrderState(PosSquareToCxiOrderStateType)(col("ord_state")))
            .select(
                "ord_id",
                "ord_desc",
                "ord_total",
                "ord_date",
                "ord_timestamp",
                "discount_amount",
                "cxi_partner_id",
                "location_id",
                "ord_state_id",
                "ord_type",
                "ord_originate_channel_id",
                "ord_target_channel_id",
                "item_quantity",
                "item_total",
                "emp_id",
                "discount_id",
                "dsp_qty",
                "dsp_ttl",
                "guest_check_line_item_id",
                "line_id",
                "taxes_id",
                "taxes_amount",
                "item_id",
                "item_price_id",
                "reason_code_id",
                "service_charge_id",
                "service_charge_amount",
                "total_taxes_amount",
                "total_tip_amount",
                "tender_ids",
                "ord_pay_total",
                "ord_sub_total",
                "feed_date"
            )
            .filter(col("ord_timestamp").isNotNull)
            .filter(col("ord_date").isNotNull)
            .join(
                cxiIdentityIdsByOrder,
                orderSummary("ord_id") === cxiIdentityIdsByOrder("ord_id"),
                "left"
            ) // adds cxi_identity_ids
            .drop(cxiIdentityIdsByOrder("ord_id"))
            .dropDuplicates("cxi_partner_id", "location_id", "ord_id", "ord_date", "item_id")
    }

    val normalizeOrderOriginateChannelTypeUdf = OrderChannelTypeNormalizationUdfs.normalizeOrderChannelType(
        OrderChannelTypeNormalization.normalizeOrderOriginateChannelType _
    )

    val normalizeOrderTargetChannelTypeUdf = OrderChannelTypeNormalizationUdfs.normalizeOrderChannelType(
        OrderChannelTypeNormalization.normalizeOrderTargetChannelType _
    )

    def writeOrderSummary(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newOrderSummary"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id <=> "$cxiPartnerId"
               |  AND $destTable.location_id <=> $srcTable.location_id
               |  AND $destTable.ord_id <=> $srcTable.ord_id
               |  AND $destTable.ord_date <=> $srcTable.ord_date
               |  AND $destTable.item_id <=> $srcTable.item_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }
}
