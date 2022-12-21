package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor

import refined_zone.hub.model.CxiIdentity.CxiIdentityIds
import refined_zone.pos_parbrink.config.ProcessorConfig
import refined_zone.pos_parbrink.model.ParbrinkRawModels.{OrderEntry, Payment}
import refined_zone.pos_parbrink.model.ParbrinkRecordType
import refined_zone.pos_parbrink.normalization.OrderChannelTypeNormalization.{
    normalizeOriginateOrderChannelType,
    normalizeTargetOrderChannelType
}
import refined_zone.pos_parbrink.normalization.OrderStateNormalization.normalizeOrderState
import refined_zone.pos_parbrink.RawRefinedParbrinkPartnerJob.getSchemaRefinedPath
import support.crypto_shredding.config.CryptoShreddingConfig
import support.crypto_shredding.PrivacyFunctions.inAuthorizedContext
import support.normalization.udf.DateNormalizationUdfs.{parseToSqlDateIsoFormat, parseToSqlDateWithPattern}
import support.normalization.udf.TimestampNormalizationUdfs.parseToTimestampIsoDateTime
import support.WorkspaceConfigReader.readWorkspaceConfig

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StringType}

object OrderSummaryProcessor {

    // the time component will always return as 00:00:00
    private val OrderDatePattern = "yyyy-MM-dd'T'HH:mm:ss"

    def process(
        spark: SparkSession,
        identitiesByOrder: DataFrame,
        config: ProcessorConfig,
        cryptoShreddingConfig: CryptoShreddingConfig,
        refinedDbName: String
    ): Unit = {

        val orderSummaryTable = config.contract.prop[String](getSchemaRefinedPath("order_summary_table"))

        val orders =
            readOrders(spark, config.dateRaw, config.refinedFullProcess, s"${config.srcDbName}.${config.srcTable}")

        val destinations = readDestinations(
            spark,
            config.dateRaw,
            config.refinedFullProcess,
            s"${config.srcDbName}.${config.srcTable}"
        )

        val processedOrders =
            transformOrders(orders, destinations, identitiesByOrder, config.cxiPartnerId, config.dateRaw)

        inAuthorizedContext(spark, readWorkspaceConfig(spark, cryptoShreddingConfig.workspaceConfigPath)) {
            writeOrders(processedOrders, config.cxiPartnerId, s"$refinedDbName.$orderSummaryTable")
        }
    }

    def readDestinations(spark: SparkSession, date: String, refinedFullProcess: String, table: String): DataFrame = {
        import spark.implicits._
        val fromRecordValue = path => get_json_object($"record_value", path)

        spark
            .table(table)
            .filter(
                $"record_type" === ParbrinkRecordType.Destinations.value && $"feed_date" === when(
                    lit(refinedFullProcess).equalTo("true"),
                    $"feed_date"
                ).otherwise(date)
            )
            .filter(col("record_value").isNotNull)
            .select(
                col("location_id"),
                fromRecordValue("$.Id").as("destination_id"),
                fromRecordValue("$.Name").as("destination_name")
            )
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
                fromRecordValue("$.Id").as("ord_id"),
                fromRecordValue("$.Name").as("ord_desc"),
                fromRecordValue("$.BusinessDate").as("ord_date"),
                fromRecordValue("$.ModifiedTime").as("ord_timestamp"),
                fromRecordValue("$.IsClosed").as("is_closed"),
                fromRecordValue("$.DestinationId").as("destination_id"),
                fromRecordValue("$.Entries").as("entries"),
                fromRecordValue("$.EmployeeId").as("emp_id"),
                fromRecordValue("$.Tax").as("total_taxes_amount"),
                fromRecordValue("$.Payments").as("payments"),
                fromRecordValue("$.Subtotal").as("ord_sub_total"),
                fromRecordValue("$.Total").as("ord_total")
            )
    }

    // scalastyle:off method.length
    def transformOrders(
        orders: DataFrame,
        destinations: DataFrame,
        identitiesByOrder: DataFrame,
        cxiPartnerId: String,
        date: String
    ): DataFrame = {
        orders
            .dropDuplicates("ord_id")
            .join(
                destinations.dropDuplicates("location_id", "destination_id"),
                Seq("location_id", "destination_id"),
                "left"
            )
            .withColumn("ord_date", parseToSqlDateWithPattern(col("ord_date"), lit(OrderDatePattern)))
            .withColumn("ord_timestamp", parseToTimestampIsoDateTime(col("ord_timestamp")))
            .withColumn(
                "payments",
                from_json(
                    col("payments"),
                    DataTypes.createArrayType(Encoders.product[Payment].schema)
                )
            )
            .withColumn(
                "entries",
                from_json(
                    col("entries"),
                    DataTypes.createArrayType(Encoders.product[OrderEntry].schema)
                )
            )
            .withColumn("total_tip_amount", aggregate(col("payments.TipAmount"), lit(0.0), (acc, x) => acc + x))
            .withColumn("tender_ids", col("payments.TenderId").cast("array<string>"))
            .withColumn("ord_state_id", normalizeOrderState(col("is_closed")))
            .withColumn("item_quantity", lit(null))
            .withColumn("ord_pay_total", col("ord_total"))
            .withColumn("ord_type", lit(null))
            .withColumn("discount_id", lit(null))
            .withColumn("discount_amount", lit(null))
            .withColumn("dsp_qty", lit(null))
            .withColumn("dsp_ttl", lit(null))
            .withColumn("line_id", lit(null))
            .withColumn("item_price_id", lit(null))
            .withColumn("reason_code_id", lit(null))
            .withColumn("service_charge_id", lit(null))
            .withColumn("service_charge_amount", lit(null))
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("item", explode(col("entries")))
            .withColumn("item_id", col("item.ItemId"))
            .withColumn("guest_check_line_item_id", col("item.Id"))
            .withColumn("item_total", col("item.GrossSales"))
            .withColumn("taxes_id", col("item.Taxes.TaxId").cast(StringType))
            .withColumn("taxes_amount", aggregate(col("item.Taxes.Amount"), lit(0.0), (acc, x) => acc + x))
            .withColumn("ord_originate_channel_id", normalizeOriginateOrderChannelType(col("destination_name")))
            .withColumn("ord_target_channel_id", normalizeTargetOrderChannelType(col("destination_name")))
            .withColumn("feed_date", parseToSqlDateIsoFormat(lit(date)))
            .join(identitiesByOrder.withColumnRenamed("order_id", "ord_id"), Seq("ord_id", "location_id"), "left")
            .select(
                "ord_id",
                "ord_desc",
                "ord_total",
                "ord_date",
                "ord_timestamp",
                "cxi_partner_id",
                "location_id",
                "ord_state_id",
                "ord_type",
                "emp_id",
                "total_taxes_amount",
                "total_tip_amount",
                "tender_ids",
                "item_quantity",
                "item_total",
                "discount_id",
                "discount_amount",
                "dsp_qty",
                "dsp_ttl",
                "guest_check_line_item_id",
                "line_id",
                "item_price_id",
                "reason_code_id",
                "service_charge_id",
                "service_charge_amount",
                "ord_sub_total",
                "ord_pay_total",
                "item_id",
                "taxes_id",
                "taxes_amount",
                "ord_originate_channel_id",
                "ord_target_channel_id",
                CxiIdentityIds,
                "feed_date"
            )
            .filter(col("ord_timestamp").isNotNull)
            .filter(col("ord_date").isNotNull)
            .dropDuplicates("ord_id", "location_id", "ord_date", "item_id", "guest_check_line_item_id")

    }

    def writeOrders(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
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
               |  AND $destTable.guest_check_line_item_id <=> $srcTable.guest_check_line_item_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

}
