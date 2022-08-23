package com.cxi.cdp.data_processing
package refined_zone.pos_toast

import refined_zone.hub.model.OrderChannelType
import refined_zone.pos_toast.config.ProcessorConfig
import refined_zone.pos_toast.model.PosToastOrderChannelTypes.{
    PosToastToCxiOriginateChannelType,
    PosToastToCxiTargetChannelType
}
import refined_zone.pos_toast.model.PosToastOrderStateTypes.PosToastToCxiOrderStateType
import refined_zone.pos_toast.RawRefinedToastPartnerJob.getSchemaRefinedPath
import support.crypto_shredding.PrivacyFunctions.inAuthorizedContext
import support.normalization.udf.DateNormalizationUdfs.{parseToSqlDate, parseToSqlDateIsoFormat}
import support.normalization.udf.OrderChannelTypeNormalizationUdfs
import support.normalization.udf.OrderStateNormalizationUdfs.normalizeOrderState
import support.normalization.udf.TimestampNormalizationUdfs.parseToTimestamp
import support.WorkspaceConfigReader

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object OrderSummaryProcessor {
    def process(
        spark: SparkSession,
        config: ProcessorConfig,
        destDbName: String,
        posIdentitiesByOrderIdAndLocationId: DataFrame
    ): Unit = {

        val orderSummaryTable = config.contract.prop[String](getSchemaRefinedPath("order_summary_table"))

        val orderSummary = readOrderSummary(spark, config.dateRaw, s"${config.srcDbName}.${config.srcTable}")
        val diningOptions = readDiningOptions(spark, config.dateRaw, s"${config.srcDbName}.${config.srcTable}")

        val processedOrderSummary =
            transformOrderSummary(
                orderSummary,
                config.dateRaw,
                config.cxiPartnerId,
                posIdentitiesByOrderIdAndLocationId,
                diningOptions
            )

        val workspaceConfigPath: String = config.contract.prop[String]("databricks_workspace_config")
        val workspaceConfig = WorkspaceConfigReader.readWorkspaceConfig(spark, workspaceConfigPath)
        inAuthorizedContext(spark, workspaceConfig) {
            writeOrderSummary(processedOrderSummary, config.cxiPartnerId, s"$destDbName.$orderSummaryTable")
        }
    }

    def readOrderSummary(spark: SparkSession, date: String, table: String): DataFrame = {
        import spark.implicits._

        val rawOrdersAsStrings = spark
            .table(table)
            .filter($"record_type" === "orders" && $"feed_date" === date)
            .select("record_value", "location_id")

        val ordersRecordTypeDf = spark.read.json(rawOrdersAsStrings.select("record_value").as[String](Encoders.STRING))

        val rawChecksAsJsonStructs = rawOrdersAsStrings
            .withColumn("record_value", from_json(col("record_value"), ordersRecordTypeDf.schema))
            .select(
                $"record_value.guid".as("ord_id"),
                $"record_value.closedDate".as("ord_timestamp"),
                $"record_value.approvalStatus".as("ord_state"),
                $"record_value.diningOption".as("diningOption"),
                $"location_id",
                explode($"record_value.checks").as("check")
            )
        rawChecksAsJsonStructs
    }

    def readDiningOptions(spark: SparkSession, date: String, table: String): DataFrame = {
        import spark.implicits._

        val rawDiningOptions = spark
            .table(table)
            .filter($"record_type" === "dining-options" && $"feed_date" === date)
            .select("record_value")
            .as[String]
        spark.read
            .json(rawDiningOptions.select("record_value").as[String](Encoders.STRING))
            .select($"guid".as("dining_option_guid"), $"behavior".as("dining_option_behavior"))
            .dropDuplicates("dining_option_guid")
    }

    // scalastyle:off method.length
    def transformOrderSummary(
        orderSummary: DataFrame,
        date: String,
        cxiPartnerId: String,
        posIdentitiesByOrderIdAndLocationId: DataFrame,
        diningOptions: DataFrame
    ): DataFrame = {
        import orderSummary.sparkSession.implicits._
        orderSummary
            .withColumn("line_items", $"check.selections")
            .withColumn("discount_id", $"check.appliedDiscounts.discount.guid") // array of discounts for an order
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("ord_desc", lit(null))
            .withColumn("ord_total", $"check.totalAmount")
            .withColumn("ord_pay_total", col("ord_total"))
            .withColumn(
                "discount_amount",
                expr(
                    """AGGREGATE(check.appliedDiscounts, 0.0d, (accumulator, item) -> accumulator + cast(item.discountAmount as double))"""
                )
            )
            .withColumn("ord_type", lit(null))
            .withColumn("line_id", lit(null))
            .withColumn("emp_id", lit(null))
            .withColumn("dsp_qty", lit(null))
            .withColumn("dsp_ttl", lit(null))
            .withColumn("reason_code_id", lit(null))
            .withColumn("service_charge_id", lit(null))
            .withColumn("guest_check_line_item_id", lit(null))
            .withColumn("service_charge", lit(null))
            .withColumn("line_item", explode(col("line_items")))
            .withColumn("item_id", col("line_item.item.guid"))
            .withColumn("item_quantity", col("line_item.quantity"))
            .withColumn("item_price_id", lit(null)) // TODO: Could use help on item/price overall
            .withColumn("item_total", col("line_item.price"))
            .withColumn("guest_check_line_item_id", col("line_item.guid"))
            .withColumn("taxes_id", col("line_item.appliedTaxes.guid"))
            .withColumn("taxes_amount", col("line_item.tax"))
            .withColumn("ord_timestamp", parseToTimestamp(col("ord_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
            .withColumn("ord_date", parseToSqlDate(col("ord_timestamp")))
            .withColumn(
                "service_charge_amount",
                expr(
                    """AGGREGATE(check.appliedServiceCharges, 0.0d, (accumulator, item) -> accumulator + cast(item.chargeAmount as double))"""
                )
            )
            .withColumn("total_taxes_amount", $"check.taxAmount")
            .withColumn(
                "total_tip_amount",
                expr(
                    """AGGREGATE(check.payments, 0.0d, (accumulator, item) -> accumulator + cast(item.tipAmount as double))"""
                )
            )
            .withColumn(
                "ord_sub_total",
                col("ord_total") - (col("total_taxes_amount") + col("total_tip_amount") + col("service_charge_amount"))
            )
            .withColumn("tender_ids", col("check.payments.guid"))
            .withColumn("feed_date", parseToSqlDateIsoFormat(lit(date)))
            .withColumn("ord_state_id", normalizeOrderState(PosToastToCxiOrderStateType)(col("ord_state")))
            .join(diningOptions, $"diningOption.guid" === diningOptions.as("dinopt")("dining_option_guid"), "left")
            .withColumn(
                "ord_target_channel_id",
                getOrdTargetChannelId(PosToastToCxiTargetChannelType)(col("dining_option_behavior"))
            )
            .withColumn(
                "ord_originate_channel_id",
                getOrdOriginateChannelId(PosToastToCxiOriginateChannelType)(col("dining_option_behavior"))
            )
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
            .join(
                posIdentitiesByOrderIdAndLocationId,
                orderSummary("ord_id") === posIdentitiesByOrderIdAndLocationId("order_id") && orderSummary(
                    "location_id"
                ) === posIdentitiesByOrderIdAndLocationId("location_id"),
                "left"
            ) // adds cxi_identity_ids
            .drop(posIdentitiesByOrderIdAndLocationId("order_id"))
            .drop(posIdentitiesByOrderIdAndLocationId("location_id"))
            .dropDuplicates(
                "cxi_partner_id",
                "location_id",
                "ord_id",
                "ord_date",
                "item_id",
                "guest_check_line_item_id"
            )
    }

    private def channelTypeNormalizationUdf(mapping: Map[String, OrderChannelType]): UserDefinedFunction = {
        OrderChannelTypeNormalizationUdfs.normalizeOrderChannelType((behavior: Option[String]) => {
            behavior
                .map(_.toUpperCase())
                .map(b => mapping.getOrElse(b, OrderChannelType.Other))
                .getOrElse(OrderChannelType.Unknown)
        })
    }

    def getOrdTargetChannelId(mapping: Map[String, OrderChannelType]): UserDefinedFunction = {
        channelTypeNormalizationUdf(mapping)
    }

    def getOrdOriginateChannelId(mapping: Map[String, OrderChannelType]): UserDefinedFunction = {
        channelTypeNormalizationUdf(mapping)
    }

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
               |  AND $destTable.guest_check_line_item_id <=> $srcTable.guest_check_line_item_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }
}
