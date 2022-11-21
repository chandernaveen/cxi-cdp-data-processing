package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore

import raw_zone.pos_omnivore.model.{OrderType, Payment, TicketItem}
import refined_zone.pos_omnivore.config.ProcessorConfig
import refined_zone.pos_omnivore.model.PosOmnivoreOrderStateTypes.PosOmnivoreToCxiOrderStateType
import refined_zone.pos_omnivore.normalization.OrderChannelTypeNormalization
import refined_zone.pos_omnivore.RawRefinedOmnivorePartnerJob.getSchemaRefinedPath
import support.crypto_shredding.PrivacyFunctions.inAuthorizedContext
import support.normalization.udf.DateNormalizationUdfs.{parseToSqlDate, parseToSqlDateIsoFormat}
import support.normalization.udf.MoneyNormalizationUdfs.convertCentsToMoney
import support.normalization.udf.OrderChannelTypeNormalizationUdfs
import support.normalization.udf.OrderStateNormalizationUdfs.normalizeOrderState
import support.normalization.udf.TimestampNormalizationUdfs.convertToTimestamp
import support.WorkspaceConfigReader

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

object OrderSummaryProcessor {

    def process(
        spark: SparkSession,
        config: ProcessorConfig,
        destDbName: String,
        posIdentityIdsByOrder: DataFrame
    ): Unit = {

        val orderSummaryTable = config.contract.prop[String](getSchemaRefinedPath("order_summary_table"))

        val orderSummary = readOrderSummary(spark, config.dateRaw, config.srcDbName, config.srcTable)

        val processedOrderSummary =
            transformOrderSummary(orderSummary, config.dateRaw, config.cxiPartnerId, posIdentityIdsByOrder)

        val workspaceConfigPath: String = config.contract.prop[String]("databricks_workspace_config")
        val workspaceConfig = WorkspaceConfigReader.readWorkspaceConfig(spark, workspaceConfigPath)
        inAuthorizedContext(spark, workspaceConfig) {
            writeOrderSummary(processedOrderSummary, config.cxiPartnerId, s"$destDbName.$orderSummaryTable")
        }
    }

    def readOrderSummary(spark: SparkSession, date: String, dbName: String, table: String): DataFrame = {
        spark.sql(s"""
               |SELECT
               |get_json_object(record_value, "$$.id") as ord_id,
               |get_json_object(record_value, "$$.totals.total") as ord_total,
               |get_json_object(record_value, "$$.opened_at") as opened_at,
               |get_json_object(record_value, "$$.closed_at") as closed_at,
               |get_json_object(record_value, "$$._links.self.href") as location_url,
               |get_json_object(record_value, "$$.open") as open,
               |get_json_object(record_value, "$$.void") as void,
               |get_json_object(record_value, "$$._embedded.order_type") as ord_type,
               |get_json_object(record_value, "$$._embedded.employee.id") as emp_id,
               |get_json_object(record_value, "$$.totals.discounts") as discount_amount,
               |get_json_object(record_value, "$$.totals.service_charges") as service_charge_amount,
               |get_json_object(record_value, "$$.totals.tax") as total_taxes_amount,
               |get_json_object(record_value, "$$.totals.tips") as total_tip_amount,
               |get_json_object(record_value, "$$.totals.sub_total") as ord_sub_total,
               |get_json_object(record_value, "$$.totals.paid") as ord_pay_total,
               |get_json_object(record_value, "$$._embedded.items") as items,
               |get_json_object(record_value, "$$._embedded.service_charges") as service_charges,
               |get_json_object(record_value, "$$._embedded.payments") as payments
               |FROM $dbName.$table
               |WHERE record_type="tickets" AND feed_date="$date"
               |""".stripMargin)
    }

    // scalastyle:off method.length
    def transformOrderSummary(
        orderSummary: DataFrame,
        date: String,
        cxiPartnerId: String,
        posIdentityIdsByOrder: DataFrame
    ): DataFrame = {

        val locIdIndex: Int = 5
        val getOrderStateUdf = udf(getOrderState)

        orderSummary
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("ord_desc", lit(null))
            .withColumn("ord_total", convertCentsToMoney("ord_total"))
            .withColumn(
                "ord_timestamp",
                convertToTimestamp(coalesce(col("closed_at"), col("opened_at")))
            )
            .withColumn("ord_date", parseToSqlDate(col("ord_timestamp")))
            .withColumn("location_id", split(col("location_url"), "/")(locIdIndex))
            .withColumn(
                "ord_state_id",
                normalizeOrderState(PosOmnivoreToCxiOrderStateType)(getOrderStateUdf(col("open"), col("void")))
            )
            .withColumn("ord_type", from_json(col("ord_type"), Encoders.product[OrderType].schema))
            .withColumn("ord_originate_channel_id", normalizeOrderOriginateChannelTypeUdf(col("ord_type")))
            .withColumn("ord_target_channel_id", normalizeOrderTargetChannelTypeUdf(col("ord_type")))
            .withColumn("ord_type", lit(null))
            .withColumn(
                "items",
                from_json(col("items"), DataTypes.createArrayType(Encoders.product[TicketItem].schema))
            )
            .withColumn("item", explode(col("items")))
            .withColumn("item_id", col("item.id"))
            .withColumn("item_quantity", col("item.quantity").cast("integer"))
            .withColumn("item_price_id", lit(null))
            .withColumn("item_total", convertCentsToMoney("item.price"))
            .withColumn("discount_id", lit(null))
            .withColumn("discount_amount", convertCentsToMoney("discount_amount"))
            .withColumn("dsp_qty", lit(null))
            .withColumn("dsp_ttl", lit(null))
            .withColumn("reason_code_id", lit(null))
            .withColumn("service_charge_id", lit(null))
            .withColumn("guest_check_line_item_id", lit(null))
            .withColumn("line_id", lit(null))
            .withColumn("taxes_id", lit(null))
            .withColumn("taxes_amount", convertCentsToMoney("item.included_tax"))
            .withColumn("service_charge_amount", convertCentsToMoney("service_charge_amount"))
            .withColumn("total_taxes_amount", convertCentsToMoney("total_taxes_amount"))
            .withColumn("total_tip_amount", convertCentsToMoney("total_tip_amount"))
            .withColumn("ord_sub_total", convertCentsToMoney("ord_sub_total"))
            .withColumn("ord_pay_total", convertCentsToMoney("ord_pay_total"))
            .withColumn(
                "payments",
                from_json(col("payments"), DataTypes.createArrayType(Encoders.product[Payment].schema))
            )
            .withColumn("tender_ids", col("payments._embedded.tender_type.id"))
            .withColumn("feed_date", parseToSqlDateIsoFormat(lit(date)))
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
                "ord_originate_channel_id",
                "ord_target_channel_id",
                "item_quantity",
                "item_total",
                "emp_id",
                "discount_id",
                "discount_amount",
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
                "ord_sub_total",
                "ord_pay_total",
                "feed_date"
            )
            .filter(col("ord_timestamp").isNotNull)
            .filter(col("ord_date").isNotNull)
            .join(
                posIdentityIdsByOrder,
                Seq("ord_id", "location_id"),
                "left"
            )
            .drop(posIdentityIdsByOrder("ord_id"))
            .drop(posIdentityIdsByOrder("location_id"))
            .dropDuplicates("cxi_partner_id", "location_id", "ord_id", "ord_date", "item_id")
    }

    val normalizeOrderOriginateChannelTypeUdf = OrderChannelTypeNormalizationUdfs.normalizeOrderChannelType(
        OrderChannelTypeNormalization.normalizeOrderOriginateChannelType _
    )

    val normalizeOrderTargetChannelTypeUdf = OrderChannelTypeNormalizationUdfs.normalizeOrderChannelType(
        OrderChannelTypeNormalization.normalizeOrderTargetChannelType _
    )

    private def getOrderState: ((String, String) => String) = { (open, void) =>
        (open, void) match {
            case ("true", "true") => "CANCELLED"
            case ("true", "false") => "OPEN"
            case ("false", "true") => "CANCELLED"
            case ("false", "false") => "COMPLETED"
            case _ => "UNKNOWN"
        }
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
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }
}
