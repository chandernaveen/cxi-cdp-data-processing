package com.cxi.cdp.data_processing
package refined_zone.pos_toast

import refined_zone.pos_toast.config.ProcessorConfig
import refined_zone.pos_toast.RawRefinedToastPartnerJob.getSchemaRefinedPath

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions.{coalesce, col, concat, explode, from_json, lit}

object PaymentsProcessor {
    def process(spark: SparkSession, config: ProcessorConfig, destDbName: String): DataFrame = {

        val paymentTable = config.contract.prop[String](getSchemaRefinedPath("payment_table"))

        val payments = readPayments(spark, config.dateRaw, s"${config.srcDbName}.${config.srcTable}")

        val processedPayments = transformPayments(payments, config.cxiPartnerId)

        writePayments(
            processedPayments,
            config.cxiPartnerId,
            s"$destDbName.$paymentTable"
        )
        processedPayments
    }

    def readPayments(spark: SparkSession, date: String, table: String): DataFrame = {
        import spark.implicits._

        val rawOrdersAsStrings = spark
            .table(table)
            .filter($"record_type" === "orders" && $"feed_date" === date)
            .select("record_value", "location_id")

        val ordersRecordTypeDf = spark.read.json(rawOrdersAsStrings.select("record_value").as[String](Encoders.STRING))

        val df = rawOrdersAsStrings
            .withColumn("record_value", from_json(col("record_value"), ordersRecordTypeDf.schema))
            .select(
                $"record_value.guid".as("order_id"),
                $"location_id",
                explode($"record_value.checks").as("check")
            )
            .withColumn("payment", explode($"check.payments"))
            .select("order_id", "location_id", "payment.*")

        // Q: no first/last name in source payment ? even though present in schema, they marked as 'internal use only'
        // https://doc.toasttab.com/openapi/orders/operation/ordersBulkGet/#!c=200&path=checks/payments/cardHolderFirstName&t=response
        val dfWithFirstLastName = Seq("cardHolderFirstName", "cardHolderLastName")
            .foldLeft(df) { case (acc, col_name) =>
                if (df.columns.contains(col_name)) {
                    acc.withColumn(col_name, col(col_name))
                } else {
                    acc.withColumn(col_name, lit(null))
                }
            }

        dfWithFirstLastName
            .select(
                $"order_id",
                $"location_id",
                $"guid" as "payment_id",
                $"paymentStatus" as "status", // possible values: AUTHORIZED, CAPTURED, DENIED, null
                $"cardType" as "card_brand",
                $"last4Digits" as "pan",
                $"cardHolderFirstName" as "first_name",
                $"cardHolderLastName" as "last_name"
            )
    }

    def transformPayments(payments: DataFrame, cxiPartnerId: String): DataFrame = {
        payments
            .withColumn("bin", lit(null)) // Q: no bin in source payment ?
            .withColumn("exp_month", lit(null)) // Q: no exp_month in source payment ?
            .withColumn("exp_year", lit(null)) // Q: no exp_year in source payment ?
            // alternative checks[*].appliedPreauthInfo but toast docs mention it as 'internal use only' field
            // https://doc.toasttab.com/openapi/orders/operation/ordersBulkGet/#!c=200&path=checks/appliedPreauthInfo&t=response
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .dropDuplicates("cxi_partner_id", "location_id", "order_id", "payment_id")
    }

    def writePayments(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newPayments"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id <=> "$cxiPartnerId" AND $destTable.location_id <=> $srcTable.location_id
               |  AND $destTable.order_id <=> $srcTable.order_id AND $destTable.payment_id <=> $srcTable.payment_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

}
