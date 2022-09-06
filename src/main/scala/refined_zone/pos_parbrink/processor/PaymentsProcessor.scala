package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor

import refined_zone.pos_parbrink.config.ProcessorConfig
import refined_zone.pos_parbrink.model.{CardBrandType, ParbrinkRecordType}
import refined_zone.pos_parbrink.model.ParbrinkRawModels.Payment
import refined_zone.pos_parbrink.normalization.PaymentStatusNormalization.normalizePaymentStatus
import refined_zone.pos_parbrink.RawRefinedParbrinkPartnerJob.getSchemaRefinedPath

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, DataTypes}

object PaymentsProcessor {

    def process(spark: SparkSession, config: ProcessorConfig, destDbName: String): DataFrame = {

        val paymentTable = config.contract.prop[String](getSchemaRefinedPath("payment_table"))

        val payments = readPayments(spark, config.dateRaw, s"${config.srcDbName}.${config.srcTable}")
        val orderTenders = readOrderTenders(spark, config.dateRaw, s"${config.srcDbName}.${config.srcTable}")

        val processedPayments = transformPayments(payments, orderTenders, config.cxiPartnerId)

        writePayments(processedPayments, config.cxiPartnerId, s"$destDbName.$paymentTable")

        processedPayments
    }

    def readOrderTenders(spark: SparkSession, date: String, table: String): DataFrame = {
        import spark.implicits._
        val fromRecordValue = path => get_json_object($"record_value", path)

        spark
            .table(table)
            .filter($"record_type" === ParbrinkRecordType.OrderTenders.value && $"feed_date" === date)
            .filter(col("record_value").isNotNull)
            .select(
                col("location_id"),
                fromRecordValue("$.Id").as("tender_id"),
                fromRecordValue("$.Active").as("is_active_tender").cast(BooleanType),
                fromRecordValue("$.CardType").as("tender_card_type")
            )
            .dropDuplicates("location_id", "tender_id")
    }

    def readPayments(spark: SparkSession, date: String, table: String): DataFrame = {
        import spark.implicits._
        val fromRecordValue = path => get_json_object($"record_value", path)

        spark
            .table(table)
            .filter($"record_type" === ParbrinkRecordType.Orders.value && $"feed_date" === date)
            .select(
                col("location_id"),
                fromRecordValue("$.Id").as("order_id"),
                fromRecordValue("$.Payments").as("payments")
            )
    }

    def transformPayments(payments: DataFrame, tenders: DataFrame, cxiPartnerId: String): DataFrame = {
        payments
            .dropDuplicates("order_id", "location_id")
            .withColumn(
                "payments",
                from_json(
                    col("payments"),
                    DataTypes.createArrayType(Encoders.product[Payment].schema)
                )
            )
            .withColumn("payments", explode(col("payments")))
            .withColumn("tender_id", col("payments.TenderId"))
            .join(tenders, Seq("tender_id", "location_id"), "left_outer")
            .withColumn("payment_id", concat(col("order_id"), lit("_"), col("payments.Id")))
            .withColumn("name", col("payments.CardHolderName"))
            .withColumn("pan", col("payments.CardNumber"))
            .withColumn("status", normalizePaymentStatus(col("payments.IsDeleted"), col("is_active_tender")))
            .withColumn("card_brand", getPaymentCardBrand(col("tender_card_type")))
            .select("payment_id", "order_id", "location_id", "status", "name", "card_brand", "pan")
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("bin", lit(null))
            .withColumn("exp_month", lit(null))
            .withColumn("exp_year", lit(null))
    }

    def writePayments(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newPayments"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id <=> "$cxiPartnerId"
               |    AND $destTable.location_id <=> $srcTable.location_id
               |    AND $destTable.payment_id <=> $srcTable.payment_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

    def getPaymentCardBrand: UserDefinedFunction = {
        udf((code: Int) => CardBrandType.withValueOpt(code).map(_.name))
    }

}
