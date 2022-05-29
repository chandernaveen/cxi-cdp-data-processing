package com.cxi.cdp.data_processing
package curated_zone.signal_framework.loyalty

import curated_zone.model.CustomerMetricsTimePeriod
import curated_zone.signal_framework.loyalty.service._
import refined_zone.hub.identity.model.IdentityId
import support.utils.ContractUtils
import support.SparkSessionFactory

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.nio.file.Paths

object LoyaltyTypeSignalsJob {

    private val logger = Logger.getLogger(this.getClass.getName)

    final val LoyaltySignalDomain = "loyalty"
    final val LoyaltyTypeSignalName = "loyalty_type"

    private val newCustomerService: NewCustomerService = NewCustomerService()
    private val loyalCustomerService: LoyalCustomerService = LoyalCustomerService()
    private val atRiskCustomerService: AtRiskCustomerService = AtRiskCustomerService(loyalCustomerService)
    private val regularCustomerService: RegularCustomerService = RegularCustomerService()
    private val loyaltyTypeSignalService: LoyaltyTypeSignalService =
        LoyaltyTypeSignalService(
            newCustomerService,
            atRiskCustomerService,
            loyalCustomerService,
            regularCustomerService
        )

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val spark = SparkSessionFactory.getSparkSession()

        val contractPath = "/mnt/" + args(0)
        val feedDate = args(1)

        val contract: ContractUtils = new ContractUtils(Paths.get(contractPath))

        val curatedDb = contract.prop[String]("datalake.curated.db_name")
        val customer360TableName = contract.prop[String]("datalake.curated.customer_360_table")
        val refinedHubDb = contract.prop[String]("datalake.refined_hub.db_name")
        val orderSummaryTableName = contract.prop[String]("datalake.refined_hub.order_summary_table")
        val customer360partnerLocationWeeklySignalsTable =
            contract.prop[String]("datalake.curated.customer_360_partner_location_weekly_signals")

        val loyaltyConfig = LoyaltyConfig(
            newCustomerTimeframeDays = contract.prop[Int]("loyalty.timeframes.new_customer_days"),
            loyalCustomerTimeframeDays = contract.prop[Int]("loyalty.timeframes.loyal_customer_days"),
            atRiskCustomerTimeframeDays = contract.prop[Int]("loyalty.timeframes.at_risk_customer_days"),
            regularCustomerTimeframeDays = contract.prop[Int]("loyalty.timeframes.regular_customer_days"),
            rfmThreshold = contract.prop[Double]("loyalty.rfmThreshold")
        )

        val finalDf = process(
            spark,
            s"$curatedDb.$customer360TableName",
            s"$refinedHubDb.$orderSummaryTableName",
            feedDate,
            loyaltyConfig
        )

        write(finalDf, s"$curatedDb.$customer360partnerLocationWeeklySignalsTable", feedDate)
    }

    def process(
        spark: SparkSession,
        customer360Table: String,
        orderSummaryTable: String,
        feedDate: String,
        loyaltyConfig: LoyaltyConfig
    ): DataFrame = {

        val (customer360identities, orders) = read(spark, customer360Table, orderSummaryTable)

        val customer360orders = orders
            .join(customer360identities, Seq("qualified_identity_id"), "inner")
            .dropDuplicates(Seq("customer_360_id", "cxi_partner_id", "location_id", "ord_id"))
            .select("customer_360_id", "cxi_partner_id", "ord_date", "location_id", "ord_id", "ord_pay_total")

        val loyaltyTypeSignals7days =
            loyaltyTypeSignalService.getLoyaltyTypesForTimePeriod(
                customer360orders,
                CustomerMetricsTimePeriod.Period7days,
                feedDate,
                loyaltyConfig
            )
        val loyaltyTypeSignals30days =
            loyaltyTypeSignalService.getLoyaltyTypesForTimePeriod(
                customer360orders,
                CustomerMetricsTimePeriod.Period30days,
                feedDate,
                loyaltyConfig
            )
        val loyaltyTypeSignals60days =
            loyaltyTypeSignalService.getLoyaltyTypesForTimePeriod(
                customer360orders,
                CustomerMetricsTimePeriod.Period60days,
                feedDate,
                loyaltyConfig
            )
        val loyaltyTypeSignals90days =
            loyaltyTypeSignalService.getLoyaltyTypesForTimePeriod(
                customer360orders,
                CustomerMetricsTimePeriod.Period90days,
                feedDate,
                loyaltyConfig
            )

        loyaltyTypeSignals7days
            .unionByName(loyaltyTypeSignals30days)
            .unionByName(loyaltyTypeSignals60days)
            .unionByName(loyaltyTypeSignals90days)
    }

    def read(spark: SparkSession, customer360Table: String, orderSummaryTable: String): (DataFrame, DataFrame) = {
        val qualifiedIdentityId = udf(IdentityId.qualifiedIdentityId _)

        val customer360identities = spark
            .table(customer360Table)
            .where(col("active_flag") === true)
            .select(
                col("customer_360_id"),
                explode(col("identities")).as(Seq("identity_type", "identity_values_array"))
            )
            .withColumn("identity_id", explode(col("identity_values_array")))
            .withColumn("qualified_identity_id", qualifiedIdentityId(col("identity_type"), col("identity_id")))
            .select("customer_360_id", "qualified_identity_id")

        val orders = spark
            .table(orderSummaryTable)
            .na
            .drop(Seq("cxi_identity_ids", "ord_date"))
            .select("cxi_identity_ids", "cxi_partner_id", "location_id", "ord_pay_total", "ord_date", "ord_id")
            .withColumn("identities", explode(col("cxi_identity_ids")))
            .withColumn("identity_type", col("identities.identity_type"))
            .withColumn("identity_id", col("identities.cxi_identity_id"))
            .withColumn("qualified_identity_id", qualifiedIdentityId(col("identity_type"), col("identity_id")))
            .drop("cxi_identity_ids", "identities", "identity_id", "identity_type")

        (customer360identities, orders)
    }

    def write(df: DataFrame, destTable: String, feedDate: String): Unit = {
        val srcTable = "new_customer_360_partner_location_weekly_signals"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
               |INSERT OVERWRITE TABLE $destTable
               |PARTITION(signal_generation_date = '$feedDate', signal_domain = '$LoyaltySignalDomain', signal_name = '$LoyaltyTypeSignalName', cxi_partner_id)
               |SELECT cxi_partner_id, location_id, date_option, customer_360_id, signal_value FROM $srcTable
               |""".stripMargin)
    }

    case class LoyaltyConfig(
        newCustomerTimeframeDays: Int,
        loyalCustomerTimeframeDays: Int,
        atRiskCustomerTimeframeDays: Int,
        regularCustomerTimeframeDays: Int,
        rfmThreshold: Double
    )

}
