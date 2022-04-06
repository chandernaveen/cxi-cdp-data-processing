package com.cxi.cdp.data_processing
package curated_zone.signal_framework.loyalty.service

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.joda.time.LocalDate.parse

object LoyaltyFunctions {

    final val DateFormat = "yyyy-MM-dd"
    private val monthTimeframeDays = 30
    final val AllLocationsAlias = "_ALL"
    final val ByCustomerAndPartnerAndLocation: WindowSpec = Window.partitionBy("customer_360_id", "location_id", "cxi_partner_id")
    final val ByCustomerAndPartner: WindowSpec = Window.partitionBy("customer_360_id", "cxi_partner_id")
    final val ByPartnerAndLocation: WindowSpec = Window.partitionBy("location_id", "cxi_partner_id")
    final val ByPartner: WindowSpec = Window.partitionBy("cxi_partner_id")

    private def recencyScore(customer360orders: DataFrame, startDate: String, latestOrderSpec: WindowSpec, dayToTxnSpec: WindowSpec): DataFrame = {
        customer360orders
            .withColumn("latest_order", max("ord_date").over(latestOrderSpec))
            .withColumn("days_to_txn", datediff(col("latest_order"), to_date(lit(startDate))))
            .withColumn("max_days_to_txn", max("days_to_txn").over(dayToTxnSpec))
            .withColumn("min_days_to_txn", min("days_to_txn").over(dayToTxnSpec))
            .withColumn("recency_score", (col("days_to_txn") - col("min_days_to_txn")) /
                (col("max_days_to_txn") - col("min_days_to_txn")))
    }

    private[service] def recencyScorePerPartnerAndLocation(customer360orders: DataFrame, startDate: String): DataFrame = {
        recencyScore(customer360orders, startDate, ByCustomerAndPartnerAndLocation, ByPartnerAndLocation)
            .dropDuplicates(Seq("cxi_partner_id", "customer_360_id", "location_id"))
            .na.fill(1.0)
            .select("customer_360_id", "location_id", "cxi_partner_id", "recency_score")
    }

    private[service] def recencyScorePerPartnerForAllLocations(customer360orders: DataFrame, startDate: String): DataFrame = {
        recencyScore(customer360orders, startDate, ByCustomerAndPartner, ByPartner)
            .dropDuplicates(Seq("cxi_partner_id", "customer_360_id"))
            .withColumn("location_id", lit(AllLocationsAlias))
            .na.fill(1.0)
            .select("customer_360_id", "location_id", "cxi_partner_id", "recency_score")
    }

    private def frequencyScore(customer360orders: DataFrame, groupByColumns: Seq[String], windowSpec: WindowSpec): DataFrame = {
        customer360orders
            .groupBy(groupByColumns.map(c => col(c)): _*)
            .count()
            .withColumn("max_count", max("count").over(windowSpec))
            .withColumn("min_count", min("count").over(windowSpec))
            .withColumn("frequency_score", (col("count") - col("min_count")) / (col("max_count") - col("min_count")))
            .na.fill(1.0)
    }

    private[service] def frequencyScorePerPartnerAndLocation(customer360orders: DataFrame): DataFrame = {
        frequencyScore(customer360orders, Seq("customer_360_id", "location_id", "cxi_partner_id"), ByPartnerAndLocation)
            .select("customer_360_id", "location_id", "cxi_partner_id", "frequency_score")
    }

    private[service] def frequencyScorePerPartnerForAllLocations(customer360orders: DataFrame): DataFrame = {
        frequencyScore(customer360orders, Seq("customer_360_id", "cxi_partner_id"), ByPartner)
            .withColumn("location_id", lit(AllLocationsAlias))
            .select("customer_360_id", "location_id", "cxi_partner_id", "frequency_score")
    }

    private def monetaryScore(customer360orders: DataFrame, ordPalTotalSpec: WindowSpec, minMaxAmountSpec : WindowSpec): DataFrame = {
        customer360orders
            .withColumn("total_amount", sum("ord_pay_total").over(ordPalTotalSpec))
            .withColumn("max_amount", max("total_amount").over(minMaxAmountSpec))
            .withColumn("min_amount", min("total_amount").over(minMaxAmountSpec))
            .withColumn("monetary_score", (col("total_amount") - col("min_amount")) /
                (col("max_amount") - col("min_amount")))
    }

    private[service] def monetaryScorePerPartnerAndLocation(customer360orders: DataFrame): DataFrame = {
        monetaryScore(customer360orders, ByCustomerAndPartnerAndLocation, ByPartnerAndLocation)
            .dropDuplicates(Seq("cxi_partner_id", "customer_360_id", "location_id"))
            .na.fill(1.0)
            .select("customer_360_id", "location_id", "cxi_partner_id", "monetary_score")
    }

    private[service] def monetaryScorePerPartnerForAllLocations(customer360orders: DataFrame): DataFrame = {
        monetaryScore(customer360orders, ByCustomerAndPartner, ByPartner)
            .dropDuplicates(Seq("cxi_partner_id", "customer_360_id"))
            .na.fill(1.0)
            .withColumn("location_id", lit(AllLocationsAlias))
            .select("customer_360_id", "location_id", "cxi_partner_id", "monetary_score")
    }

    private def loyalIndicator(customer360orders: DataFrame, endDate: String, windowSpec: WindowSpec): DataFrame = {
        val end = parse(endDate)
        val month1 = (end.minusDays(monthTimeframeDays * 3 - 1).toString(DateFormat), end.minusDays(monthTimeframeDays * 2).toString(DateFormat))
        val month2 = (end.minusDays(monthTimeframeDays * 2 - 1).toString(DateFormat), end.minusDays(monthTimeframeDays).toString(DateFormat))
        val month3 = (end.minusDays(monthTimeframeDays - 1).toString(DateFormat), end.toString(DateFormat))

        val isLoyal = (col("month1_count") > 0) && (col("month2_count") > 0) && (col("month3_count") > 0)
        customer360orders
            .withColumn("month1", when(col("ord_date").between(month1._1, month1._2), 1).otherwise(0))
            .withColumn("month2", when(col("ord_date").between(month2._1, month2._2), 1).otherwise(0))
            .withColumn("month3", when(col("ord_date").between(month3._1, month3._2), 1).otherwise(0))
            .withColumn("month1_count", sum(col("month1")).over(windowSpec))
            .withColumn("month2_count", sum(col("month2")).over(windowSpec))
            .withColumn("month3_count", sum(col("month3")).over(windowSpec))
            .withColumn("txn_all_months", when(isLoyal, 1).otherwise(0))
    }

    private[service] def loyalIndicatorPerPartnerAndLocation(customer360orders: DataFrame, endDate: String): DataFrame = {
        loyalIndicator(customer360orders, endDate, ByCustomerAndPartnerAndLocation)
            .dropDuplicates("customer_360_id", "location_id", "cxi_partner_id")
            .select("customer_360_id", "location_id", "cxi_partner_id", "txn_all_months")
    }

    private[service] def loyalIndicatorPerPartnerForAllLocations(customer360orders: DataFrame, endDate: String): DataFrame = {
       loyalIndicator(customer360orders, endDate, ByCustomerAndPartner)
            .dropDuplicates("customer_360_id",  "cxi_partner_id")
            .withColumn("location_id", lit(AllLocationsAlias))
            .select("customer_360_id", "location_id", "cxi_partner_id", "txn_all_months")
    }

    private def loyalty
        (customer360orders: DataFrame, joinColumns: Seq[String], rfmThreshold: Double, rfmScores: RfmScores, windowSpec: WindowSpec): DataFrame = {

        val customer360ordersByDateWithScores = customer360orders
            .join(rfmScores.recencyScore, joinColumns, "left")
            .join(rfmScores.frequencyScore, joinColumns, "left")
            .join(rfmScores.monetaryScore, joinColumns, "left")
            .join(rfmScores.loyalIndicator, joinColumns, "left")

        val isLoyal = (col("rfm_score") > rfmThreshold) && (col("txn_all_months") === 1)

        customer360ordersByDateWithScores
            .withColumn("final_score", col("recency_score") + col("frequency_score") + col("monetary_score"))
            .withColumn("max_final_score", max("final_score").over(windowSpec))
            .withColumn("min_final_score", min("final_score").over(windowSpec))
            .withColumn("rfm_score", (col("final_score") - col("min_final_score")) / (col("max_final_score") - col("min_final_score")))
            .na.fill(1.0)
            .withColumn("is_loyal", when(isLoyal, true).otherwise(false))
    }

    private[service] def loyaltyPerPartnerAndLocation(customer360orders: DataFrame, rfmThreshold: Double, rfmScores: RfmScores): DataFrame = {

        loyalty(customer360orders, Seq("customer_360_id", "location_id", "cxi_partner_id"), rfmThreshold, rfmScores, ByPartnerAndLocation)
            .dropDuplicates("customer_360_id", "cxi_partner_id", "location_id", "txn_all_months", "rfm_score")
            .select("customer_360_id", "location_id", "cxi_partner_id", "is_loyal")
    }

    private[service] def loyaltyPerPartnerForAllLocations(customer360orders: DataFrame, rfmThreshold: Double, rfmScores: RfmScores): DataFrame = {

        loyalty(customer360orders, Seq("customer_360_id", "cxi_partner_id"), rfmThreshold, rfmScores, ByPartner)
            .dropDuplicates("customer_360_id", "cxi_partner_id", "txn_all_months", "rfm_score")
            .select("customer_360_id", "cxi_partner_id", "is_loyal")
            .withColumn("location_id", lit(AllLocationsAlias))
            .select("customer_360_id", "location_id", "cxi_partner_id", "is_loyal")
    }

    private[service] def getRfmScoresAndLoyaltyIndicatorPerPartnerAndLocation(customer360orders: DataFrame, startDate: String, endDate: String): RfmScores = {

        val recencyScore = recencyScorePerPartnerAndLocation(customer360orders, startDate)
        val frequencyScore = frequencyScorePerPartnerAndLocation(customer360orders)
        val monetaryScore = monetaryScorePerPartnerAndLocation(customer360orders)
        val loyalIndicator = loyalIndicatorPerPartnerAndLocation(customer360orders, endDate)

        RfmScores(recencyScore, frequencyScore, monetaryScore, loyalIndicator)
    }

    private[service] def getRfmScoresAndLoyaltyIndicatorPerPartnerForAllLocations
        (customer360orders: DataFrame, startDate: String, endDate: String): RfmScores = {

        val recencyScore = recencyScorePerPartnerForAllLocations(customer360orders, startDate)
        val frequencyScore = frequencyScorePerPartnerForAllLocations(customer360orders)
        val monetaryScore = monetaryScorePerPartnerForAllLocations(customer360orders)
        val loyalIndicator = loyalIndicatorPerPartnerForAllLocations(customer360orders, endDate)

        RfmScores(recencyScore, frequencyScore, monetaryScore, loyalIndicator)
    }

    case class RfmScores (recencyScore: DataFrame, frequencyScore: DataFrame, monetaryScore: DataFrame, loyalIndicator: DataFrame)
}
