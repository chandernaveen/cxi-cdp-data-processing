package com.cxi.cdp.data_processing
package curated_zone.signal_framework.loyalty.service

import curated_zone.model.CustomerLoyaltyType
import curated_zone.signal_framework.loyalty.LoyaltyTypeSignalsJob.LoyaltyTypeSignalName
import curated_zone.signal_framework.loyalty.service.LoyaltyFunctions.DateFormat

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

class LoyalCustomerService extends Serializable {

    def getLoyalCustomersPerPartnerAndLocation
        (customer360orders: DataFrame, endDate: String, loyalCustomerTimeframeDays: Int, rfmThreshold: Double): DataFrame = {

        val startDate = LocalDate.parse(endDate, DateTimeFormat.forPattern(DateFormat)).minusDays(loyalCustomerTimeframeDays - 1).toString(DateFormat)
        val customer360ordersByDate = customer360orders
            .where(col("ord_date").between(startDate, endDate))

        val rfmScores = LoyaltyFunctions.getRfmScoresAndLoyaltyIndicatorPerPartnerAndLocation(customer360ordersByDate, startDate, endDate)
        LoyaltyFunctions.loyaltyPerPartnerAndLocation(customer360ordersByDate, rfmThreshold, rfmScores)
            .where(col("is_loyal") === true)
            .withColumn(LoyaltyTypeSignalName, lit(CustomerLoyaltyType.Loyal.value))
            .select("customer_360_id", "location_id", "cxi_partner_id", LoyaltyTypeSignalName)
    }

    def getLoyalCustomersPerPartnerForAllLocations
        (customer360orders: DataFrame, endDate: String, loyalCustomerTimeframeDays: Int, rfmThreshold: Double): DataFrame = {

        val startDate = LocalDate.parse(endDate, DateTimeFormat.forPattern(DateFormat)).minusDays(loyalCustomerTimeframeDays - 1).toString(DateFormat)
        val customer360ordersByDate = customer360orders
            .where(col("ord_date").between(startDate, endDate))

        val rfmScores = LoyaltyFunctions.getRfmScoresAndLoyaltyIndicatorPerPartnerForAllLocations(customer360ordersByDate, startDate, endDate)
        LoyaltyFunctions.loyaltyPerPartnerForAllLocations(customer360ordersByDate, rfmThreshold, rfmScores)
            .where(col("is_loyal") === true)
            .withColumn(LoyaltyTypeSignalName, lit(CustomerLoyaltyType.Loyal.value))
            .select("customer_360_id", "location_id", "cxi_partner_id", LoyaltyTypeSignalName)
    }

}

object LoyalCustomerService {
    def apply(): LoyalCustomerService = {
        new LoyalCustomerService()
    }
}


