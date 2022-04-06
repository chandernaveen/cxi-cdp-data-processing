package com.cxi.cdp.data_processing
package curated_zone.signal_framework.loyalty.service

import curated_zone.model.CustomerLoyaltyType
import curated_zone.signal_framework.loyalty.LoyaltyTypeSignalsJob.{LoyaltyConfig, LoyaltyTypeSignalName}
import curated_zone.signal_framework.loyalty.service.LoyaltyFunctions.{AllLocationsAlias, DateFormat}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

class AtRiskCustomerService(val loyalCustomerService: LoyalCustomerService) extends Serializable {

    def getAtRiskCustomersPerPartnerAndLocation
        (loyalCustomers: DataFrame, customer360orders: DataFrame, endDate: String, loyaltyCfg: LoyaltyConfig): DataFrame = {

        val prevLoyalEndDate = LocalDate.parse(endDate, DateTimeFormat.forPattern(DateFormat))
            .minusDays(loyaltyCfg.atRiskCustomerTimeframeDays - 1).toString(DateFormat)

        val prevLoyal = loyalCustomerService
            .getLoyalCustomersPerPartnerAndLocation(customer360orders, prevLoyalEndDate, loyaltyCfg.loyalCustomerTimeframeDays, loyaltyCfg.rfmThreshold)
            .withColumn("is_prev_loyal", lit(true))

        val currLoyal = loyalCustomers.withColumn("is_curr_loyal", lit(true))

        val atRiskCondition = (col("is_prev_loyal") === true) && (col("is_curr_loyal") === false)

        prevLoyal.join(currLoyal, Seq("customer_360_id", "location_id", "cxi_partner_id"), "left")
            // have nulls after the left join as customer might not be present in the 'current loyal' dataframe
            .na.fill(value = false, Seq("is_curr_loyal"))
            .where(atRiskCondition)
            .select("customer_360_id", "location_id", "cxi_partner_id")
            .withColumn(LoyaltyTypeSignalName, lit(CustomerLoyaltyType.AtRisk.value))
    }

    def getAtRiskCustomersPerPartnerForAllLocations
        (loyalCustomers: DataFrame, customer360orders: DataFrame, endDate: String, loyaltyCfg: LoyaltyConfig): DataFrame = {

        val prevLoyalEndDate = LocalDate.parse(endDate, DateTimeFormat.forPattern(DateFormat))
            .minusDays(loyaltyCfg.atRiskCustomerTimeframeDays - 1).toString(DateFormat)

        val prevLoyal = loyalCustomerService
            .getLoyalCustomersPerPartnerForAllLocations(customer360orders, prevLoyalEndDate, loyaltyCfg.loyalCustomerTimeframeDays, loyaltyCfg.rfmThreshold)
            .withColumn("is_prev_loyal", lit(true))

        val currLoyal = loyalCustomers.withColumn("is_curr_loyal", lit(true))

        val atRiskCondition = (col("is_prev_loyal") === true) && (col("is_curr_loyal") === false)

        prevLoyal.join(currLoyal, Seq("customer_360_id", "cxi_partner_id"), "left")
            // have nulls after the left join as customer might not be present in the 'current loyal' dataframe
            .na.fill(value = false, Seq("is_curr_loyal"))
            .where(atRiskCondition)
            .select("customer_360_id", "cxi_partner_id")
            .withColumn("location_id", lit(AllLocationsAlias))
            .withColumn(LoyaltyTypeSignalName, lit(CustomerLoyaltyType.AtRisk.value))
    }

}

object AtRiskCustomerService {
    def apply(loyalCustomerService: LoyalCustomerService): AtRiskCustomerService = {
        new AtRiskCustomerService(loyalCustomerService)
    }
}




