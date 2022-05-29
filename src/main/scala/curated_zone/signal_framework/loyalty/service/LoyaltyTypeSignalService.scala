package com.cxi.cdp.data_processing
package curated_zone.signal_framework.loyalty.service

import curated_zone.model.CustomerMetricsTimePeriod
import curated_zone.signal_framework.loyalty.service.LoyaltyFunctions.DateFormat
import curated_zone.signal_framework.loyalty.LoyaltyTypeSignalsJob.LoyaltyConfig

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.DataFrame
import org.joda.time.LocalDate

class LoyaltyTypeSignalService(
    val newCustomerService: NewCustomerService,
    val atRiskCustomerService: AtRiskCustomerService,
    val loyalCustomerService: LoyalCustomerService,
    val regularCustomerService: RegularCustomerService
) extends Serializable {

    // TODO: refactor after the final scalafmt config is approved to remove scalastyle warning
    // scalastyle:off method.length
    def getLoyaltyTypesForTimePeriod(
        customer360orders: DataFrame,
        timePeriod: CustomerMetricsTimePeriod,
        feedDate: String,
        loyaltyCfg: LoyaltyConfig
    ): DataFrame = {

        val endDate = LocalDate.parse(feedDate).minusDays(timePeriod.numberOfDays).toString(DateFormat)

        val loyalPerLocation = loyalCustomerService.getLoyalCustomersPerPartnerAndLocation(
            customer360orders,
            endDate,
            loyaltyCfg.loyalCustomerTimeframeDays,
            loyaltyCfg.rfmThreshold
        )

        val loyalAllLocations = loyalCustomerService.getLoyalCustomersPerPartnerForAllLocations(
            customer360orders,
            endDate,
            loyaltyCfg.loyalCustomerTimeframeDays,
            loyaltyCfg.rfmThreshold
        )

        val atRiskPerLocation = atRiskCustomerService.getAtRiskCustomersPerPartnerAndLocation(
            loyalPerLocation,
            customer360orders,
            endDate,
            loyaltyCfg
        )

        val atRiskAllLocations = atRiskCustomerService.getAtRiskCustomersPerPartnerForAllLocations(
            loyalAllLocations,
            customer360orders,
            endDate,
            loyaltyCfg
        )

        val newPerLocation = newCustomerService.getNewCustomersPerPartnerAndLocation(
            customer360orders,
            endDate,
            loyaltyCfg.newCustomerTimeframeDays
        )

        val newAllLocations = newCustomerService.getNewCustomersPerPartnerForAllLocations(
            customer360orders,
            endDate,
            loyaltyCfg.newCustomerTimeframeDays
        )

        val newLoyalAtRiskPerLocation = newPerLocation.unionByName(loyalPerLocation).unionByName(atRiskPerLocation)
        val newLoyalAtRiskAllLocations = newAllLocations.unionByName(loyalAllLocations).unionByName(atRiskAllLocations)

        val regularPerLocation = regularCustomerService.getRegularCustomersPerPartnerAndLocation(
            customer360orders,
            newLoyalAtRiskPerLocation,
            endDate,
            loyaltyCfg.regularCustomerTimeframeDays
        )
        val regularAllLocations = regularCustomerService.getRegularCustomersPerPartnerAllLocations(
            customer360orders,
            newLoyalAtRiskAllLocations,
            endDate,
            loyaltyCfg.regularCustomerTimeframeDays
        )

        newLoyalAtRiskPerLocation
            .unionByName(newLoyalAtRiskAllLocations)
            .unionByName(regularPerLocation)
            .unionByName(regularAllLocations)
            .withColumn("date_option", lit(timePeriod.value))
            .withColumnRenamed("loyalty_type", "signal_value")
    }
}

object LoyaltyTypeSignalService {
    def apply(
        newCustomerService: NewCustomerService,
        atRiskCustomerService: AtRiskCustomerService,
        loyalCustomerService: LoyalCustomerService,
        regularCustomerService: RegularCustomerService
    ): LoyaltyTypeSignalService = {

        new LoyaltyTypeSignalService(
            newCustomerService,
            atRiskCustomerService,
            loyalCustomerService,
            regularCustomerService
        )
    }
}
