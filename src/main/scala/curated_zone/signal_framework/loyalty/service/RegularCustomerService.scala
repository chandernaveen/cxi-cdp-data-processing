package com.cxi.cdp.data_processing
package curated_zone.signal_framework.loyalty.service

import curated_zone.model.CustomerLoyaltyType
import curated_zone.signal_framework.loyalty.service.LoyaltyFunctions.AllLocationsAlias
import curated_zone.signal_framework.loyalty.LoyaltyTypeSignalsJob.LoyaltyTypeSignalName

import org.apache.spark.sql.functions.{col, date_sub, lit}
import org.apache.spark.sql.DataFrame

class RegularCustomerService extends Serializable {

    def getRegularCustomersPerPartnerAndLocation(
        customer360orders: DataFrame,
        newLoyalAtRiskPerLocation: DataFrame,
        endDate: String,
        regularCustomerTimeframeDays: Int
    ): DataFrame = {

        customer360orders
            // need 1 day adjustment as 'between' takes both upper and lower bound inclusively
            .where(col("ord_date").between(date_sub(lit(endDate), regularCustomerTimeframeDays - 1), endDate))
            .join(newLoyalAtRiskPerLocation, Seq("customer_360_id", "location_id", "cxi_partner_id"), "left_anti")
            .dropDuplicates("customer_360_id", "location_id", "cxi_partner_id")
            .withColumn(LoyaltyTypeSignalName, lit(CustomerLoyaltyType.Regular.value))
            .select("customer_360_id", "location_id", "cxi_partner_id", LoyaltyTypeSignalName)
    }

    def getRegularCustomersPerPartnerAllLocations(
        customer360orders: DataFrame,
        newLoyalAtRiskAllLocations: DataFrame,
        endDate: String,
        regularCustomerTimeframeDays: Int
    ): DataFrame = {

        customer360orders
            // need 1 day adjustment as 'between' takes both upper and lower bound inclusively
            .where(col("ord_date").between(date_sub(lit(endDate), regularCustomerTimeframeDays - 1), endDate))
            .join(newLoyalAtRiskAllLocations, Seq("customer_360_id", "cxi_partner_id"), "left_anti")
            .dropDuplicates("customer_360_id", "cxi_partner_id")
            .select("customer_360_id", "cxi_partner_id")
            .withColumn("location_id", lit(AllLocationsAlias))
            .withColumn(LoyaltyTypeSignalName, lit(CustomerLoyaltyType.Regular.value))
    }

}

object RegularCustomerService {
    def apply(): RegularCustomerService = {
        new RegularCustomerService
    }
}
