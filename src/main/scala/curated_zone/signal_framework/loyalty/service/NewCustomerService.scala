package com.cxi.cdp.data_processing
package curated_zone.signal_framework.loyalty.service

import curated_zone.model.CustomerLoyaltyType
import curated_zone.signal_framework.loyalty.service.LoyaltyFunctions.{AllLocationsAlias, ByCustomerAndPartner, ByCustomerAndPartnerAndLocation}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_sub, lit, min}

class NewCustomerService extends Serializable {

    def getNewCustomersPerPartnerAndLocation(customer360orders: DataFrame, endDate: String, newCustomerTimeframeDays: Int): DataFrame = {
        customer360orders
            .withColumn("oldest_order", min("ord_date").over(ByCustomerAndPartnerAndLocation))
            // need 1 day adjustment as 'between' takes both upper and lower bound inclusively
            .where(col("oldest_order").between(date_sub(lit(endDate), newCustomerTimeframeDays - 1), endDate))
            .select("customer_360_id", "cxi_partner_id", "location_id")
            .withColumn("loyalty_type", lit(CustomerLoyaltyType.New.value))
            .dropDuplicates()
    }

    def getNewCustomersPerPartnerForAllLocations(customer360orders: DataFrame, endDate: String, newCustomerTimeframeDays: Int): DataFrame = {
        customer360orders
            .withColumn("oldest_order", min("ord_date").over(ByCustomerAndPartner))
            // need 1 day adjustment as 'between' takes both upper and lower bound inclusively
            .where(col("oldest_order").between(date_sub(lit(endDate), newCustomerTimeframeDays - 1), endDate))
            .select("customer_360_id", "cxi_partner_id")
            .withColumn("location_id", lit(AllLocationsAlias))
            .withColumn("loyalty_type", lit(CustomerLoyaltyType.New.value))
            .dropDuplicates()
    }
}

object NewCustomerService {
    def apply(): NewCustomerService = {
        new NewCustomerService
    }
}
