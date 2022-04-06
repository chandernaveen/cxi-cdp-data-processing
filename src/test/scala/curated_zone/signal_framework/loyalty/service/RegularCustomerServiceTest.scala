package com.cxi.cdp.data_processing
package curated_zone.signal_framework.loyalty.service

import curated_zone.model.CustomerLoyaltyType
import curated_zone.signal_framework.loyalty.service.LoyaltyFunctions.AllLocationsAlias
import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

import java.time.LocalDate.parse

class RegularCustomerServiceTest extends BaseSparkBatchJobTest {

    test("Regular customers per partner and location") {

        // given
        import spark.implicits._
        val customer360orders = Seq(
            ("cust1", "partner1", "loc1", "ord1", parse("2022-02-24")),
            ("cust1", "partner1", "loc2", "ord2", parse("2022-02-15")),
            ("cust1", "partner1", "loc3", "ord3", parse("2022-02-22")),
            ("cust1", "partner2", "loc4", "ord4", parse("2022-02-16")),
            // 3 orders, should be taken only once
            ("cust2", "partner1", "loc1", "ord51", parse("2022-02-17")),
            ("cust2", "partner1", "loc1", "ord52", parse("2022-02-17")),
            ("cust2", "partner1", "loc1", "ord53", parse("2022-02-17")),
            ("cust3", "partner3", "loc1", "ord6", parse("2022-02-24")),
            // should be ignored, 'ord_date' is out of the timeframe
            ("cust4", "partner4", "loc1", "ord7", parse("2022-02-14")),
            // should be ignored, 'ord_date' is out of the timeframe
            ("cust5", "partner5", "loc1", "ord8", parse("2022-02-25"))

        ).toDF("customer_360_id", "cxi_partner_id", "location_id", "ord_id", "ord_date")

        val newLoyalAtRiskPerLocation = Seq(
            ("cust1", "partner1", "loc1", CustomerLoyaltyType.New.value),
            ("cust1", "partner1", "loc2", CustomerLoyaltyType.AtRisk.value),
            ("cust2", "partner1", "loc2", CustomerLoyaltyType.Loyal.value),

        ).toDF("customer_360_id", "cxi_partner_id", "location_id", "loyalty_type")

        // when
        val actual = RegularCustomerService()
            .getRegularCustomersPerPartnerAndLocation(customer360orders, newLoyalAtRiskPerLocation, "2022-02-24", 10)

        // then
        withClue("Regular customers per partner and location do not match") {
            val expected = Seq(
                ("cust1", "loc3", "partner1", CustomerLoyaltyType.Regular.value),
                ("cust1", "loc4", "partner2", CustomerLoyaltyType.Regular.value),
                ("cust2", "loc1", "partner1", CustomerLoyaltyType.Regular.value),
                ("cust3", "loc1", "partner3", CustomerLoyaltyType.Regular.value)

            ).toDF("customer_360_id", "location_id", "cxi_partner_id", "loyalty_type")
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("Regular customers per partner for all locations") {

        // given
        import spark.implicits._
        val customer360orders = Seq(
            ("cust1", "partner1", "loc1", "ord1", parse("2022-02-24")),
            ("cust1", "partner1", "loc2", "ord2", parse("2022-02-15")),
            ("cust1", "partner1", "loc3", "ord3", parse("2022-02-23")),
            // 2 orders, should be taken only once
            ("cust1", "partner2", "loc4", "ord41", parse("2022-02-16")),
            ("cust1", "partner2", "loc4", "ord42", parse("2022-02-16")),
            ("cust2", "partner1", "loc1", "ord5", parse("2022-02-22")),
            ("cust3", "partner3", "loc1", "ord6", parse("2022-02-24")),
            // should be ignored, 'ord_date' is out of the timeframe
            ("cust4", "partner4", "loc1", "ord7", parse("2022-02-14")),
            // should be ignored, 'ord_date' is out of the timeframe
            ("cust5", "partner5", "loc1", "ord8", parse("2022-02-25"))

        ).toDF("customer_360_id", "cxi_partner_id", "location_id", "ord_id", "ord_date")

        val newLoyalAtRiskAllLocations = Seq(
            ("cust1", "partner1", AllLocationsAlias, CustomerLoyaltyType.New.value),
            ("cust2", "partner1", AllLocationsAlias, CustomerLoyaltyType.AtRisk.value),
            ("cust2", "partner2", AllLocationsAlias, CustomerLoyaltyType.Loyal.value)

        ).toDF("customer_360_id", "cxi_partner_id", "location_id", "loyalty_type")

        // when
        val actual = RegularCustomerService()
            .getRegularCustomersPerPartnerAllLocations(customer360orders, newLoyalAtRiskAllLocations, "2022-02-24", 10)

        // then
        withClue("Regular customers per partner and location do not match") {
            val expected = Seq(
                ("cust1", "partner2", AllLocationsAlias, CustomerLoyaltyType.Regular.value),
                ("cust3", "partner3", AllLocationsAlias, CustomerLoyaltyType.Regular.value)

            ).toDF("customer_360_id", "cxi_partner_id", "location_id", "loyalty_type")
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

}
