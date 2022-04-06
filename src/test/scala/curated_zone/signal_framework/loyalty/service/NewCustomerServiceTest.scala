package com.cxi.cdp.data_processing
package curated_zone.signal_framework.loyalty.service

import curated_zone.model.CustomerLoyaltyType
import curated_zone.signal_framework.loyalty.service.LoyaltyFunctions.AllLocationsAlias
import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

import java.time.LocalDate.parse

class NewCustomerServiceTest extends BaseSparkBatchJobTest {

    test("Get new customers per partner and location") {

        // given
        import spark.implicits._
        val customer360orders = Seq(
            // made 2 orders, should be considered only once
            ("cust1", "partner1", parse("2022-02-24"), "loc1", "ord1"),
            ("cust1", "partner1", parse("2022-02-23"), "loc1", "ord2"),

            ("cust1", "partner1", parse("2022-02-15"), "loc2", "ord3"),
            ("cust2", "partner1", parse("2022-02-15"), "loc1", "ord4"),
            ("cust1", "partner2", parse("2022-02-15"), "loc1", "ord5"),

            // made 2 orders, one of them more than 10 days ago, not a new customer
            ("cust1", "partner0", parse("2022-02-24"), "loc1", "ord6"),
            ("cust1", "partner0", parse("2022-01-01"), "loc1", "ord7"),

            // should be ignored, 'ord_date' is out of the timeframe
            ("cust2", "partner0", parse("2022-02-25"), "loc1", "ord8"),

            // should be ignored, 'ord_date' is out of the timeframe
            ("cust3", "partner0", parse("2022-02-14"), "loc1", "ord3")
        ).toDF("customer_360_id", "cxi_partner_id", "ord_date", "location_id", "ord_id")


        // when
        val actual = NewCustomerService().getNewCustomersPerPartnerAndLocation(customer360orders, "2022-02-24", 10)

        // then
        withClue("New customers per partner and location do not match") {
            val expected = Seq(
                ("cust1", "partner1", "loc1", CustomerLoyaltyType.New.value),
                ("cust1", "partner2", "loc1", CustomerLoyaltyType.New.value),
                ("cust2", "partner1", "loc1", CustomerLoyaltyType.New.value),
                ("cust1", "partner1", "loc2", CustomerLoyaltyType.New.value)
            ).toDF("customer_360_id", "cxi_partner_id", "location_id", "loyalty_type")
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("Get new customers per partner for all locations") {

        // given
        import spark.implicits._
        val customer360orders = Seq(
            // made 2 orders, should be considered only once
            ("cust1", "partner1", parse("2022-02-24"), "loc1", "ord1"),
            ("cust1", "partner1", parse("2022-02-23"), "loc1", "ord2"),

            // made 2 orders in different locations, should be considered only once
            ("cust2", "partner1", parse("2022-02-15"), "loc2", "ord3"),
            ("cust2", "partner1", parse("2022-02-15"), "loc1", "ord4"),

            ("cust3", "partner1", parse("2022-02-15"), "loc1", "ord5"),
            ("cust2", "partner2", parse("2022-02-15"), "loc1", "ord6"),
            ("cust1", "partner2", parse("2022-02-15"), "loc1", "ord7"),

            // made 2 orders, one of them more than 10 days ago, not a new customer
            ("cust1", "partner0", parse("2022-02-24"), "loc1", "ord8"),
            ("cust1", "partner0", parse("2022-01-01"), "loc1", "ord9"),

            // should be ignored, 'ord_date' is out of the timeframe
            ("cust2", "partner0", parse("2022-02-25"), "loc1", "ord10"),

            // should be ignored, 'ord_date' is out of the timeframe
            ("cust3", "partner0", parse("2022-02-14"), "loc1", "ord3")

        ).toDF("customer_360_id", "cxi_partner_id", "ord_date", "location_id", "ord_id")

        // when
        val actual = NewCustomerService().getNewCustomersPerPartnerForAllLocations(customer360orders, "2022-02-24", 10)

        // then
        withClue("New customers per partner for all locations do not match") {
            val expected = Seq(
                ("cust1", "partner1", AllLocationsAlias, CustomerLoyaltyType.New.value),
                ("cust1", "partner2", AllLocationsAlias, CustomerLoyaltyType.New.value),
                ("cust2", "partner1", AllLocationsAlias, CustomerLoyaltyType.New.value),
                ("cust2", "partner2", AllLocationsAlias, CustomerLoyaltyType.New.value),
                ("cust3", "partner1", AllLocationsAlias, CustomerLoyaltyType.New.value)
            ).toDF("customer_360_id", "cxi_partner_id", "location_id", "loyalty_type")
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

}
