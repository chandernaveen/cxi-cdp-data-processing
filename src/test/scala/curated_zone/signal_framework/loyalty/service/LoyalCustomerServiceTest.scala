package com.cxi.cdp.data_processing
package curated_zone.signal_framework.loyalty.service

import curated_zone.model.CustomerLoyaltyType
import curated_zone.signal_framework.loyalty.service.LoyaltyFunctions.AllLocationsAlias
import curated_zone.signal_framework.loyalty.LoyaltyTypeSignalsJob.LoyaltyTypeSignalName
import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

import java.time.LocalDate.parse

class LoyalCustomerServiceTest extends BaseSparkBatchJobTest {

    test("Loyal customers per partner and location") {

        // given
        import spark.implicits._
        val customer360orders = Seq(
            // partner1, loc1
            ("cust1", "partner1", parse("2021-12-21"), "loc1", "ord11", 100.0),
            ("cust1", "partner1", parse("2022-02-15"), "loc1", "ord12", 70.0),
            ("cust1", "partner1", parse("2022-03-20"), "loc1", "ord13", 30.0),
            ("cust2", "partner1", parse("2021-12-21"), "loc1", "ord21", 90.0),
            ("cust2", "partner1", parse("2022-02-12"), "loc1", "ord22", 40.0),
            ("cust2", "partner1", parse("2022-03-19"), "loc1", "ord23", 50.0),
            ("cust3", "partner1", parse("2021-12-21"), "loc1", "ord31", 10.0),
            ("cust3", "partner1", parse("2022-02-12"), "loc1", "ord32", 20.0),
            ("cust3", "partner1", parse("2022-03-11"), "loc1", "ord33", 5.0),

            // partner2
            ("cust1", "partner2", parse("2021-12-21"), "loc1", "ord11", 10.0),
            ("cust1", "partner2", parse("2022-02-15"), "loc1", "ord12", 1.0),
            ("cust1", "partner2", parse("2022-03-20"), "loc1", "ord13", 1.0),

            // partner1, loc2 - not loyal, txn date is out of timeframe
            ("cust1", "partner1", parse("2021-12-20"), "loc2", "ord11", 10.0),
            ("cust1", "partner1", parse("2022-02-15"), "loc2", "ord12", 1.0),
            ("cust1", "partner1", parse("2022-03-20"), "loc2", "ord13", 1.0)
        ).toDF("customer_360_id", "cxi_partner_id", "ord_date", "location_id", "ord_id", "ord_pay_total")

        // when
        // month1: 2021-12-21...2022-01-19, month2: 2022-01-20...2022-02-18, month3: 2022-02-19...2022-03-20
        val actual =
            LoyalCustomerService().getLoyalCustomersPerPartnerAndLocation(customer360orders, "2022-03-20", 90, 0.5)

        // then
        withClue("Loyal customers do not match") {
            val expected = Seq(
                ("cust1", "loc1", "partner1", CustomerLoyaltyType.Loyal.value),
                ("cust2", "loc1", "partner1", CustomerLoyaltyType.Loyal.value),
                ("cust1", "loc1", "partner2", CustomerLoyaltyType.Loyal.value)
            ).toDF("customer_360_id", "location_id", "cxi_partner_id", LoyaltyTypeSignalName)
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("Loyal customers per partner for all locations") {

        // given
        import spark.implicits._
        val customer360orders = Seq(
            // partner1
            ("cust1", "partner1", parse("2021-12-21"), "loc1", "ord11", 100.0),
            ("cust1", "partner1", parse("2022-02-15"), "loc2", "ord12", 70.0),
            ("cust1", "partner1", parse("2022-03-20"), "loc3", "ord13", 30.0),
            ("cust2", "partner1", parse("2021-12-21"), "loc4", "ord21", 90.0),
            ("cust2", "partner1", parse("2022-02-12"), "loc1", "ord22", 40.0),
            ("cust2", "partner1", parse("2022-03-19"), "loc2", "ord23", 50.0),
            ("cust3", "partner1", parse("2021-12-21"), "loc5", "ord31", 10.0),
            ("cust3", "partner1", parse("2022-02-12"), "loc3", "ord32", 20.0),
            ("cust3", "partner1", parse("2022-03-11"), "loc3", "ord33", 5.0),

            // partner2
            ("cust1", "partner2", parse("2021-12-21"), "loc1", "ord11", 10.0),
            ("cust1", "partner2", parse("2022-02-15"), "loc1", "ord12", 1.0),
            ("cust1", "partner2", parse("2022-03-20"), "loc1", "ord13", 1.0),

            // partner3 - not loyal, txn date is out of timeframe
            ("cust1", "partner1", parse("2021-12-20"), "loc2", "ord11", 10.0),
            ("cust1", "partner1", parse("2022-02-15"), "loc2", "ord12", 1.0),
            ("cust1", "partner1", parse("2022-03-20"), "loc2", "ord13", 1.0)
        ).toDF("customer_360_id", "cxi_partner_id", "ord_date", "location_id", "ord_id", "ord_pay_total")

        // when
        // month1: 2021-12-21...2022-01-19, month2: 2022-01-20...2022-02-18, month3: 2022-02-19...2022-03-20
        val actual =
            LoyalCustomerService().getLoyalCustomersPerPartnerForAllLocations(customer360orders, "2022-03-20", 90, 0.5)

        // then
        withClue("Loyal customers do not match") {
            val expected = Seq(
                ("cust1", AllLocationsAlias, "partner1", CustomerLoyaltyType.Loyal.value),
                ("cust2", AllLocationsAlias, "partner1", CustomerLoyaltyType.Loyal.value),
                ("cust1", AllLocationsAlias, "partner2", CustomerLoyaltyType.Loyal.value)
            ).toDF("customer_360_id", "location_id", "cxi_partner_id", LoyaltyTypeSignalName)
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

}
