package com.cxi.cdp.data_processing
package curated_zone.signal_framework.loyalty.service

import curated_zone.model.CustomerLoyaltyType
import curated_zone.signal_framework.loyalty.service
import curated_zone.signal_framework.loyalty.service.LoyaltyFunctions.AllLocationsAlias
import curated_zone.signal_framework.loyalty.LoyaltyTypeSignalsJob.{LoyaltyConfig, LoyaltyTypeSignalName}
import support.BaseSparkBatchJobTest

import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}
import org.scalatestplus.mockito.MockitoSugar.mock

import java.time.LocalDate.parse

class AtRiskCustomerServiceTest extends BaseSparkBatchJobTest {

    test("At risk customers per partner and location (integration with LoyalCustomerService)") {

        // given
        import spark.implicits._
        val customer360orders = Seq(
            // previously loyal, currently not loyal - at risk
            ("cust1", "partner1", parse("2021-11-22"), "loc1", "ord11", 100.0),
            ("cust1", "partner1", parse("2021-12-23"), "loc1", "ord12", 70.0),
            ("cust1", "partner1", parse("2022-01-31"), "loc1", "ord13", 30.0),
            ("cust1", "partner1", parse("2021-11-22"), "loc1", "ord111", 100.0),
            ("cust1", "partner1", parse("2021-12-23"), "loc1", "ord121", 70.0),
            ("cust1", "partner1", parse("2022-01-31"), "loc1", "ord131", 30.0),

            // previously loyal, currently loyal - not at risk
            ("cust2", "partner1", parse("2021-11-22"), "loc1", "ord21", 90.0),
            ("cust2", "partner1", parse("2021-12-23"), "loc1", "ord22", 40.0),
            ("cust2", "partner1", parse("2022-01-31"), "loc1", "ord23", 50.0),
            ("cust2", "partner1", parse("2022-01-19"), "loc1", "ord21", 100.0),
            ("cust2", "partner1", parse("2022-02-18"), "loc1", "ord22", 70.0),
            ("cust2", "partner1", parse("2022-03-20"), "loc1", "ord23", 30.0),

            // previously not loyal, currently not loyal - not at risk
            ("cust3", "partner1", parse("2021-11-22"), "loc1", "ord31", 10.0),
            ("cust3", "partner1", parse("2021-12-23"), "loc1", "ord32", 20.0),
            ("cust3", "partner1", parse("2022-01-31"), "loc1", "ord33", 5.0),
            ("cust3", "partner1", parse("2022-01-19"), "loc1", "ord321", 1.0),
            ("cust3", "partner1", parse("2022-02-18"), "loc1", "ord322", 1.0),
            ("cust3", "partner1", parse("2022-03-30"), "loc1", "ord323", 1.0),

            // previously didn't exist, currently loyal - not at risk
            ("cust4", "partner4", parse("2022-01-19"), "loc4", "ord21", 100.0),
            ("cust4", "partner4", parse("2022-02-18"), "loc4", "ord22", 70.0),
            ("cust4", "partner4", parse("2022-03-20"), "loc4", "ord23", 30.0)
        ).toDF("customer_360_id", "cxi_partner_id", "ord_date", "location_id", "ord_id", "ord_pay_total")

        val loyaltyCfg = LoyaltyConfig(30, 90, 30, 120, 0.5)
        val loyalCustomerService = LoyalCustomerService()
        val endDate = "2022-03-20"

        // when
        // current loyalty timeframe: 2021-12-21...2022-03-20
        // previous loyalty timeframe: 2021-11-22...2022-02-19
        val loyalCustomers = loyalCustomerService.getLoyalCustomersPerPartnerAndLocation(
            customer360orders,
            endDate,
            loyaltyCfg.loyalCustomerTimeframeDays,
            loyaltyCfg.rfmThreshold
        )
        val actual = AtRiskCustomerService(loyalCustomerService)
            .getAtRiskCustomersPerPartnerAndLocation(loyalCustomers, customer360orders, endDate, loyaltyCfg)

        // then
        withClue("Loyal customers do not match") {
            val expected = Seq(
                ("cust2", "loc1", "partner1", CustomerLoyaltyType.Loyal.value),
                ("cust4", "loc4", "partner4", CustomerLoyaltyType.Loyal.value)
            ).toDF("customer_360_id", "location_id", "cxi_partner_id", LoyaltyTypeSignalName)
            loyalCustomers.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            loyalCustomers.collect() should contain theSameElementsAs expected.collect()
        }
        withClue("At risk customers do not match") {
            val expected = Seq(
                ("cust1", "loc1", "partner1", CustomerLoyaltyType.AtRisk.value)
            ).toDF("customer_360_id", "location_id", "cxi_partner_id", LoyaltyTypeSignalName)
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("At risk customers per partner for all locations  (integration with LoyalCustomerService)") {

        // given
        import spark.implicits._
        val customer360orders = Seq(
            // previously loyal, currently not loyal - at risk
            ("cust1", "partner1", parse("2021-11-22"), "loc1", "ord11", 100.0),
            ("cust1", "partner1", parse("2021-12-23"), "loc2", "ord12", 70.0),
            ("cust1", "partner1", parse("2022-01-31"), "loc3", "ord13", 30.0),
            ("cust1", "partner1", parse("2021-11-22"), "loc4", "ord111", 100.0),
            ("cust1", "partner1", parse("2021-12-23"), "loc1", "ord121", 70.0),
            ("cust1", "partner1", parse("2022-01-31"), "loc3", "ord131", 30.0),

            // previously loyal, currently loyal - not at risk
            ("cust2", "partner1", parse("2021-11-22"), "loc2", "ord21", 90.0),
            ("cust2", "partner1", parse("2021-12-23"), "loc6", "ord22", 40.0),
            ("cust2", "partner1", parse("2022-01-31"), "loc7", "ord23", 50.0),
            ("cust2", "partner1", parse("2022-01-19"), "loc3", "ord21", 100.0),
            ("cust2", "partner1", parse("2022-02-18"), "loc4", "ord22", 70.0),
            ("cust2", "partner1", parse("2022-03-20"), "loc1", "ord23", 30.0),

            // previously not loyal, currently not loyal - not at risk
            ("cust3", "partner1", parse("2021-11-22"), "loc8", "ord31", 10.0),
            ("cust3", "partner1", parse("2021-12-23"), "loc5", "ord32", 20.0),
            ("cust3", "partner1", parse("2022-01-31"), "loc3", "ord33", 5.0),
            ("cust3", "partner1", parse("2022-01-19"), "loc1", "ord321", 1.0),
            ("cust3", "partner1", parse("2022-02-18"), "loc1", "ord322", 1.0),
            ("cust3", "partner1", parse("2022-03-30"), "loc7", "ord323", 1.0),

            // previously didn't exist, currently loyal - not at risk
            ("cust4", "partner4", parse("2022-01-19"), "loc4", "ord21", 100.0),
            ("cust4", "partner4", parse("2022-02-18"), "loc4", "ord22", 70.0),
            ("cust4", "partner4", parse("2022-03-20"), "loc4", "ord23", 30.0)
        ).toDF("customer_360_id", "cxi_partner_id", "ord_date", "location_id", "ord_id", "ord_pay_total")

        val loyaltyCfg = LoyaltyConfig(30, 90, 30, 120, 0.5)
        val loyalCustomerService = LoyalCustomerService()
        val endDate = "2022-03-20"

        // when
        // current loyalty timeframe: 2021-12-21...2022-03-20
        // previous loyalty timeframe: 2021-11-22...2022-02-19
        val loyalCustomers = loyalCustomerService.getLoyalCustomersPerPartnerForAllLocations(
            customer360orders,
            endDate,
            loyaltyCfg.loyalCustomerTimeframeDays,
            loyaltyCfg.rfmThreshold
        )
        val actual = AtRiskCustomerService(loyalCustomerService)
            .getAtRiskCustomersPerPartnerForAllLocations(loyalCustomers, customer360orders, endDate, loyaltyCfg)

        // then
        withClue("Loyal customers do not match") {
            val expected = Seq(
                ("cust2", AllLocationsAlias, "partner1", CustomerLoyaltyType.Loyal.value),
                ("cust4", AllLocationsAlias, "partner4", CustomerLoyaltyType.Loyal.value)
            ).toDF("customer_360_id", "location_id", "cxi_partner_id", LoyaltyTypeSignalName)
            loyalCustomers.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            loyalCustomers.collect() should contain theSameElementsAs expected.collect()
        }
        withClue("At risk customers do not match") {
            val expected = Seq(
                ("cust1", "partner1", AllLocationsAlias, CustomerLoyaltyType.AtRisk.value)
            ).toDF("customer_360_id", "cxi_partner_id", "location_id", LoyaltyTypeSignalName)
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("At risk customers per partner and location") {

        // given
        import spark.implicits._
        val customer360orders = Seq(
            // customer360orders content doesn't matter in this test
            ("cust1", "partner1", parse("2021-11-22"), "loc1", "ord11", 100.0)
        ).toDF("customer_360_id", "cxi_partner_id", "ord_date", "location_id", "ord_id", "ord_pay_total")

        val loyalCustomerService = mock[LoyalCustomerService]
        val prevLoyal = Seq(
            ("cust1", "loc1", "partner1", CustomerLoyaltyType.Loyal.value),
            ("cust2", "loc2", "partner1", CustomerLoyaltyType.Loyal.value),
            ("cust2", "loc3", "partner2", CustomerLoyaltyType.Loyal.value),
            ("cust1", "loc1", "partner2", CustomerLoyaltyType.Loyal.value),
            ("cust1", "loc2", "partner1", CustomerLoyaltyType.Loyal.value)
        ).toDF("customer_360_id", "location_id", "cxi_partner_id", LoyaltyTypeSignalName)
        val currLoyal = Seq(
            ("cust1", "loc1", "partner2", CustomerLoyaltyType.Loyal.value),
            ("cust2", "loc2", "partner2", CustomerLoyaltyType.Loyal.value),
            ("cust3", "loc1", "partner1", CustomerLoyaltyType.Loyal.value)
        ).toDF("customer_360_id", "location_id", "cxi_partner_id", LoyaltyTypeSignalName)

        when(
            loyalCustomerService.getLoyalCustomersPerPartnerAndLocation(
                ArgumentMatchers.eq(customer360orders),
                any[String],
                ArgumentMatchers.eq(90),
                ArgumentMatchers.eq(0.5)
            )
        )
            .thenReturn(prevLoyal)

        val loyaltyCfg = LoyaltyConfig(30, 90, 30, 120, 0.5)

        // when
        val actual = new service.AtRiskCustomerService(loyalCustomerService).getAtRiskCustomersPerPartnerAndLocation(
            currLoyal,
            customer360orders,
            "2022-03-20",
            loyaltyCfg
        )

        // then
        withClue("At risk customers do not match") {
            val expected = Seq(
                ("cust1", "loc1", "partner1", CustomerLoyaltyType.AtRisk.value),
                ("cust2", "loc2", "partner1", CustomerLoyaltyType.AtRisk.value),
                ("cust2", "loc3", "partner2", CustomerLoyaltyType.AtRisk.value),
                ("cust1", "loc2", "partner1", CustomerLoyaltyType.AtRisk.value)
            ).toDF("customer_360_id", "location_id", "cxi_partner_id", LoyaltyTypeSignalName)
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("At risk customers per partner for all locations") {

        // given
        import spark.implicits._
        val customer360orders = Seq(
            // customer360orders content doesn't matter in this test
            ("cust1", "partner1", parse("2021-11-22"), "loc1", "ord11", 100.0)
        ).toDF("customer_360_id", "cxi_partner_id", "ord_date", "location_id", "ord_id", "ord_pay_total")

        val loyalCustomerService = mock[LoyalCustomerService]
        val prevLoyal = Seq(
            ("cust1", AllLocationsAlias, "partner1", CustomerLoyaltyType.Loyal.value),
            ("cust2", AllLocationsAlias, "partner1", CustomerLoyaltyType.Loyal.value),
            ("cust1", AllLocationsAlias, "partner2", CustomerLoyaltyType.Loyal.value),
            ("cust2", AllLocationsAlias, "partner2", CustomerLoyaltyType.Loyal.value),
            ("cust3", AllLocationsAlias, "partner3", CustomerLoyaltyType.Loyal.value)
        ).toDF("customer_360_id", "location_id", "cxi_partner_id", LoyaltyTypeSignalName)
        val currLoyal = Seq(
            ("cust1", AllLocationsAlias, "partner1", CustomerLoyaltyType.Loyal.value),
            ("cust2", AllLocationsAlias, "partner2", CustomerLoyaltyType.Loyal.value),
            ("cust3", AllLocationsAlias, "partner1", CustomerLoyaltyType.Loyal.value)
        ).toDF("customer_360_id", "location_id", "cxi_partner_id", LoyaltyTypeSignalName)

        when(
            loyalCustomerService.getLoyalCustomersPerPartnerForAllLocations(
                ArgumentMatchers.eq(customer360orders),
                any[String],
                ArgumentMatchers.eq(90),
                ArgumentMatchers.eq(0.5)
            )
        )
            .thenReturn(prevLoyal)

        val loyaltyCfg = LoyaltyConfig(30, 90, 30, 120, 0.5)

        // when
        val actual = new service.AtRiskCustomerService(loyalCustomerService)
            .getAtRiskCustomersPerPartnerForAllLocations(currLoyal, customer360orders, "2022-03-20", loyaltyCfg)

        // then
        withClue("At risk customers do not match") {
            val expected = Seq(
                ("cust2", "partner1", AllLocationsAlias, CustomerLoyaltyType.AtRisk.value),
                ("cust1", "partner2", AllLocationsAlias, CustomerLoyaltyType.AtRisk.value),
                ("cust3", "partner3", AllLocationsAlias, CustomerLoyaltyType.AtRisk.value)
            ).toDF("customer_360_id", "cxi_partner_id", "location_id", LoyaltyTypeSignalName)
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

}
