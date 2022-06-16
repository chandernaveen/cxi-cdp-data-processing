package com.cxi.cdp.data_processing
package curated_zone.signal_framework.loyalty.service

import curated_zone.model.{CustomerLoyaltyType, CustomerMetricsTimePeriod}
import curated_zone.signal_framework.loyalty.service.LoyaltyFunctions.AllLocationsAlias
import curated_zone.signal_framework.loyalty.LoyaltyTypeSignalsJob.LoyaltyConfig
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.DataFrame
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.when
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}
import org.scalatestplus.mockito.MockitoSugar.mock

import java.time.LocalDate.parse

class LoyaltyTypeSignalServiceTest extends BaseSparkBatchJobTest {

    test("Create loyalty type signals for specific end date") {
        // given
        import spark.implicits._
        val customer360orders = Seq(
            // customer360orders content doesn't matter in this test
            ("cust1", "partner1", parse("2021-11-22"), "loc1", "ord11", 100.0)
        ).toDF("customer_360_id", "cxi_partner_id", "ord_date", "location_id", "ord_id", "ord_pay_total")

        val newCustomerService = mock[NewCustomerService]
        val atRiskCustomerService = mock[AtRiskCustomerService]
        val loyalCustomerService = mock[LoyalCustomerService]
        val regularCustomerService = mock[RegularCustomerService]

        val loyaltyCfg = LoyaltyConfig(5, 10, 14, 18, 0.5)
        when(
            newCustomerService.getNewCustomersPerPartnerAndLocation(
                ArgumentMatchers.eq(customer360orders),
                ArgumentMatchers.eq("2022-02-17"),
                ArgumentMatchers.eq(loyaltyCfg.newCustomerTimeframeDays)
            )
        )
            .thenReturn(
                Seq(("cust1", "partner1", "loc1", CustomerLoyaltyType.New.value))
                    .toDF("customer_360_id", "cxi_partner_id", "location_id", "loyalty_type")
            )

        when(
            newCustomerService.getNewCustomersPerPartnerForAllLocations(
                ArgumentMatchers.eq(customer360orders),
                ArgumentMatchers.eq("2022-02-17"),
                ArgumentMatchers.eq(loyaltyCfg.newCustomerTimeframeDays)
            )
        )
            .thenReturn(
                Seq(("cust2", "partner3", AllLocationsAlias, CustomerLoyaltyType.New.value))
                    .toDF("customer_360_id", "cxi_partner_id", "location_id", "loyalty_type")
            )

        val loyalPerLocation = Seq(("cust5", "partner2", "loc3", CustomerLoyaltyType.Loyal.value))
            .toDF("customer_360_id", "cxi_partner_id", "location_id", "loyalty_type")
        when(
            loyalCustomerService.getLoyalCustomersPerPartnerAndLocation(
                ArgumentMatchers.eq(customer360orders),
                ArgumentMatchers.eq("2022-02-17"),
                ArgumentMatchers.eq(loyaltyCfg.loyalCustomerTimeframeDays),
                ArgumentMatchers.eq(loyaltyCfg.rfmThreshold)
            )
        )
            .thenReturn(loyalPerLocation)

        val loyalAllLocations = Seq(("cust6", "partner3", AllLocationsAlias, CustomerLoyaltyType.Loyal.value))
            .toDF("customer_360_id", "cxi_partner_id", "location_id", "loyalty_type")
        when(
            loyalCustomerService.getLoyalCustomersPerPartnerForAllLocations(
                ArgumentMatchers.eq(customer360orders),
                ArgumentMatchers.eq("2022-02-17"),
                ArgumentMatchers.eq(loyaltyCfg.loyalCustomerTimeframeDays),
                ArgumentMatchers.eq(loyaltyCfg.rfmThreshold)
            )
        )
            .thenReturn(loyalAllLocations)

        when(
            atRiskCustomerService.getAtRiskCustomersPerPartnerAndLocation(
                ArgumentMatchers.eq(loyalPerLocation),
                ArgumentMatchers.eq(customer360orders),
                ArgumentMatchers.eq("2022-02-17"),
                ArgumentMatchers.eq(loyaltyCfg)
            )
        )
            .thenReturn(
                Seq(("cust3", "partner1", "loc2", CustomerLoyaltyType.AtRisk.value))
                    .toDF("customer_360_id", "cxi_partner_id", "location_id", "loyalty_type")
            )

        when(
            atRiskCustomerService.getAtRiskCustomersPerPartnerForAllLocations(
                ArgumentMatchers.eq(loyalAllLocations),
                ArgumentMatchers.eq(customer360orders),
                ArgumentMatchers.eq("2022-02-17"),
                ArgumentMatchers.eq(loyaltyCfg)
            )
        )
            .thenReturn(
                Seq(("cust4", "partner2", AllLocationsAlias, CustomerLoyaltyType.AtRisk.value))
                    .toDF("customer_360_id", "cxi_partner_id", "location_id", "loyalty_type")
            )

        when(
            regularCustomerService.getRegularCustomersPerPartnerAndLocation(
                ArgumentMatchers.eq(customer360orders),
                ArgumentMatchers.any[DataFrame],
                ArgumentMatchers.eq("2022-02-17"),
                ArgumentMatchers.eq(loyaltyCfg.regularCustomerTimeframeDays)
            )
        )
            .thenReturn(
                Seq(("cust7", "partner4", "loc1", CustomerLoyaltyType.Regular.value))
                    .toDF("customer_360_id", "cxi_partner_id", "location_id", "loyalty_type")
            )

        when(
            regularCustomerService.getRegularCustomersPerPartnerAllLocations(
                ArgumentMatchers.eq(customer360orders),
                ArgumentMatchers.any[DataFrame],
                ArgumentMatchers.eq("2022-02-17"),
                ArgumentMatchers.eq(loyaltyCfg.regularCustomerTimeframeDays)
            )
        )
            .thenReturn(
                Seq(("cust8", "partner1", AllLocationsAlias, CustomerLoyaltyType.Regular.value))
                    .toDF("customer_360_id", "cxi_partner_id", "location_id", "loyalty_type")
            )

        // when
        val actual = LoyaltyTypeSignalService(
            newCustomerService,
            atRiskCustomerService,
            loyalCustomerService,
            regularCustomerService
        )
            .getLoyaltyTypesForTimePeriod(
                customer360orders,
                CustomerMetricsTimePeriod.Period7days,
                "2022-02-24",
                loyaltyCfg
            )

        // then
        withClue("Loyalty type signals do not match") {
            val expected = Seq(
                ("cust1", "partner1", "loc1", CustomerLoyaltyType.New.value, "time_period_7"),
                ("cust2", "partner3", AllLocationsAlias, CustomerLoyaltyType.New.value, "time_period_7"),
                ("cust3", "partner1", "loc2", CustomerLoyaltyType.AtRisk.value, "time_period_7"),
                ("cust4", "partner2", AllLocationsAlias, CustomerLoyaltyType.AtRisk.value, "time_period_7"),
                ("cust5", "partner2", "loc3", CustomerLoyaltyType.Loyal.value, "time_period_7"),
                ("cust6", "partner3", AllLocationsAlias, CustomerLoyaltyType.Loyal.value, "time_period_7"),
                ("cust7", "partner4", "loc1", CustomerLoyaltyType.Regular.value, "time_period_7"),
                ("cust8", "partner1", AllLocationsAlias, CustomerLoyaltyType.Regular.value, "time_period_7")
            ).toDF("customer_360_id", "cxi_partner_id", "location_id", "signal_value", "date_option")
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

}
