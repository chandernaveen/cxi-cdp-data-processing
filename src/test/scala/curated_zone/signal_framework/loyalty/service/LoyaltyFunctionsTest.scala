package com.cxi.cdp.data_processing
package curated_zone.signal_framework.loyalty.service

import curated_zone.signal_framework.loyalty.service.LoyaltyFunctions.AllLocationsAlias
import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

import java.time.LocalDate.parse

class LoyaltyFunctionsTest extends BaseSparkBatchJobTest {

    test("Recency score per partner and location") {
        // given
        import spark.implicits._
        val customer360orders = Seq(
            // partner1, loc1
            // the least recent order, recency = 0.0
            ("cust1", "partner1", parse("2022-02-10"), "loc1"),

            // only the last order is considered, recency = 0.6
            ("cust2", "partner1", parse("2022-02-14"), "loc1"),
            ("cust2", "partner1", parse("2022-02-16"), "loc1"),

            // the most recent order, recency = 1.0
            ("cust3", "partner1", parse("2022-02-20"), "loc1"),

            // recency = 0.7
            ("cust6", "partner1", parse("2022-02-17"), "loc1"),

            // partner1, loc2
            // the least recent order, recency = 0.0
            ("cust7", "partner1", parse("2022-02-12"), "loc2"),
            // the most recent order, recency = 1.0
            ("cust8", "partner1", parse("2022-02-16"), "loc2"),
            // recency = 0.75
            ("cust9", "partner1", parse("2022-02-15"), "loc2"),

            // single order per partner and location, recency = 1.0
            ("cust10", "partner2", parse("2022-02-15"), "loc1"),

            // partner3, loc1
            // the least recent order, recency = 0.0
            ("cust1", "partner3", parse("2022-02-10"), "loc1"),

            // only the last order is considered, recency = 0.6
            ("cust2", "partner3", parse("2022-02-14"), "loc1"),
            ("cust2", "partner3", parse("2022-02-16"), "loc1"),

            // the most recent order, recency = 1.0
            ("cust3", "partner3", parse("2022-02-20"), "loc1")
        ).toDF("customer_360_id", "cxi_partner_id", "ord_date", "location_id")

        // when
        val actual = LoyaltyFunctions.recencyScorePerPartnerAndLocation(customer360orders, "2022-02-10")

        // then
        withClue("Recency score per partner and locations does not match") {
            val expected = Seq(
                ("cust1", "loc1", "partner1", 0.0),
                ("cust2", "loc1", "partner1", 0.6),
                ("cust3", "loc1", "partner1", 1.0),
                ("cust6", "loc1", "partner1", 0.7),
                ("cust7", "loc2", "partner1", 0.0),
                ("cust8", "loc2", "partner1", 1.0),
                ("cust9", "loc2", "partner1", 0.75),
                ("cust10", "loc1", "partner2", 1.0),
                ("cust1", "loc1", "partner3", 0.0),
                ("cust2", "loc1", "partner3", 0.6),
                ("cust3", "loc1", "partner3", 1.0)
            ).toDF("customer_360_id", "location_id", "cxi_partner_id", "recency_score")
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("Recency score per partner for all locations") {
        // given
        import spark.implicits._
        val customer360orders = Seq(
            // partner1
            // the least recent order, recency = 0.0
            ("cust1", "partner1", parse("2022-02-10"), "loc1"),

            // only the last order is considered, recency = 0.6
            ("cust2", "partner1", parse("2022-02-14"), "loc1"),
            ("cust2", "partner1", parse("2022-02-16"), "loc2"),

            // the most recent order, recency = 1.0
            ("cust3", "partner1", parse("2022-02-20"), "loc3"),

            // recency = 0.7
            ("cust4", "partner1", parse("2022-02-17"), "loc5"),

            // partner2, single order, recency = 1.0
            ("cust1", "partner2", parse("2022-02-15"), "loc1"),

            // partner3
            // the least recent order, recency = 0.0
            ("cust1", "partner3", parse("2022-02-10"), "loc1"),

            // only the last order is considered, recency = 0.6
            ("cust2", "partner3", parse("2022-02-14"), "loc2"),
            ("cust2", "partner3", parse("2022-02-16"), "loc2"),

            // the most recent order, recency = 1.0
            ("cust3", "partner3", parse("2022-02-20"), "loc3")
        ).toDF("customer_360_id", "cxi_partner_id", "ord_date", "location_id")

        // when
        val actual = LoyaltyFunctions.recencyScorePerPartnerForAllLocations(customer360orders, "2022-02-10")

        // then
        withClue("Recency score per partner and locations does not match") {
            val expected = Seq(
                ("cust1", AllLocationsAlias, "partner1", 0.0),
                ("cust2", AllLocationsAlias, "partner1", 0.6),
                ("cust3", AllLocationsAlias, "partner1", 1.0),
                ("cust4", AllLocationsAlias, "partner1", 0.7),
                ("cust1", AllLocationsAlias, "partner2", 1.0),
                ("cust1", AllLocationsAlias, "partner3", 0.0),
                ("cust2", AllLocationsAlias, "partner3", 0.6),
                ("cust3", AllLocationsAlias, "partner3", 1.0)
            ).toDF("customer_360_id", "location_id", "cxi_partner_id", "recency_score")
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("Frequency score per partner and location") {
        // given
        import spark.implicits._
        val customer360orders = Seq(
            // partner1, loc1
            // 3 orders - the most frequent - 1.0
            ("cust1", "partner1", "loc1", "ord11"),
            ("cust1", "partner1", "loc1", "ord12"),
            ("cust1", "partner1", "loc1", "ord13"),
            // 2 orders - frequency 0.5
            ("cust2", "partner1", "loc1", "ord21"),
            ("cust2", "partner1", "loc1", "ord22"),
            // 1 order - the least frequent - 0.0
            ("cust3", "partner1", "loc1", "ord31"),

            // partner2, loc1
            // 2 orders - the most frequent - 1.0
            ("cust1", "partner2", "loc1", "ord11"),
            ("cust1", "partner2", "loc1", "ord12"),
            // 1 order - the least frequent - 0.0
            ("cust2", "partner2", "loc1", "ord21"),

            // single order per partner and location, recency = 1.0
            ("cust1", "partner2", "loc3", "ord21")
        ).toDF("customer_360_id", "cxi_partner_id", "location_id", "ord_id")

        // when
        val actual = LoyaltyFunctions.frequencyScorePerPartnerAndLocation(customer360orders)

        // then
        withClue("Frequency score per partner and locations does not match") {
            val expected = Seq(
                ("cust1", "loc1", "partner1", 1.0),
                ("cust2", "loc1", "partner1", 0.5),
                ("cust3", "loc1", "partner1", 0.0),
                ("cust1", "loc1", "partner2", 1.0),
                ("cust2", "loc1", "partner2", 0.0),
                ("cust1", "loc3", "partner2", 1.0)
            ).toDF("customer_360_id", "location_id", "cxi_partner_id", "frequency_score")
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("Frequency score per partner for all locations") {
        // given
        import spark.implicits._
        val customer360orders = Seq(
            // partner1
            // 3 orders - the most frequent - 1.0
            ("cust1", "partner1", "loc1", "ord11"),
            ("cust1", "partner1", "loc2", "ord12"),
            ("cust1", "partner1", "loc3", "ord13"),
            // 2 orders - frequency 0.5
            ("cust2", "partner1", "loc1", "ord21"),
            ("cust2", "partner1", "loc3", "ord22"),
            // 1 order - the least frequent - 0.0
            ("cust3", "partner1", "loc4", "ord31"),

            // partner2
            // 2 orders - the most frequent - 1.0
            ("cust1", "partner2", "loc1", "ord11"),
            ("cust1", "partner2", "loc1", "ord12"),
            // 1 order - the least frequent - 0.0
            ("cust2", "partner2", "loc2", "ord21"),

            // single order per partner, recency = 1.0
            ("cust1", "partner3", "loc1", "ord21")
        ).toDF("customer_360_id", "cxi_partner_id", "location_id", "ord_id")

        // when
        val actual = LoyaltyFunctions.frequencyScorePerPartnerForAllLocations(customer360orders)

        // then
        withClue("Frequency score per partner and locations does not match") {
            val expected = Seq(
                ("cust1", AllLocationsAlias, "partner1", 1.0),
                ("cust2", AllLocationsAlias, "partner1", 0.5),
                ("cust3", AllLocationsAlias, "partner1", 0.0),
                ("cust1", AllLocationsAlias, "partner2", 1.0),
                ("cust2", AllLocationsAlias, "partner2", 0.0),
                ("cust1", AllLocationsAlias, "partner3", 1.0)
            ).toDF("customer_360_id", "location_id", "cxi_partner_id", "frequency_score")
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("Monetary score per partner and location") {
        // given
        import spark.implicits._
        val customer360orders = Seq(
            // partner1, loc1
            // cust1 - total amount 25.0 - score 1.0
            ("cust1", "partner1", "loc1", "ord11", 15.5),
            ("cust1", "partner1", "loc1", "ord12", 9.5),
            // cust2 - total amount 15.0 - score 0.5
            ("cust2", "partner1", "loc1", "ord21", 8.75),
            ("cust2", "partner1", "loc1", "ord22", 6.25),
            // cust3 - total amount 5.0 - score 0.0
            ("cust3", "partner1", "loc1", "ord31", 5.0),

            // partner1, loc2
            // cust1 - total amount 100.0 - score 1.0
            ("cust1", "partner1", "loc2", "ord11", 100.0),
            // cust2 - total amount 60.0 - score 0.6
            ("cust2", "partner1", "loc2", "ord21", 60.0),
            // cust3 - total amount 0.0 - score 0.0
            ("cust3", "partner1", "loc2", "ord31", 0.0),

            // partner2, loc1
            // cust1 - total amount 60.0 - score 0.25
            ("cust1", "partner2", "loc1", "ord11", 25.0),
            ("cust1", "partner2", "loc1", "ord12", 35.0),
            // cust2 - total amount 120.0 - score 1.0
            ("cust2", "partner2", "loc1", "ord21", 120.0),
            // cust3 - total amount 40.0 - score 0.0
            ("cust3", "partner2", "loc1", "ord31", 25.0),
            ("cust3", "partner2", "loc1", "ord31", 15.0),

            // partner3, loc1
            ("cust1", "partner3", "loc1", "ord31", 15.0)
        ).toDF("customer_360_id", "cxi_partner_id", "location_id", "ord_id", "ord_pay_total")

        // when
        val actual = LoyaltyFunctions.monetaryScorePerPartnerAndLocation(customer360orders)

        // then
        withClue("Monetary score per partner and locations does not match") {
            val expected = Seq(
                ("cust1", "loc1", "partner1", 1.0),
                ("cust2", "loc1", "partner1", 0.5),
                ("cust3", "loc1", "partner1", 0.0),
                ("cust1", "loc2", "partner1", 1.0),
                ("cust2", "loc2", "partner1", 0.6),
                ("cust3", "loc2", "partner1", 0.0),
                ("cust1", "loc1", "partner2", 0.25),
                ("cust2", "loc1", "partner2", 1.0),
                ("cust3", "loc1", "partner2", 0.0),
                ("cust1", "loc1", "partner3", 1.0)
            ).toDF("customer_360_id", "location_id", "cxi_partner_id", "monetary_score")
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("Monetary score per partner for all locations") {
        // given
        import spark.implicits._
        val customer360orders = Seq(
            // partner1
            // cust1 - total amount 25.0 - score 1.0
            ("cust1", "partner1", "loc1", "ord11", 15.5),
            ("cust1", "partner1", "loc2", "ord12", 9.5),
            // cust2 - total amount 15.0 - score 0.5
            ("cust2", "partner1", "loc2", "ord21", 8.75),
            ("cust2", "partner1", "loc1", "ord22", 6.25),
            // cust3 - total amount 5.0 - score 0.0
            ("cust3", "partner1", "loc3", "ord31", 5.0),

            // partner2
            // cust1 - total amount 100.0 - score 1.0
            ("cust1", "partner2", "loc2", "ord11", 100.0),
            // cust2 - total amount 60.0 - score 0.6
            ("cust2", "partner2", "loc1", "ord21", 60.0),
            // cust3 - total amount 0.0 - score 0.0
            ("cust3", "partner2", "loc3", "ord31", 0.0),

            // partner3
            // cust1 - total amount 60.0 - score 0.25
            ("cust1", "partner3", "loc1", "ord11", 25.0),
            ("cust1", "partner3", "loc2", "ord12", 35.0),
            // cust2 - total amount 120.0 - score 1.0
            ("cust2", "partner3", "loc3", "ord21", 120.0),
            // cust3 - total amount 40.0 - score 0.0
            ("cust3", "partner3", "loc4", "ord31", 25.0),
            ("cust3", "partner3", "loc1", "ord31", 15.0),

            // partner4
            ("cust1", "partner4", "loc1", "ord31", 15.0)
        ).toDF("customer_360_id", "cxi_partner_id", "location_id", "ord_id", "ord_pay_total")

        // when
        val actual = LoyaltyFunctions.monetaryScorePerPartnerForAllLocations(customer360orders)

        // then
        withClue("Monetary score per partner and locations does not match") {
            val expected = Seq(
                ("cust1", AllLocationsAlias, "partner1", 1.0),
                ("cust2", AllLocationsAlias, "partner1", 0.5),
                ("cust3", AllLocationsAlias, "partner1", 0.0),
                ("cust1", AllLocationsAlias, "partner2", 1.0),
                ("cust2", AllLocationsAlias, "partner2", 0.6),
                ("cust3", AllLocationsAlias, "partner2", 0.0),
                ("cust1", AllLocationsAlias, "partner3", 0.25),
                ("cust2", AllLocationsAlias, "partner3", 1.0),
                ("cust3", AllLocationsAlias, "partner3", 0.0),
                ("cust1", AllLocationsAlias, "partner4", 1.0)
            ).toDF("customer_360_id", "location_id", "cxi_partner_id", "monetary_score")
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("Loyalty indicator per partner and location") {
        // given
        import spark.implicits._
        val customer360orders = Seq(
            // partner1, loc1
            ("cust1", "partner1", parse("2021-12-25"), "loc1"),
            ("cust1", "partner1", parse("2022-01-25"), "loc1"),
            ("cust1", "partner1", parse("2022-03-15"), "loc1"),
            ("cust2", "partner1", parse("2021-12-21"), "loc1"),
            ("cust2", "partner1", parse("2022-01-20"), "loc1"),
            ("cust2", "partner1", parse("2022-02-19"), "loc1"),
            ("cust3", "partner1", parse("2022-01-19"), "loc1"),
            ("cust3", "partner1", parse("2022-02-18"), "loc1"),
            ("cust3", "partner1", parse("2022-03-20"), "loc1"),

            // partner1, loc2
            ("cust1", "partner1", parse("2022-01-20"), "loc2"),
            ("cust1", "partner1", parse("2022-01-25"), "loc2"),
            ("cust1", "partner1", parse("2022-03-15"), "loc2"),
            ("cust2", "partner1", parse("2022-01-25"), "loc2"),
            ("cust2", "partner1", parse("2022-03-15"), "loc2"),

            // partner3, loc1
            ("cust1", "partner3", parse("2022-01-19"), "loc1"),
            ("cust1", "partner3", parse("2022-01-20"), "loc1"),
            ("cust1", "partner3", parse("2022-03-20"), "loc1"),
            ("cust2", "partner3", parse("2022-01-19"), "loc1"),
            ("cust2", "partner3", parse("2022-01-18"), "loc1"),
            ("cust2", "partner3", parse("2022-03-20"), "loc1")
        ).toDF("customer_360_id", "cxi_partner_id", "ord_date", "location_id")

        // when
        // month1: 2021-12-21...2022-01-19, month2: 2022-01-20...2022-02-18, month3: 2022-02-19...2022-03-20
        val actual = LoyaltyFunctions.loyalIndicatorPerPartnerAndLocation(customer360orders, "2022-03-20")

        // then
        withClue("Frequency score per partner and locations does not match") {
            val expected = Seq(
                ("cust1", "loc1", "partner1", 1),
                ("cust2", "loc1", "partner1", 1),
                ("cust3", "loc1", "partner1", 1),
                ("cust1", "loc2", "partner1", 0),
                ("cust2", "loc2", "partner1", 0),
                ("cust1", "loc1", "partner3", 1),
                ("cust2", "loc1", "partner3", 0)
            ).toDF("customer_360_id", "location_id", "cxi_partner_id", "txn_all_months")
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("Loyalty indicator per partner for all locations") {
        // given
        import spark.implicits._
        val customer360orders = Seq(
            // partner1
            ("cust1", "partner1", parse("2021-12-25"), "loc1"),
            ("cust1", "partner1", parse("2022-01-25"), "loc2"),
            ("cust1", "partner1", parse("2022-03-15"), "loc3"),
            ("cust2", "partner1", parse("2021-12-21"), "loc1"),
            ("cust2", "partner1", parse("2022-01-20"), "loc5"),
            ("cust2", "partner1", parse("2022-02-19"), "loc6"),
            ("cust3", "partner1", parse("2022-01-19"), "loc2"),
            ("cust3", "partner1", parse("2022-02-18"), "loc3"),
            ("cust3", "partner1", parse("2022-03-20"), "loc3"),

            // partner2
            ("cust1", "partner2", parse("2022-01-20"), "loc2"),
            ("cust1", "partner2", parse("2022-01-25"), "loc1"),
            ("cust1", "partner2", parse("2022-03-15"), "loc3"),
            ("cust2", "partner2", parse("2022-01-25"), "loc2"),
            ("cust2", "partner2", parse("2022-03-15"), "loc3"),

            // partner3
            ("cust1", "partner3", parse("2022-01-19"), "loc1"),
            ("cust1", "partner3", parse("2022-01-20"), "loc2"),
            ("cust1", "partner3", parse("2022-03-20"), "loc2"),
            ("cust2", "partner3", parse("2022-01-19"), "loc3"),
            ("cust2", "partner3", parse("2022-01-18"), "loc3"),
            ("cust2", "partner3", parse("2022-03-20"), "loc3")
        ).toDF("customer_360_id", "cxi_partner_id", "ord_date", "location_id")

        // when
        // month1: 2021-12-21...2022-01-19, month2: 2022-01-20...2022-02-18, month3: 2022-02-19...2022-03-20
        val actual = LoyaltyFunctions.loyalIndicatorPerPartnerForAllLocations(customer360orders, "2022-03-20")

        // then
        withClue("Frequency score per partner and locations does not match") {
            val expected = Seq(
                ("cust1", AllLocationsAlias, "partner1", 1),
                ("cust2", AllLocationsAlias, "partner1", 1),
                ("cust3", AllLocationsAlias, "partner1", 1),
                ("cust1", AllLocationsAlias, "partner2", 0),
                ("cust2", AllLocationsAlias, "partner2", 0),
                ("cust1", AllLocationsAlias, "partner3", 1),
                ("cust2", AllLocationsAlias, "partner3", 0)
            ).toDF("customer_360_id", "location_id", "cxi_partner_id", "txn_all_months")
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("Loyalty per partner and location") {
        // given
        import spark.implicits._
        val customer360orders = Seq(
            // partner1, loc1
            // rfm_score = 1.0, ,txn_all_months = 1, is_loyal = true
            ("cust1", "partner1", "loc1", "ord11"),
            ("cust1", "partner1", "loc1", "ord12"),
            // rfm_score = 0.53.., ,txn_all_months = 1, is_loyal = true
            ("cust2", "partner1", "loc1", "ord21"),
            // rfm_score = 0.0, ,txn_all_months = 1, is_loyal = false
            ("cust3", "partner1", "loc1", "ord31"),
            // rfm_score = 0.23.., ,txn_all_months = 1, is_loyal = false
            ("cust4", "partner1", "loc1", "ord41"),

            // partner2, loc1
            // rfm_score = 1.0, ,txn_all_months = 0, is_loyal = false
            ("cust1", "partner2", "loc1", "ord112"),

            // partner1, loc2
            // rfm_score = 1.0, ,txn_all_months = 1, is_loyal = true
            ("cust1", "partner1", "loc2", "ord113")
        ).toDF("customer_360_id", "cxi_partner_id", "location_id", "ord_id")

        val recencyScore = Seq(
            ("cust1", "loc1", "partner1", 1.0),
            ("cust2", "loc1", "partner1", 0.6),
            ("cust3", "loc1", "partner1", 0.0),
            ("cust4", "loc1", "partner1", 0.2),
            ("cust1", "loc1", "partner2", 1.0),
            ("cust1", "loc2", "partner1", 0.0)
        ).toDF("customer_360_id", "location_id", "cxi_partner_id", "recency_score")
        val frequencyScore = Seq(
            ("cust1", "loc1", "partner1", 1.0),
            ("cust2", "loc1", "partner1", 0.5),
            ("cust3", "loc1", "partner1", 0.0),
            ("cust4", "loc1", "partner1", 0.2),
            ("cust1", "loc1", "partner2", 1.0),
            ("cust1", "loc2", "partner1", 0.0)
        ).toDF("customer_360_id", "location_id", "cxi_partner_id", "frequency_score")
        val monetaryScore = Seq(
            ("cust1", "loc1", "partner1", 1.0),
            ("cust2", "loc1", "partner1", 0.5),
            ("cust3", "loc1", "partner1", 0.0),
            ("cust4", "loc1", "partner1", 0.3),
            ("cust1", "loc1", "partner2", 1.0),
            ("cust1", "loc2", "partner1", 0.0)
        ).toDF("customer_360_id", "location_id", "cxi_partner_id", "monetary_score")
        val loyalIndicator = Seq(
            ("cust1", "loc1", "partner1", 1),
            ("cust2", "loc1", "partner1", 1),
            ("cust3", "loc1", "partner1", 1),
            ("cust4", "loc1", "partner1", 1),
            ("cust1", "loc1", "partner2", 0),
            ("cust1", "loc2", "partner1", 1)
        ).toDF("customer_360_id", "location_id", "cxi_partner_id", "txn_all_months")

        val rfmScores = LoyaltyFunctions.RfmScores(recencyScore, frequencyScore, monetaryScore, loyalIndicator)

        // when
        val actual = LoyaltyFunctions.loyaltyPerPartnerAndLocation(customer360orders, 0.5, rfmScores)

        // then
        withClue("Loyalty per partner and locations does not match") {
            val expected = Seq(
                ("cust1", "loc1", "partner1", true),
                ("cust2", "loc1", "partner1", true),
                ("cust3", "loc1", "partner1", false),
                ("cust4", "loc1", "partner1", false),
                ("cust1", "loc1", "partner2", false),
                ("cust1", "loc2", "partner1", true)
            ).toDF("customer_360_id", "location_id", "cxi_partner_id", "is_loyal")
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }

    test("Loyalty per partner for all locations") {
        // given
        import spark.implicits._
        val customer360orders = Seq(
            // partner1
            // rfm_score = 1.0, ,txn_all_months = 1, is_loyal = true
            ("cust1", "partner1", "loc1", "ord11"),
            ("cust1", "partner1", "loc3", "ord12"),
            // rfm_score = 0.53.., ,txn_all_months = 1, is_loyal = true
            ("cust2", "partner1", "loc2", "ord21"),
            // rfm_score = 0.0, ,txn_all_months = 1, is_loyal = false
            ("cust3", "partner1", "loc3", "ord31"),
            // rfm_score = 0.23.., ,txn_all_months = 1, is_loyal = false
            ("cust4", "partner1", "loc4", "ord41"),

            // partner2
            // rfm_score = 1.0, ,txn_all_months = 0, is_loyal = false
            ("cust1", "partner2", "loc1", "ord112"),

            // partner3
            // rfm_score = 1.0, ,txn_all_months = 1, is_loyal = true
            ("cust1", "partner3", "loc2", "ord113")
        ).toDF("customer_360_id", "cxi_partner_id", "location_id", "ord_id")

        val recencyScore = Seq(
            ("cust1", "loc1", "partner1", 1.0),
            ("cust2", "loc2", "partner1", 0.6),
            ("cust3", "loc1", "partner1", 0.0),
            ("cust4", "loc4", "partner1", 0.2),
            ("cust1", "loc1", "partner2", 1.0),
            ("cust1", "loc2", "partner3", 0.0)
        ).toDF("customer_360_id", "location_id", "cxi_partner_id", "recency_score")
        val frequencyScore = Seq(
            ("cust1", "loc3", "partner1", 1.0),
            ("cust2", "loc2", "partner1", 0.5),
            ("cust3", "loc2", "partner1", 0.0),
            ("cust4", "loc4", "partner1", 0.2),
            ("cust1", "loc2", "partner2", 1.0),
            ("cust1", "loc3", "partner3", 0.0)
        ).toDF("customer_360_id", "location_id", "cxi_partner_id", "frequency_score")
        val monetaryScore = Seq(
            ("cust1", "loc1", "partner1", 1.0),
            ("cust2", "loc7", "partner1", 0.5),
            ("cust3", "loc1", "partner1", 0.0),
            ("cust4", "loc5", "partner1", 0.3),
            ("cust1", "loc3", "partner2", 1.0),
            ("cust1", "loc4", "partner3", 0.0)
        ).toDF("customer_360_id", "location_id", "cxi_partner_id", "monetary_score")
        val loyalIndicator = Seq(
            ("cust1", "loc8", "partner1", 1),
            ("cust2", "loc3", "partner1", 1),
            ("cust3", "loc5", "partner1", 1),
            ("cust4", "loc3", "partner1", 1),
            ("cust1", "loc3", "partner2", 0),
            ("cust1", "loc5", "partner3", 1)
        ).toDF("customer_360_id", "location_id", "cxi_partner_id", "txn_all_months")

        val rfmScores = LoyaltyFunctions.RfmScores(recencyScore, frequencyScore, monetaryScore, loyalIndicator)

        // when
        val actual = LoyaltyFunctions.loyaltyPerPartnerForAllLocations(customer360orders, 0.5, rfmScores)

        // then
        withClue("Loyalty per partner and locations does not match") {
            val expected = Seq(
                ("cust1", AllLocationsAlias, "partner1", true),
                ("cust2", AllLocationsAlias, "partner1", true),
                ("cust3", AllLocationsAlias, "partner1", false),
                ("cust4", AllLocationsAlias, "partner1", false),
                ("cust1", AllLocationsAlias, "partner2", false),
                ("cust1", AllLocationsAlias, "partner3", true)
            ).toDF("customer_360_id", "location_id", "cxi_partner_id", "is_loyal")
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }
    }
}
