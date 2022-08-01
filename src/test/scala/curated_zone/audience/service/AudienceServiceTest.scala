package com.cxi.cdp.data_processing
package curated_zone.audience.service

import com.cxi.cdp.data_processing.curated_zone.audience.model.Customer360
import com.cxi.cdp.data_processing.curated_zone.audience.service.AudienceService._
import com.cxi.cdp.data_processing.refined_zone.hub.identity.model.IdentityId
import com.cxi.cdp.data_processing.refined_zone.hub.model.CxiIdentity
import com.cxi.cdp.data_processing.support.BaseSparkBatchJobTest

import org.apache.spark.sql.functions.collect_list
import org.scalatest.Matchers

class AudienceServiceTest extends BaseSparkBatchJobTest with Matchers {
    import spark.implicits._

    case class TestCaseInput(vertices: Seq[String], edges: Seq[(String, String)], expectedClusters: Array[Seq[String]])

    test("test connected components") {
        // given
        val testCases = Seq(
            TestCaseInput(
                vertices = Seq("1", "2", "3", "4", "5"),
                edges = Seq(("1", "2"), ("3", "4"), ("2", "5")),
                expectedClusters = Array(Seq("1", "2", "5"), Seq("3", "4"))
            ),
            TestCaseInput(
                vertices = Seq("1", "2", "3", "4", "5"),
                edges = Seq(),
                expectedClusters = Array(Seq("1"), Seq("2"), Seq("3"), Seq("4"), Seq("5"))
            ),
            TestCaseInput(
                vertices = Seq("1", "2", "3", "4", "5"),
                edges = Seq(("1", "2"), ("2", "3"), ("3", "4"), ("4", "5")),
                expectedClusters = Array(Seq("1", "2", "3", "4", "5"))
            ),
            TestCaseInput(
                vertices = Seq("1", "2", "3", "4", "5"),
                edges = Seq(("1", "2"), ("1", "2"), ("3", "4"), ("3", "4")),
                expectedClusters = Array(Seq("1", "2"), Seq("3", "4"), Seq("5"))
            )
        )

        // when
        for (testCase <- testCases) {
            val connectedComponents = AudienceService
                .buildConnectedComponents(testCase.vertices.toDF("id"), testCase.edges.toDF("src", "dst"))
                .sort(CxiIdentity.CxiIdentityId)

            // then
            connectedComponents.count() shouldBe testCase.vertices.length
            val clusters = connectedComponents
                .groupBy("component_id")
                .agg(collect_list(CxiIdentity.CxiIdentityId) as "ids")
                .collect()
                .map(c => c.getAs[Seq[String]]("ids"))
            clusters shouldBe testCase.expectedClusters
        }
    }

    private final val DefaultDate = java.sql.Date.valueOf(java.time.LocalDate.of(2022, java.time.Month.of(1), 30))
    private final val DayAfterDefaultDate =
        java.sql.Date.valueOf(java.time.LocalDate.of(2022, java.time.Month.of(1), 31))

    test("updateCustomer360 for new customers") {
        val connectedComponents = Seq(
            ("email:one@example.com", 1L),
            ("phone:222", 1L),
            ("phone:333", 1L),
            ("email:two@example.com", 2L),
            ("phone:555", 2L)
        ).toDF(CxiIdentity.CxiIdentityId, AudienceService.ComponentIdCol)

        val prevCustomer360 = Seq.empty[Customer360].toDS

        val newCustomer360 = AudienceService.updateCustomer360(spark, connectedComponents, prevCustomer360).collect

        val firstCustomer = getCustomerWithIdentity(newCustomer360, "email:one@example.com")
        firstCustomer.identities shouldBe Map("email" -> Seq("one@example.com"), "phone" -> Seq("222", "333"))
        firstCustomer.active_flag shouldBe true

        val secondCustomer = getCustomerWithIdentity(newCustomer360, "email:two@example.com")
        secondCustomer.identities shouldBe Map("email" -> Seq("two@example.com"), "phone" -> Seq("555"))
        secondCustomer.active_flag shouldBe true
    }

    test("updateCustomer360 for customer expansion") {
        val connectedComponents = Seq(
            ("email:one@example.com", 1L),
            ("phone:222", 1L), // this is a new identity
            ("phone:333", 1L),
            ("email:two@example.com", 2L), // this is a new identity
            ("phone:555", 2L),
            ("phone:777", 3L) // a new customer
        ).toDF(CxiIdentity.CxiIdentityId, AudienceService.ComponentIdCol)

        val prevCustomer360 = Seq(
            Customer360(
                "FirstCustomer",
                Map("email" -> Seq("one@example.com"), "phone" -> Seq("222")),
                DefaultDate,
                DefaultDate,
                true
            ),
            Customer360("SecondCustomer", Map("phone" -> Seq("555")), DefaultDate, DefaultDate, true)
        ).toDS

        val newCustomer360 = AudienceService.updateCustomer360(spark, connectedComponents, prevCustomer360).collect

        val firstCustomer = getCustomerWithIdentity(newCustomer360, "email:one@example.com")
        firstCustomer.customer_360_id shouldBe "FirstCustomer"
        firstCustomer.identities shouldBe Map("email" -> Seq("one@example.com"), "phone" -> Seq("222", "333"))
        firstCustomer.active_flag shouldBe true

        val secondCustomer = getCustomerWithIdentity(newCustomer360, "email:two@example.com")
        secondCustomer.customer_360_id shouldBe "SecondCustomer"
        secondCustomer.identities shouldBe Map("email" -> Seq("two@example.com"), "phone" -> Seq("555"))
        secondCustomer.active_flag shouldBe true

        val thirdCustomer = getCustomerWithIdentity(newCustomer360, "phone:777")
        Seq("FirstCustomer", "SecondCustomer").contains(thirdCustomer.customer_360_id) shouldBe false
        thirdCustomer.identities shouldBe Map("phone" -> Seq("777"))
        thirdCustomer.active_flag shouldBe true
    }

    test("updateCustomer360 for customer merge") {
        val connectedComponents = Seq(
            ("email:one@example.com", 1L),
            ("phone:222", 1L),
            ("email:two@example.com", 1L), // this is a new identity
            ("phone:555", 1L)
        ).toDF(CxiIdentity.CxiIdentityId, AudienceService.ComponentIdCol)

        val prevCustomer360 = Seq(
            Customer360(
                "FirstCustomer",
                Map("email" -> Seq("one@example.com"), "phone" -> Seq("222")),
                DefaultDate,
                DefaultDate,
                true
            ),
            Customer360("SecondCustomer", Map("phone" -> Seq("555")), DayAfterDefaultDate, DayAfterDefaultDate, true)
        ).toDS

        val newCustomer360 = AudienceService.updateCustomer360(spark, connectedComponents, prevCustomer360).collect

        // pick FirstCustomer as the merged customer name because it is older
        val firstCustomer = getCustomerWithIdentity(newCustomer360, "email:one@example.com", active = true)
        firstCustomer.customer_360_id shouldBe "FirstCustomer"
        firstCustomer.identities shouldBe Map(
            "email" -> Seq("one@example.com", "two@example.com"),
            "phone" -> Seq("222", "555")
        )
        firstCustomer.active_flag shouldBe true
        firstCustomer.create_date shouldBe DefaultDate

        val secondCustomer = getCustomerWithIdentity(newCustomer360, "phone:555", active = false)
        secondCustomer.customer_360_id shouldBe "SecondCustomer"
        secondCustomer.identities shouldBe Map("phone" -> Seq("555"))
        secondCustomer.active_flag shouldBe false
    }

    /** This test case explains the need for the condition of reusing customer_360 IDs only if more than
      * half of their previous identities matched the identities in the connected component.
      *
      * day 1:
      * phone:111 is connected to phone:222, and phone:222 is connected to phone:333
      * customer_360 looks like this:
      * | customer_360_id | identities                                 |
      * | customer-id-1   | identities: {"phone": "111", "222", "333"} |
      *
      * day 2:
      * let's say identity phone:222 and all its connections are removed - e.g. we removed it as a noisy node
      * Then we are left with phone:111 and phone:333 which are no longer connected.
      * Let's try and match existing customer_360_ids against the connected components:
      *
      * | connected_component | identity_id | matched customer_360_id |
      * | 1                   | phone:111   | customer-id-1           |
      * | 2                   | phone:333   | customer-id-1           |
      *
      * We have two new customers that both match the previous customer-id-1.
      * Without the added condition the following questions arise:
      * - which one of the new customers should get the old ID, which should get a new ID?
      * - how to communicate the above decision inside a single Spark operation? now every connected component
      *   potentially depends on all the other connected component
      * - would it even make sense to say that phone:111 is the old customer-id-1, given that it is only one third
      *   of the previous customer?
      *
      * With the added condition customer_360_id is deactivated, and new components are considered new customers.
      */
    test("updateCustomer360 for customer unmerge - minimal case") {
        val connectedComponents = Seq(
            ("phone:111", 1L),
            ("phone:333", 2L)
        ).toDF(CxiIdentity.CxiIdentityId, AudienceService.ComponentIdCol)

        val prevCustomer360 = Seq(
            Customer360("customer_360_id", Map("phone" -> Seq("111", "222", "333")), DefaultDate, DefaultDate, true)
        ).toDS

        // previous customers have lost their inner connections
        val newCustomer360 = AudienceService.updateCustomer360(spark, connectedComponents, prevCustomer360).collect

        val firstCustomer = getCustomerWithIdentity(newCustomer360, "phone:111", active = true)
        firstCustomer.customer_360_id should not be "customer_360_id"
        firstCustomer.identities shouldBe Map("phone" -> Seq("111"))
        firstCustomer.active_flag shouldBe true

        val secondCustomer = getCustomerWithIdentity(newCustomer360, "phone:333", active = true)
        secondCustomer.customer_360_id should not be "customer_360_id"
        secondCustomer.identities shouldBe Map("phone" -> Seq("333"))
        secondCustomer.active_flag shouldBe true

        val prevCustomer = getCustomerWithIdentity(newCustomer360, "phone:111", active = false)
        prevCustomer.customer_360_id shouldBe "customer_360_id"
        prevCustomer.active_flag shouldBe false
    }

    test("updateCustomer360 for customer unmerge") {
        val connectedComponents = Seq(
            ("email:one@example.com", 1L),
            ("phone:111", 1L),
            ("email:two@example.com", 2L),
            ("phone:222", 2L)
        ).toDF(CxiIdentity.CxiIdentityId, AudienceService.ComponentIdCol)

        val prevCustomer360 = Seq(
            Customer360("FirstCustomer", Map("phone" -> Seq("111", "222")), DefaultDate, DefaultDate, true),
            Customer360(
                "SecondCustomer",
                Map("email" -> Seq("one@example.com", "two@example.com")),
                DefaultDate,
                DefaultDate,
                true
            )
        ).toDS

        // previous customers have lost their inner connections
        val newCustomer360 = AudienceService.updateCustomer360(spark, connectedComponents, prevCustomer360).collect

        val firstCustomer = getCustomerWithIdentity(newCustomer360, "email:one@example.com", active = true)
        Seq("FirstCustomer", "SecondCustomer").contains(firstCustomer.customer_360_id) shouldBe false
        firstCustomer.identities shouldBe Map("email" -> Seq("one@example.com"), "phone" -> Seq("111"))
        firstCustomer.active_flag shouldBe true

        val secondCustomer = getCustomerWithIdentity(newCustomer360, "email:two@example.com", active = true)
        Seq("FirstCustomer", "SecondCustomer").contains(secondCustomer.customer_360_id) shouldBe false
        secondCustomer.identities shouldBe Map("email" -> Seq("two@example.com"), "phone" -> Seq("222"))
        secondCustomer.active_flag shouldBe true

        val firstPrevCustomer = getCustomerWithIdentity(newCustomer360, "phone:111", active = false)
        firstPrevCustomer.customer_360_id shouldBe "FirstCustomer"
        firstPrevCustomer.active_flag shouldBe false

        val secondPrevCustomer = getCustomerWithIdentity(newCustomer360, "email:one@example.com", active = false)
        secondPrevCustomer.customer_360_id shouldBe "SecondCustomer"
        secondPrevCustomer.active_flag shouldBe false
    }

    private def getCustomerWithIdentity(
        customers: Seq[Customer360],
        qualifiedIdentityId: String,
        active: Boolean = true
    ): Customer360 = {
        val identityId = IdentityId.fromQualifiedIdentityId(qualifiedIdentityId)

        val matchingCustomers = customers
            .filter(_.active_flag == active)
            .filter(customer => {
                customer.identities
                    .getOrElse(identityId.identity_type, Seq.empty)
                    .contains(identityId.cxi_identity_id)
            })

        matchingCustomers match {
            case Seq(customer) => customer
            case other => throw new Exception(s"Found ${other.size} matching customers, expected one!")
        }
    }

    test("IntermediateCustomer.mergeForSameComponent") {
        val firstComponent = IntermediateCustomer(
            componentId = 10L,
            cxiIdentityIds = Set("1", "2"),
            prevCustomers = Map(
                "FirstCustomer" -> MatchedPrevCustomer(1, 10, DefaultDate),
                "SecondCustomer" -> MatchedPrevCustomer(2, 20, DefaultDate)
            )
        )

        val secondComponent = IntermediateCustomer(
            componentId = 10L,
            cxiIdentityIds = Set("3"),
            prevCustomers = Map(
                "SecondCustomer" -> MatchedPrevCustomer(11, 20, DefaultDate),
                "ThirdCustomer" -> MatchedPrevCustomer(3, 30, DefaultDate)
            )
        )

        val expectedComponent = IntermediateCustomer(
            componentId = 10L,
            cxiIdentityIds = Set("1", "2", "3"),
            prevCustomers = Map(
                "FirstCustomer" -> MatchedPrevCustomer(1, 10, DefaultDate),
                "SecondCustomer" -> MatchedPrevCustomer(13, 20, DefaultDate),
                "ThirdCustomer" -> MatchedPrevCustomer(3, 30, DefaultDate)
            )
        )

        IntermediateCustomer.mergeForSameComponent(firstComponent, secondComponent) shouldBe expectedComponent
    }

    test("findWinningPrevCustomer depending on the number of matched identities") {
        // not enough matched identities - 1 out of 10
        findWinningPrevCustomer(Map("FirstCustomer" -> MatchedPrevCustomer(1, 10, DefaultDate))) shouldBe None

        // not enough matched identities - 5 out of 10
        findWinningPrevCustomer(Map("FirstCustomer" -> MatchedPrevCustomer(5, 10, DefaultDate))) shouldBe None

        // enough matched identities - 66 out of 10
        findWinningPrevCustomer(Map("FirstCustomer" -> MatchedPrevCustomer(6, 10, DefaultDate))) shouldBe
            Some(("FirstCustomer", MatchedPrevCustomer(6, 10, DefaultDate)))
    }

    test("findWinningPrevCustomer when there is a match") {
        val prevCustomers = Map(
            "FirstCustomer" -> MatchedPrevCustomer(1, 10, DefaultDate), // not enough matched identities
            "SecondCustomer" -> MatchedPrevCustomer(13, 20, DefaultDate),
            "ThirdCustomer" -> MatchedPrevCustomer(16, 30, DefaultDate), // oldest customer with the most connections
            "FourthCustomer" -> MatchedPrevCustomer(26, 35, DayAfterDefaultDate)
        )

        findWinningPrevCustomer(prevCustomers) shouldBe Some(
            ("ThirdCustomer", MatchedPrevCustomer(16, 30, DefaultDate))
        )
    }

    test("groupIdentitiesByType") {
        val qualifiedIdentityIds = Set(
            "email:one@example.com",
            "email:two@example.com",
            "phone:123",
            "phone:456",
            "combination-bin:XXX"
        )

        groupIdentitiesByType(qualifiedIdentityIds) shouldBe Map(
            "email" -> Seq("one@example.com", "two@example.com"),
            "phone" -> Seq("123", "456"),
            "combination-bin" -> Seq("XXX")
        )
    }

    test("mergePrevAndNewCustomer360") {
        val prevCustomer360 = Seq(
            Customer360("customer_exists_in_both", Map("phone" -> Seq("222")), DefaultDate, DefaultDate, true),
            Customer360("customer_exists_in_prev", Map("phone" -> Seq("333")), DefaultDate, DefaultDate, true)
        )

        val newCustomer360 = Seq(
            Customer360("customer_exists_in_new", Map("phone" -> Seq("111")), DefaultDate, DefaultDate, true),
            Customer360("customer_exists_in_both", Map("phone" -> Seq("222", "333")), DefaultDate, DefaultDate, true)
        )

        val expectedCustomer360 = newCustomer360 :+
            // this customer is no longer active
            Customer360("customer_exists_in_prev", Map("phone" -> Seq("333")), DefaultDate, DefaultDate, false)

        val actualCustomer360 = mergePrevAndNewCustomer360(spark, prevCustomer360.toDS, newCustomer360.toDS).collect

        actualCustomer360 should contain theSameElementsAs expectedCustomer360
    }

}
