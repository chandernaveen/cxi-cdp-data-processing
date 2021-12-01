package com.cxi.cdp.data_processing
package curated_zone.tmi

import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

class TotalMarketInsightsJobTest extends BaseSparkBatchJobTest {

    test("test partner market insights computation") {
        // given
        import spark.implicits._
        val cxiPartnerId1 = "foo-partner-id-1"
        val cxiPartnerId2 = "bar-partner-id-2"
        val cxiPartnerId3 = "baz-partner-id-2"
        val orderSummary = List(
            ("1", cxiPartnerId1, "P300002", "city_1", "state_1", "region_1", "2021-10-10", "Restaurant", 10),
            ("2", cxiPartnerId1, "P300001", "city_2", "state_2", "region_1", "2021-10-10", "Restaurant", 20),
            ("3", cxiPartnerId2, "A300003", "city_3", "state_3", "region_1", "2021-10-10", "Bar", 30),
            ("4", cxiPartnerId2, "B300004", "city_4", "state_4", "region_1", "2021-10-11", "Restaurant", 40),
            ("5", cxiPartnerId3, "C300005", "city_5", "state_5", "region_2", "2021-10-11", "Restaurant", 40),
        ).toDF("ord_id", "cxi_partner_id", "location_id", "city", "state", "region", "ord_date", "location_type", "ord_pay_total")

        // when
        val actual = TotalMarketInsightsJob.computePartnerMarketInsights(orderSummary)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned should contain theSameElementsAs
                Seq("cxi_partner_id", "location_type", "region", "state", "city", "location_id", "date", "transaction_amount", "transaction_quantity")
        }
        val actualPartnerMarketInsightsData = actual.collect()
        withClue("Partner market insights data do not match") {
            val expected = List(
                (cxiPartnerId1, "Restaurant", "region_1", "state_1", "city_1", "P300002", "2021-10-10", 10, 1),
                (cxiPartnerId1, "Restaurant", "region_1", "state_2", "city_2", "P300001", "2021-10-10", 20, 1),
                (cxiPartnerId2, "Bar", "region_1", "state_3", "city_3", "A300003", "2021-10-10", 30, 1),
                (cxiPartnerId2, "Restaurant", "region_1", "state_4", "city_4", "B300004", "2021-10-11", 40, 1),
                (cxiPartnerId3, "Restaurant", "region_2", "state_5", "city_5", "C300005", "2021-10-11", 40, 1),
            ).toDF("cxi_partner_id", "location_type", "region", "state", "city", "location_id", "date", "transaction_amount", "transaction_quantity").collect()
            actualPartnerMarketInsightsData.length should equal(expected.length)
            actualPartnerMarketInsightsData should contain theSameElementsAs expected
        }
    }

    test("test total market insights computation") {
        // given
        import spark.implicits._
        val cxiPartnerId1 = "foo-partner-id-1"
        val cxiPartnerId2 = "bar-partner-id-2"
        val cxiPartnerId3 = "baz-partner-id-2"
        val partnerMarketInsights = List(
            (cxiPartnerId1, "Restaurant", "region_1", "state_1", "city_1", "P300002", "2021-10-10", 10, 1),
            (cxiPartnerId1, "Restaurant", "region_1", "state_1", "city_1", "P300001", "2021-10-10", 20, 1),
            (cxiPartnerId2, "Bar", "region_1", "state_1", "city_1", "A300003", "2021-10-10", 30, 1),
            (cxiPartnerId2, "Restaurant", "region_2", "state_4", "city_4", "B300004", "2021-10-11", 40, 1),
            (cxiPartnerId3, "Restaurant", "region_3", "state_5", "city_5", "C300005", "2021-10-11", 40, 1),
        ).toDF("cxi_partner_id", "location_type", "region", "state", "city", "location_id", "date", "transaction_amount", "transaction_quantity")

        // when
        val actual = TotalMarketInsightsJob.computeTotalMarketInsights(partnerMarketInsights)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned should contain theSameElementsAs
                Seq("location_type", "region", "state", "city", "date", "transaction_amount", "transaction_quantity")
        }
        val actualPartnerMarketInsightsData = actual.collect()
        withClue("Partner market insights data do not match") {
            val expected = List(
                ("Restaurant", "region_1", "state_1", "city_1", "2021-10-10", 30, 2),
                ("Bar", "region_1", "state_1", "city_1", "2021-10-10", 30, 1),
                ("Restaurant", "region_2", "state_4", "city_4", "2021-10-11", 40, 1),
                ("Restaurant", "region_3", "state_5", "city_5", "2021-10-11", 40, 1),
            ).toDF("location_type", "region", "state", "city", "date", "transaction_amount", "transaction_quantity").collect()
            actualPartnerMarketInsightsData.length should equal(expected.length)
            actualPartnerMarketInsightsData should contain theSameElementsAs expected
        }
    }

}
