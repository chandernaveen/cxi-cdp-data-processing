package com.cxi.cdp.data_processing
package curated_zone

import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

class TotalMarketInsightJobTest extends BaseSparkBatchJobTest {

    test("test partner market insights computation") {
        // given
        import spark.implicits._
        val cxiPartnerId1 = "foo-partner-id-1"
        val cxiPartnerId2 = "bar-partner-id-2"
        val cxiPartnerId3 = "baz-partner-id-2"
        val orderSummaryWithPartnerInfo = List(
            ("1", cxiPartnerId1, "P300002", "2021-10-10", "restaurant", 10),
            ("2", cxiPartnerId1, "P300001", "2021-10-10", "restaurant", 20),
            ("3", cxiPartnerId2, "A300003", "2021-10-10", "bar", 30),
            ("4", cxiPartnerId2, "B300004", "2021-10-11", "restaurant", 40),
            ("5", cxiPartnerId3, "C300005", "2021-10-11", "restaurant", 40),
        ).toDF("ord_id", "cxi_partner_id", "location_id", "ord_date", "partner_type", "item_total")

        // when
        val actual = TotalMarketInsightJob.computePartnerMarketInsights(orderSummaryWithPartnerInfo)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array("cxi_partner_id", "location_id", "ord_date", "partner_type", "amount_of_sales", "foot_traffic")
        }
        val actualPartnerMarketInsightsData = actual.collect()
        withClue("Partner market insights data do not match") {
            val expected = List(
                (cxiPartnerId1, "P300002", "2021-10-10", "restaurant", 10, 1),
                (cxiPartnerId1, "P300001", "2021-10-10", "restaurant", 20, 1),
                (cxiPartnerId2, "A300003", "2021-10-10", "bar", 30, 1),
                (cxiPartnerId2, "B300004", "2021-10-11", "restaurant", 40, 1),
                (cxiPartnerId3, "C300005", "2021-10-11", "restaurant", 40, 1),
            ).toDF("cxi_partner_id", "location_id", "ord_date", "partner_type", "amount_of_sales", "foot_traffic").collect()
            actualPartnerMarketInsightsData.length should equal(expected.length)
            actualPartnerMarketInsightsData should contain theSameElementsAs expected
        }
    }

}
