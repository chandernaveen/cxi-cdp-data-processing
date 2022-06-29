package com.cxi.cdp.data_processing
package curated_zone.signal_framework.transactional_insights.pre_aggr.service

import curated_zone.model.signal.transactional_insights.TenderTypeMetric
import refined_zone.hub.model.OrderTenderType
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.functions.col
import org.scalatest.Matchers

class MetricsServiceHelperTest extends BaseSparkBatchJobTest with Matchers {

    import MetricsServiceHelper._

    test("dollarsToCentsUdf") {
        // given
        import spark.implicits._

        val expected = List(
            (Some(10.54), Some(1054L)),
            (Some(12.0), Some(1200L)),
            (None, None)
        ).toDF("dollars", "cents")

        val input = expected.select("dollars")

        // when
        val actual = input.withColumn("cents", dollarsToCentsUdf(col("dollars")))

        // then
        actual.schema shouldBe expected.schema
        actual.collect should contain theSameElementsAs expected.collect
    }

    test("extractTenderTypeMetric") {
        val testCases = Seq(
            OrderTenderType.GiftCard.code -> Some(TenderTypeMetric.GiftCard),
            OrderTenderType.CreditCard.code -> Some(TenderTypeMetric.Card),
            OrderTenderType.Cash.code -> Some(TenderTypeMetric.Cash),
            OrderTenderType.Wallet.code -> Some(TenderTypeMetric.Wallet),
            OrderTenderType.Other.code -> None,
            OrderTenderType.Unknown.code -> None,
            12345 -> None // this should never happen as all tender types are stored in normalized form
        )

        testCases.foreach { case (tenderType, expectedResult) =>
            extractTenderTypeMetric(tenderType) shouldBe expectedResult
        }
    }

}
